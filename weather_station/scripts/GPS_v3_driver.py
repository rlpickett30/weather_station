"""
GPS_v3_driver.py
----------------
Persistent, long-lived UART driver for the Adafruit Ultimate GPS v3 (UART NMEA).

Design intent:
- The driver OWNS the UART for the lifetime of the process.
- It continuously ingests NMEA and maintains a "latest snapshot" in memory.
- It exposes raw facts (no gating, filtering, PPS logic, or time fallback).
- The manager (GPS_PPS_management.py) will apply policy: validation, filtering,
  drift/discipline, and fallback to Pi time when PPS is unavailable.

Defaults:
- Port: /dev/serial0
- Baud: 9600
- Update rate: 1 Hz
- Sentence set: RMC + GGA (minimum viable for time + fix + altitude)

Dependencies:
- pyserial
- adafruit-circuitpython-gps  (import adafruit_gps)

Notes:
- This driver deliberately does NOT block waiting for a fix.
- It does NOT attempt reconnection loops silently. If start() fails, it raises.
- If the stream stops after start(), staleness is observable via timestamps/counters.
"""

from __future__ import annotations

import threading
import time
from typing import Any, Dict, Optional

import serial
import adafruit_gps


class GPSv3Driver:
    """
    Persistent GPS driver that continuously reads/parses NMEA in a background thread.
    """

    def __init__(
        self,
        port: str = "/dev/serial0",
        baud: int = 9600,
        update_hz: float = 1.0,
        debug_nmea: bool = False,
    ) -> None:
        if update_hz <= 0:
            raise ValueError("update_hz must be > 0")

        self.port = port
        self.baud = baud
        self.update_hz = update_hz
        self.debug_nmea = debug_nmea

        self._uart: Optional[serial.Serial] = None
        self._gps: Optional[adafruit_gps.GPS] = None

        self._lock = threading.Lock()
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._started = False

        # Snapshot state (kept consistent under self._lock)
        now_mono = time.monotonic()
        self._state: Dict[str, Any] = {
            # Identity / config
            "serial_port": self.port,
            "baud": self.baud,
            "update_hz": self.update_hz,
            # Link / health
            "last_nmea_rx_monotonic": None,  # float | None
            "last_sentence_type": None,  # str | None
            "nmea_rx_count": 0,  # int
            "nmea_parse_errors": 0,  # int
            "driver_start_monotonic": None,  # float | None
            "last_driver_error": None,  # str | None
            "last_driver_error_monotonic": None,  # float | None
            # Fix facts (raw)
            "has_fix": False,
            "fix_quality": None,
            "fix_type": None,
            "satellites": None,
            "hdop": None,
            "lat_dd": None,
            "lon_dd": None,
            "alt_m": None,
            "speed_knots": None,
            "track_angle_deg": None,
            # Time facts (raw)
            "utc_datetime": None,  # datetime.datetime | None
            "utc_valid": None,  # bool | None
            "last_utc_update_monotonic": None,  # float | None
            # Freshness helper
            "snapshot_monotonic": now_mono,
        }

    def start(self) -> None:
        """
        Open UART, configure GPS output, and start background ingestion thread.
        Raises RuntimeError if already started.
        Raises serial.SerialException or other exceptions if UART cannot be opened.
        """
        if self._started:
            raise RuntimeError("GPSv3Driver is already started")

        # Open UART once.
        self._uart = serial.Serial(self.port, baudrate=self.baud, timeout=1)

        # Instantiate Adafruit GPS parser.
        self._gps = adafruit_gps.GPS(self._uart, debug=self.debug_nmea)

        # Configure minimal sentence set: RMC (time/speed/track) + GGA (fix/alt/sats)
        # PMTK314 format: enable/disable sentences; this enables RMC and GGA only.
        # Update rate: PMTK220,<ms>
        self._gps.send_command(b"PMTK314,0,1,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0")
        interval_ms = int(round(1000.0 / self.update_hz))
        self._gps.send_command(f"PMTK220,{interval_ms}".encode("ascii"))

        with self._lock:
            self._state["driver_start_monotonic"] = time.monotonic()
            self._state["last_driver_error"] = None
            self._state["last_driver_error_monotonic"] = None
            self._state["serial_port"] = self.port
            self._state["baud"] = self.baud
            self._state["update_hz"] = self.update_hz
            self._state["snapshot_monotonic"] = time.monotonic()

        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, name="GPSv3DriverThread", daemon=True)
        self._thread.start()
        self._started = True

    def stop(self) -> None:
        """
        Stop background ingestion and close UART.
        Safe to call multiple times.
        """
        self._stop_event.set()

        t = self._thread
        if t and t.is_alive():
            t.join(timeout=2.0)

        self._thread = None
        self._started = False

        # Close UART last.
        if self._uart:
            try:
                self._uart.close()
            except Exception:
                pass

        self._uart = None
        self._gps = None

    def get_snapshot(self) -> Dict[str, Any]:
        """
        Return a shallow copy of the latest state snapshot.
        Never blocks for long.
        """
        with self._lock:
            snap = dict(self._state)
            snap["snapshot_monotonic"] = time.monotonic()
            return snap

    def is_started(self) -> bool:
        return self._started

    # -----------------
    # Internal methods
    # -----------------

    def _run(self) -> None:
        assert self._gps is not None

        # Conservative loop cadence. Parsing is driven by incoming serial data.
        # We do not sleep heavily because GPS.update() itself is lightweight with timeout=1 on UART.
        while not self._stop_event.is_set():
            try:
                # update() reads from UART and parses what it can. It does not guarantee a full sentence.
                self._gps.update()

                # If we made it here without exception, consider incrementing RX count only when we see
                # evidence of a new sentence. The library does not expose "sentence received" directly,
                # so we approximate: if any of these fields changed recently, or if time advanced.
                # For simplicity and robustness, we update state opportunistically on every call.
                self._update_state_from_gps()

            except Exception as e:
                # Record the error, but keep running. The manager can decide what to do with staleness/errors.
                with self._lock:
                    self._state["nmea_parse_errors"] += 1
                    self._state["last_driver_error"] = f"{type(e).__name__}: {e}"
                    self._state["last_driver_error_monotonic"] = time.monotonic()
                    self._state["snapshot_monotonic"] = time.monotonic()

                # Brief backoff to avoid tight exception loops.
                time.sleep(0.2)

    def _update_state_from_gps(self) -> None:
        assert self._gps is not None

        mono = time.monotonic()

        # Raw fields from adafruit_gps.GPS
        has_fix = bool(self._gps.has_fix)

        # utc_datetime can exist even before has_fix depending on sentence availability,
        # but you should treat it as raw data here and let the manager interpret validity.
        utc_dt = getattr(self._gps, "timestamp_utc", None)

        # Some chipsets expose quality/hdop/sats; these may be None.
        fix_quality = getattr(self._gps, "fix_quality", None)
        satellites = getattr(self._gps, "satellites", None)
        hdop = getattr(self._gps, "hdop", None)

        # Fix type is not always present in this library; keep as None unless you later augment it.
        fix_type = getattr(self._gps, "fix_type", None) if hasattr(self._gps, "fix_type") else None

        lat = getattr(self._gps, "latitude", None)
        lon = getattr(self._gps, "longitude", None)
        alt = getattr(self._gps, "altitude_m", None)

        speed_knots = getattr(self._gps, "speed_knots", None)
        track_angle_deg = getattr(self._gps, "track_angle_deg", None)

        # Update state atomically.
        with self._lock:
            # Approximate that we "received" data if update() ran; manager will use freshness/age.
            self._state["last_nmea_rx_monotonic"] = mono
            self._state["nmea_rx_count"] += 1

            # Sentence type is not exposed; keep None for now. If you later swap parsers, fill it.
            self._state["last_sentence_type"] = self._state.get("last_sentence_type", None)

            self._state["has_fix"] = has_fix
            self._state["fix_quality"] = fix_quality
            self._state["fix_type"] = fix_type
            self._state["satellites"] = satellites
            self._state["hdop"] = hdop

            self._state["lat_dd"] = float(lat) if lat is not None else None
            self._state["lon_dd"] = float(lon) if lon is not None else None
            self._state["alt_m"] = float(alt) if alt is not None else None

            self._state["speed_knots"] = float(speed_knots) if speed_knots is not None else None
            self._state["track_angle_deg"] = float(track_angle_deg) if track_angle_deg is not None else None

            # Time facts (raw)
            self._state["utc_datetime"] = utc_dt

            # A simple raw validity flag: timestamp exists and has reasonable components.
            # The manager will apply stricter criteria if desired.
            utc_valid = bool(utc_dt)  # could be refined later without changing manager contract
            self._state["utc_valid"] = utc_valid
            if utc_valid:
                self._state["last_utc_update_monotonic"] = mono

            self._state["snapshot_monotonic"] = mono


# Optional: simple manual sanity check (not used by the dispatcher)
if __name__ == "__main__":
    gps = GPSv3Driver(port="/dev/serial0", baud=9600, update_hz=1.0, debug_nmea=False)
    try:
        gps.start()
        print("GPS driver started. Printing 10 snapshots (1 per second).")
        for _ in range(10):
            snap = gps.get_snapshot()
            print(
                {
                    "has_fix": snap["has_fix"],
                    "lat": snap["lat_dd"],
                    "lon": snap["lon_dd"],
                    "alt_m": snap["alt_m"],
                    "sats": snap["satellites"],
                    "hdop": snap["hdop"],
                    "utc_valid": snap["utc_valid"],
                    "utc": str(snap["utc_datetime"]) if snap["utc_datetime"] else None,
                    "rx_count": snap["nmea_rx_count"],
                    "last_err": snap["last_driver_error"],
                }
            )
            time.sleep(1.0)
    finally:
        gps.stop()
        print("GPS driver stopped.")
