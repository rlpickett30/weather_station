"""
GPS_PPS_management.py
---------------------
GPS + PPS manager for WeatherStation.

Purpose:
- Consume raw GPS snapshots from GPS_v3_driver.py (driver-owned UART).
- Apply validation / gating for position data (satellites, HDOP, freshness).
- Track PPS presence and lock state (kernel PPS preferred via /dev/pps*).
- Produce a single, stable output dict for driver_manager.py (easy refactor surface).
- Prefer PPS-disciplined UTC time; fall back to GPS UTC; then fall back to Pi clock.

Design notes:
- The GPS driver remains "facts only" (raw NMEA-derived fields).
- This manager applies policy and state: validation, lock detection, discipline offset.
- This module does NOT set the system clock. It produces a disciplined time estimate.

Kernel PPS:
- Uses Linux PPS API via /dev/pps0 (or configured path).
- If /dev/pps0 cannot be opened, PPS is treated as unavailable and the system
  falls back to GPS UTC or Pi clock.

Output contract (get_output()):
- Always returns a dict with consistent keys. Values may be None if unavailable.

Dependencies:
- Standard library only (plus your GPS driver).
"""

from __future__ import annotations

import datetime as _dt
import os
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from GPS_v3_driver import GPSv3Driver

# -----------------------------
# PPS (Linux) low-level support
# -----------------------------
# This is intentionally minimal. It reads PPS events from /dev/ppsX via ioctl.
# If this fails on a given platform, PPS will be gracefully disabled.

try:
    import fcntl  # type: ignore
    import struct  # type: ignore

    _HAS_FCNTL = True
except Exception:
    _HAS_FCNTL = False


# ioctl constants from linux/pps.h (commonly stable across kernels)
_PPS_MAGIC = ord("p")
_PPS_FETCH = 0xC010700A  # _IOWR('p', 0x0a, struct pps_fdata)
# Note: We are using a fixed value for PPS_FETCH to avoid kernel header dependency.
# If this ever fails, PPS will simply be disabled and you will fall back to GPS/Pi time.


@dataclass
class PPSEvent:
    seq: int
    assert_sec: int
    assert_nsec: int

    @property
    def assert_time(self) -> float:
        return float(self.assert_sec) + float(self.assert_nsec) * 1e-9


class KernelPPSReader:
    """
    Reads PPS events from a Linux PPS character device (e.g., /dev/pps0).

    If the device is missing or ioctl fails, open() will raise and PPS will be disabled.
    """

    def __init__(self, pps_device: str = "/dev/pps0") -> None:
        self.pps_device = pps_device
        self._fd: Optional[int] = None
        self._last_seq: Optional[int] = None

    def open(self) -> None:
        if not _HAS_FCNTL:
            raise RuntimeError("Kernel PPS requires fcntl/struct (Linux).")
        if not os.path.exists(self.pps_device):
            raise FileNotFoundError(f"PPS device not found: {self.pps_device}")
        self._fd = os.open(self.pps_device, os.O_RDWR | os.O_NONBLOCK)
        self._last_seq = None

    def close(self) -> None:
        if self._fd is not None:
            try:
                os.close(self._fd)
            except Exception:
                pass
        self._fd = None
        self._last_seq = None

    def read_event(self, timeout_ms: int = 0) -> Optional[PPSEvent]:
        """
        Attempt to fetch the latest PPS assert event.

        This is non-blocking by design (timeout_ms is reserved for future expansion).
        Returns:
            PPSEvent if a new event is available since the last call, otherwise None.
        """
        if self._fd is None:
            return None

        # struct pps_fdata { pps_info_t info; pps_timeu_t timeout; int timeout_flags; };
        # pps_info_t contains assert_timestamp and assert_sequence.
        #
        # We will pack/unpack only the relevant parts with a conservative layout that
        # matches typical 64-bit Linux ABI. If ioctl fails, we disable PPS upstream.
        #
        # Layout assumption (common):
        # - assert_timestamp: struct timespec (sec: long, nsec: long)
        # - clear_timestamp: struct timespec (ignored)
        # - assert_sequence: unsigned int
        # - clear_sequence: unsigned int (ignored)
        #
        # We allocate a buffer and let ioctl fill it.

        # Buffer size for pps_fdata is commonly 48 or 64 bytes. Use 64 for safety.
        buf = bytearray(64)

        try:
            fcntl.ioctl(self._fd, _PPS_FETCH, buf, True)
        except BlockingIOError:
            return None
        except OSError:
            # Treat as no event; caller may decide to disable PPS if persistent.
            return None

        # Unpack assert_timestamp.sec, assert_timestamp.nsec, clear_timestamp.sec,
        # clear_timestamp.nsec, assert_sequence, clear_sequence.
        #
        # Use native endianness, native long size.
        # Format: 4 longs + 2 unsigned ints. Then ignore the rest.
        try:
            sec_a, nsec_a, _sec_c, _nsec_c, seq_a, _seq_c = struct.unpack_from("llllII", buf, 0)
        except Exception:
            return None

        if self._last_seq is not None and seq_a == self._last_seq:
            return None

        self._last_seq = seq_a
        return PPSEvent(seq=int(seq_a), assert_sec=int(sec_a), assert_nsec=int(nsec_a))


# -----------------------------
# GPS/PPS management
# -----------------------------

@dataclass
class GPSGateConfig:
    # Freshness gating (NMEA stream health)
    max_nmea_age_s: float = 3.0

    # Position gating (quality)
    min_satellites: int = 4
    max_hdop: float = 3.0  # None-like behavior is handled (missing hdop -> not automatically rejected)

    # PPS lock behavior
    pps_present_window_s: float = 2.5
    pps_lock_pulses_required: int = 5
    pps_unlock_miss_s: float = 3.5

    # Discipline filter
    offset_alpha: float = 0.15  # low-pass filter strength for PPS-derived offset


class GPSPPSManager:
    """
    Consumes GPSv3Driver snapshots and (optionally) kernel PPS events to produce
    validated navigation outputs and disciplined time.

    This class is designed to be imported and used by driver_manager.py.
    """

    def __init__(
        self,
        gps_driver: GPSv3Driver,
        *,
        pps_device: str = "/dev/pps0",
        gate: Optional[GPSGateConfig] = None,
        loop_hz: float = 5.0,
    ) -> None:
        if loop_hz <= 0:
            raise ValueError("loop_hz must be > 0")

        self.gps = gps_driver
        self.pps_device = pps_device
        self.gate = gate or GPSGateConfig()
        self.loop_hz = loop_hz

        self._pps_reader: Optional[KernelPPSReader] = None
        self._pps_enabled = False

        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._started = False

        # PPS state
        self._pps_last_event_mono: Optional[float] = None
        self._pps_consecutive: int = 0
        self._pps_locked: bool = False
        self._pps_present: bool = False
        self._pps_last_seq: Optional[int] = None

        # Discipline offset: UTC_estimate = system_time + offset_seconds
        self._offset_seconds: Optional[float] = None
        self._last_offset_update_mono: Optional[float] = None

        # Last computed output
        self._output: Dict[str, Any] = self._empty_output()

    def start(self) -> None:
        if self._started:
            raise RuntimeError("GPSPPSManager is already started")

        # PPS is optional. If it fails, we fall back gracefully.
        self._pps_reader = None
        self._pps_enabled = False
        try:
            reader = KernelPPSReader(self.pps_device)
            reader.open()
            self._pps_reader = reader
            self._pps_enabled = True
        except Exception:
            self._pps_reader = None
            self._pps_enabled = False

        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, name="GPSPPSManagerThread", daemon=True)
        self._thread.start()
        self._started = True

    def stop(self) -> None:
        self._stop_event.set()
        t = self._thread
        if t and t.is_alive():
            t.join(timeout=2.0)
        self._thread = None
        self._started = False

        if self._pps_reader is not None:
            try:
                self._pps_reader.close()
            except Exception:
                pass
        self._pps_reader = None
        self._pps_enabled = False

    def is_started(self) -> bool:
        return self._started

    def get_output(self) -> Dict[str, Any]:
        """
        Return the most recent manager output dict.
        """
        with self._lock:
            return dict(self._output)

    # -----------------
    # Internal routines
    # -----------------

    def _run(self) -> None:
        period = 1.0 / self.loop_hz

        while not self._stop_event.is_set():
            loop_start = time.monotonic()

            gps_snap = self.gps.get_snapshot()
            pps_event = self._try_read_pps()

            self._update_pps_state(pps_event)
            self._update_offset_if_possible(pps_event, gps_snap)

            out = self._compute_output(gps_snap)

            with self._lock:
                self._output = out

            # Pace loop
            elapsed = time.monotonic() - loop_start
            sleep_s = period - elapsed
            if sleep_s > 0:
                time.sleep(sleep_s)

    def _try_read_pps(self) -> Optional[PPSEvent]:
        if not self._pps_enabled or self._pps_reader is None:
            return None
        try:
            return self._pps_reader.read_event()
        except Exception:
            return None

    def _update_pps_state(self, evt: Optional[PPSEvent]) -> None:
        now = time.monotonic()

        if evt is not None:
            # New event
            self._pps_last_event_mono = now
            self._pps_present = True

            if self._pps_last_seq is None or evt.seq != self._pps_last_seq:
                self._pps_consecutive += 1
            self._pps_last_seq = evt.seq

            if not self._pps_locked and self._pps_consecutive >= self.gate.pps_lock_pulses_required:
                self._pps_locked = True

        # Presence decay
        if self._pps_last_event_mono is None:
            self._pps_present = False
            self._pps_locked = False
            self._pps_consecutive = 0
            return

        age = now - self._pps_last_event_mono
        self._pps_present = age <= self.gate.pps_present_window_s

        # Unlock if missing too long
        if self._pps_locked and age > self.gate.pps_unlock_miss_s:
            self._pps_locked = False
            self._pps_consecutive = 0

    def _update_offset_if_possible(self, evt: Optional[PPSEvent], gps_snap: Dict[str, Any]) -> None:
        """
        If PPS is present and we have a GPS UTC label, estimate offset between system time and UTC.

        We do not set the system clock. We compute:
            utc_est = system_time + offset_seconds

        On each PPS edge:
        - Assume the PPS edge occurs very near an integer UTC second.
        - Use the most recent GPS UTC datetime as a label.
        - Update offset with a low-pass filter.
        """
        if evt is None:
            return

        # Require at least "some" UTC info from GPS.
        utc_dt = gps_snap.get("utc_datetime")
        utc_valid = bool(gps_snap.get("utc_valid"))
        if not utc_valid or utc_dt is None:
            return

        if not isinstance(utc_dt, _dt.datetime):
            return

        # Convert GPS UTC label to seconds since epoch (UTC).
        if utc_dt.tzinfo is None:
            # Treat as UTC-naive
            utc_dt = utc_dt.replace(tzinfo=_dt.timezone.utc)
        else:
            utc_dt = utc_dt.astimezone(_dt.timezone.utc)

        gps_utc_epoch = utc_dt.timestamp()

        # System wall clock at "now" is not the PPS assert time; however, the ioctl does not
        # provide wall time. We discipline using the moment we processed the PPS event and
        # rely on kernel timestamping for stability. This is still beneficial for drift smoothing.
        sys_epoch = time.time()

        # Snap GPS label to nearest integer second (PPS edge represents a second boundary).
        gps_utc_epoch_rounded = round(gps_utc_epoch)

        new_offset = gps_utc_epoch_rounded - sys_epoch

        if self._offset_seconds is None:
            self._offset_seconds = float(new_offset)
        else:
            a = self.gate.offset_alpha
            self._offset_seconds = (1.0 - a) * self._offset_seconds + a * float(new_offset)

        self._last_offset_update_mono = time.monotonic()

    def _compute_output(self, gps_snap: Dict[str, Any]) -> Dict[str, Any]:
        now_mono = time.monotonic()

        # Link health
        last_rx = gps_snap.get("last_nmea_rx_monotonic")
        nmea_age = (now_mono - last_rx) if isinstance(last_rx, (int, float)) else None
        gps_link_ok = (nmea_age is not None) and (nmea_age <= self.gate.max_nmea_age_s)

        # Raw fix facts
        has_fix = bool(gps_snap.get("has_fix"))
        sats = gps_snap.get("satellites")
        hdop = gps_snap.get("hdop")
        lat = gps_snap.get("lat_dd")
        lon = gps_snap.get("lon_dd")
        alt = gps_snap.get("alt_m")

        # Validation gating (position)
        sats_ok = (sats is None) or (isinstance(sats, int) and sats >= self.gate.min_satellites)
        hdop_ok = (hdop is None) or (isinstance(hdop, (int, float)) and float(hdop) <= self.gate.max_hdop)

        gps_position_valid = bool(gps_link_ok and has_fix and sats_ok and hdop_ok and lat is not None and lon is not None)

        # Time selection
        utc_dt = gps_snap.get("utc_datetime")
        utc_valid = bool(gps_snap.get("utc_valid")) and isinstance(utc_dt, _dt.datetime)

        pps_present = bool(self._pps_present)
        pps_locked = bool(self._pps_locked)

        # Determine best available UTC estimate
        utc_est: Optional[_dt.datetime] = None
        time_source: str = "pi_clock"

        if pps_locked and self._offset_seconds is not None:
            # PPS-disciplined system time (best)
            est_epoch = time.time() + float(self._offset_seconds)
            utc_est = _dt.datetime.fromtimestamp(est_epoch, tz=_dt.timezone.utc)
            time_source = "gps_pps"
        elif utc_valid:
            # GPS UTC label (good, but not PPS-disciplined)
            if utc_dt.tzinfo is None:
                utc_est = utc_dt.replace(tzinfo=_dt.timezone.utc)
            else:
                utc_est = utc_dt.astimezone(_dt.timezone.utc)
            time_source = "gps_utc"
        else:
            # Local Pi clock (fallback)
            utc_est = _dt.datetime.now(tz=_dt.timezone.utc)
            time_source = "pi_clock"

        # node_time_s: seconds since midnight UTC
        node_time_s: Optional[int] = None
        if utc_est is not None:
            midnight = utc_est.replace(hour=0, minute=0, second=0, microsecond=0)
            node_time_s = int((utc_est - midnight).total_seconds())

        out = self._empty_output()
        out.update(
            {
                # Time outputs
                "time_source": time_source,          # "gps_pps" | "gps_utc" | "pi_clock"
                "utc_estimate": utc_est,             # datetime (UTC)
                "node_time_s": node_time_s,          # int seconds since midnight UTC
                "utc_valid_raw": utc_valid,          # GPS library raw validity
                "pps_present": pps_present,
                "pps_locked": pps_locked,
                "offset_seconds": self._offset_seconds,
                "last_offset_update_mono": self._last_offset_update_mono,

                # GPS link / health
                "gps_link_ok": gps_link_ok,
                "nmea_age_s": nmea_age,
                "nmea_rx_count": gps_snap.get("nmea_rx_count"),
                "nmea_parse_errors": gps_snap.get("nmea_parse_errors"),
                "last_driver_error": gps_snap.get("last_driver_error"),

                # GPS raw and gated nav outputs
                "gps_has_fix_raw": has_fix,
                "satellites": sats,
                "hdop": hdop,
                "gps_position_valid": gps_position_valid,

                "lat_dd": float(lat) if gps_position_valid else None,
                "lon_dd": float(lon) if gps_position_valid else None,
                "alt_m": float(alt) if (gps_position_valid and alt is not None) else None,
            }
        )
        return out

    @staticmethod
    def _empty_output() -> Dict[str, Any]:
        return {
            # Time
            "time_source": "pi_clock",
            "utc_estimate": None,
            "node_time_s": None,
            "utc_valid_raw": None,
            "pps_present": False,
            "pps_locked": False,
            "offset_seconds": None,
            "last_offset_update_mono": None,

            # Health
            "gps_link_ok": False,
            "nmea_age_s": None,
            "nmea_rx_count": 0,
            "nmea_parse_errors": 0,
            "last_driver_error": None,

            # Navigation
            "gps_has_fix_raw": False,
            "satellites": None,
            "hdop": None,
            "gps_position_valid": False,
            "lat_dd": None,
            "lon_dd": None,
            "alt_m": None,
        }


# Optional: manual sanity check
if __name__ == "__main__":
    gps = GPSv3Driver(port="/dev/serial0", baud=9600, update_hz=1.0, debug_nmea=False)
    mgr = GPSPPSManager(gps, pps_device="/dev/pps0", loop_hz=5.0)

    try:
        gps.start()
        mgr.start()
        print("GPS+PPS manager started. Printing 15 outputs (1 per second).")
        for _ in range(15):
            out = mgr.get_output()
            print(
                {
                    "time_source": out["time_source"],
                    "pps_present": out["pps_present"],
                    "pps_locked": out["pps_locked"],
                    "node_time_s": out["node_time_s"],
                    "gps_link_ok": out["gps_link_ok"],
                    "gps_position_valid": out["gps_position_valid"],
                    "lat": out["lat_dd"],
                    "lon": out["lon_dd"],
                    "sats": out["satellites"],
                    "hdop": out["hdop"],
                    "nmea_age_s": out["nmea_age_s"],
                    "err": out["last_driver_error"],
                }
            )
            time.sleep(1.0)
    finally:
        mgr.stop()
        gps.stop()
        print("Stopped.")
