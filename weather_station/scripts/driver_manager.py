"""
driver_manager.py
-----------------
WeatherStation Driver Manager (GPS-only for now).

Purpose:
- Own lifecycle of GPS_v3_driver + GPS_PPS_management.
- Provide a single, stable surface for weather_dispatcher.py to import.
- Use try/except for resilience and print useful debug messages.

Current scope:
- GPS + PPS manager only (ignores SHT45 and BMP390 for now).

Design contract:
- get_payload() ALWAYS returns a dict with stable keys.
- If GPS or PPS is degraded, payload clearly indicates status and fallback behavior.

This module is intended to be the import boundary:
    from driver_manager import DriverManager
"""

from __future__ import annotations

import time
import traceback
from typing import Any, Dict, Optional

from GPS_v3_driver import GPSv3Driver
from GPS_PPS_management import GPSPPSManager, GPSGateConfig


class DriverManager:
    """
    Orchestrates device drivers and managers, and exposes a single payload for the dispatcher.
    """

    def __init__(
        self,
        *,
        gps_port: str = "/dev/serial0",
        gps_baud: int = 9600,
        gps_update_hz: float = 1.0,
        pps_device: str = "/dev/pps0",
        manager_loop_hz: float = 5.0,
        debug: bool = True,
        gate_config: Optional[GPSGateConfig] = None,
    ) -> None:
        self.debug = debug

        self.gps_driver = GPSv3Driver(
            port=gps_port,
            baud=gps_baud,
            update_hz=gps_update_hz,
            debug_nmea=False,
        )

        self.gps_pps_manager = GPSPPSManager(
            self.gps_driver,
            pps_device=pps_device,
            gate=gate_config or GPSGateConfig(),
            loop_hz=manager_loop_hz,
        )

        self._started = False
        self._last_error: Optional[str] = None
        self._last_error_monotonic: Optional[float] = None

    # -----------------
    # Lifecycle
    # -----------------

    def start(self) -> None:
        """
        Start GPS driver and GPS/PPS manager.
        Any failure is recorded and re-raised for dispatcher policy (retry/backoff).
        """
        if self._started:
            self._dbg("DriverManager.start(): already started.")
            return

        self._dbg("DriverManager.start(): starting GPS driver...")
        try:
            self.gps_driver.start()
            self._dbg("DriverManager.start(): GPS driver started.")
        except Exception as e:
            self._record_error(f"GPS driver start failed: {type(e).__name__}: {e}")
            self._dbg(self._last_error)
            raise

        self._dbg("DriverManager.start(): starting GPS/PPS manager...")
        try:
            self.gps_pps_manager.start()
            self._dbg("DriverManager.start(): GPS/PPS manager started.")
        except Exception as e:
            self._record_error(f"GPS/PPS manager start failed: {type(e).__name__}: {e}")
            self._dbg(self._last_error)

            # If manager fails, stop GPS driver so we do not leak UART ownership.
            try:
                self.gps_driver.stop()
            except Exception:
                pass
            raise

        self._started = True
        self._dbg("DriverManager.start(): complete.")

    def stop(self) -> None:
        """
        Stop manager then driver. Never raises.
        """
        self._dbg("DriverManager.stop(): stopping...")
        try:
            self.gps_pps_manager.stop()
        except Exception:
            self._dbg("DriverManager.stop(): manager stop raised, continuing.")
        try:
            self.gps_driver.stop()
        except Exception:
            self._dbg("DriverManager.stop(): driver stop raised, continuing.")
        self._started = False
        self._dbg("DriverManager.stop(): complete.")

    def is_started(self) -> bool:
        return self._started

    # -----------------
    # Data surface
    # -----------------

    def get_payload(self) -> Dict[str, Any]:
        """
        Return a single payload dict suitable for the dispatcher.

        Behavior:
- Never raises; returns status-rich payload even on partial failure.
- Includes both manager output (preferred) and a minimal driver snapshot (for debugging).
        """
        payload: Dict[str, Any] = {
            "subsystem": "weather_station",
            "module": "driver_manager",
            "started": self._started,
            "timestamp_monotonic": time.monotonic(),
            "status": "ok",
            "last_error": self._last_error,
            "last_error_monotonic": self._last_error_monotonic,
            "gps": None,
            "gps_raw": None,
        }

        if not self._started:
            payload["status"] = "not_started"
            return payload

        # Manager output is the primary surface for GPS + PPS.
        try:
            mgr_out = self.gps_pps_manager.get_output()
            payload["gps"] = mgr_out
        except Exception as e:
            self._record_error(f"Manager get_output failed: {type(e).__name__}: {e}")
            payload["status"] = "degraded"
            payload["gps"] = None
            self._dbg(self._last_error)
            self._dbg(traceback.format_exc())

        # Raw driver snapshot (secondary) to aid debugging.
        try:
            raw = self.gps_driver.get_snapshot()
            payload["gps_raw"] = {
                "gps_link_last_rx_mono": raw.get("last_nmea_rx_monotonic"),
                "nmea_rx_count": raw.get("nmea_rx_count"),
                "nmea_parse_errors": raw.get("nmea_parse_errors"),
                "has_fix_raw": raw.get("has_fix"),
                "satellites": raw.get("satellites"),
                "hdop": raw.get("hdop"),
                "lat_dd": raw.get("lat_dd"),
                "lon_dd": raw.get("lon_dd"),
                "alt_m": raw.get("alt_m"),
                "utc_valid": raw.get("utc_valid"),
                "utc_datetime": raw.get("utc_datetime"),
                "last_driver_error": raw.get("last_driver_error"),
            }
        except Exception as e:
            self._record_error(f"Driver get_snapshot failed: {type(e).__name__}: {e}")
            payload["status"] = "degraded"
            payload["gps_raw"] = None
            self._dbg(self._last_error)
            self._dbg(traceback.format_exc())

        return payload

    # -----------------
    # Internals
    # -----------------

    def _dbg(self, msg: str) -> None:
        if self.debug:
            print(msg)

    def _record_error(self, msg: str) -> None:
        self._last_error = msg
        self._last_error_monotonic = time.monotonic()


# Optional: standalone smoke test (not used by dispatcher)
if __name__ == "__main__":
    dm = DriverManager(
        gps_port="/dev/serial0",
        gps_baud=9600,
        gps_update_hz=1.0,
        pps_device="/dev/pps0",
        manager_loop_hz=5.0,
        debug=True,
    )

    try:
        dm.start()
        print("DriverManager started. Printing 20 payloads (1 per second).")
        for _ in range(20):
            p = dm.get_payload()
            gps = p.get("gps") or {}
            print(
                {
                    "status": p["status"],
                    "time_source": gps.get("time_source"),
                    "pps_present": gps.get("pps_present"),
                    "pps_locked": gps.get("pps_locked"),
                    "gps_link_ok": gps.get("gps_link_ok"),
                    "gps_position_valid": gps.get("gps_position_valid"),
                    "lat": gps.get("lat_dd"),
                    "lon": gps.get("lon_dd"),
                    "sats": gps.get("satellites"),
                    "hdop": gps.get("hdop"),
                    "node_time_s": gps.get("node_time_s"),
                    "last_error": p.get("last_error"),
                }
            )
            time.sleep(1.0)
    finally:
        dm.stop()

