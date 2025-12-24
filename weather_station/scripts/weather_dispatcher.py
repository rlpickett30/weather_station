"""
weather_dispatcher.py
---------------------
WeatherStation Dispatcher (GPS-only, console output).

Purpose:
- Serve as the top-level runtime loop for the WeatherStation.
- Import DriverManager and consume its payload.
- Print clean, human-readable status to the command line.
- Mirror the structural role of BirdStation's dispatcher, without transport logic.

Scope (for now):
- No WiFi, no LoRa, no persistence.
- GPS + PPS only.
- Output is purely diagnostic and observational.

Design principles:
- Dispatcher never talks directly to drivers.
- Dispatcher never applies validation or filtering.
- Dispatcher never crashes on partial failure.
"""

from __future__ import annotations

import time
import signal
import sys
from typing import Optional

from driver_manager import DriverManager


class WeatherDispatcher:
    """
    Top-level dispatcher loop for the WeatherStation.
    """

    def __init__(
        self,
        *,
        loop_hz: float = 1.0,
        debug: bool = True,
    ) -> None:
        if loop_hz <= 0:
            raise ValueError("loop_hz must be > 0")

        self.loop_hz = loop_hz
        self.debug = debug
        self._running = False

        self.driver_manager = DriverManager(debug=debug)

    # -----------------
    # Lifecycle
    # -----------------

    def start(self) -> None:
        if self._running:
            return

        self._dbg("WeatherDispatcher.start(): starting DriverManager...")
        self.driver_manager.start()
        self._running = True
        self._dbg("WeatherDispatcher.start(): started.")

    def stop(self) -> None:
        if not self._running:
            return

        self._dbg("WeatherDispatcher.stop(): stopping...")
        try:
            self.driver_manager.stop()
        except Exception:
            pass
        self._running = False
        self._dbg("WeatherDispatcher.stop(): stopped.")

    # -----------------
    # Main loop
    # -----------------

    def run_forever(self) -> None:
        """
        Blocking dispatcher loop.
        """
        period = 1.0 / self.loop_hz
        self.start()

        try:
            while self._running:
                loop_start = time.monotonic()
                self._dispatch_once()

                elapsed = time.monotonic() - loop_start
                sleep_s = period - elapsed
                if sleep_s > 0:
                    time.sleep(sleep_s)

        except KeyboardInterrupt:
            self._dbg("WeatherDispatcher: KeyboardInterrupt received.")
        finally:
            self.stop()

    def _dispatch_once(self) -> None:
        payload = self.driver_manager.get_payload()

        status = payload.get("status")
        gps = payload.get("gps") or {}

        # Build a compact, readable line similar to BirdStation-style diagnostics.
        line = {
            "status": status,
            "time_source": gps.get("time_source"),
            "pps_present": gps.get("pps_present"),
            "pps_locked": gps.get("pps_locked"),
            "gps_link_ok": gps.get("gps_link_ok"),
            "gps_valid": gps.get("gps_position_valid"),
            "lat": gps.get("lat_dd"),
            "lon": gps.get("lon_dd"),
            "alt_m": gps.get("alt_m"),
            "sats": gps.get("satellites"),
            "hdop": gps.get("hdop"),
            "node_time_s": gps.get("node_time_s"),
        }

        print(line)

        # Optional verbose diagnostics
        if self.debug and payload.get("last_error"):
            print("  ERROR:", payload["last_error"])

    # -----------------
    # Utilities
    # -----------------

    def _dbg(self, msg: str) -> None:
        if self.debug:
            print(msg)


# -----------------
# Signal handling
# -----------------

def _install_signal_handlers(dispatcher: WeatherDispatcher) -> None:
    def _handle_signal(signum, frame) -> None:
        print(f"\nWeatherDispatcher: received signal {signum}, shutting down.")
        dispatcher.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)


# -----------------
# Entry point
# -----------------

if __name__ == "__main__":
    dispatcher = WeatherDispatcher(loop_hz=1.0, debug=True)
    _install_signal_handlers(dispatcher)
    dispatcher.run_forever()
