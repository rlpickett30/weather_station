#!/usr/bin/env python3
"""
weather_dispatcher.py

WeatherStation Dispatcher (heartbeat mode).

Purpose:
- Periodically emit a node heartbeat containing GPS + environment status.
- Reuse NodeDatabase and send_over_wifi (UDP JSON transport).
- Run independently under systemd with clean failure isolation.

Testing cadence: 10 seconds
Production cadence: 300 seconds (5 minutes)
"""

from __future__ import annotations

import signal
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict

from driver_manager import DriverManager
from node_database import NodeDatabase
import send_over_wifi


# -----------------
# Configuration
# -----------------

NODE_ID = "node0"

#HEARTBEAT_PERIOD_S = 10.0   # testing cadence
HEARTBEAT_PERIOD_S = 300.0  # production cadence (5 minutes)

DB_PATH = "/home/node0/weather_station/runtime/weatherstation_events.db"


class WeatherDispatcher:
    """
    Top-level dispatcher for WeatherStation heartbeat events.
    """

    def __init__(self, *, debug: bool = True) -> None:
        self.debug = debug
        self._running = False

        self.driver_manager = DriverManager(debug=debug)
        self.db = NodeDatabase(DB_PATH)
        self.db.set_send_func(send_over_wifi.send_event)

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
        except Exception as exc:
            self._dbg(f"WeatherDispatcher.stop(): DriverManager stop error (ignored): {exc}")
        finally:
            self._running = False

        self._dbg("WeatherDispatcher.stop(): stopped.")

    # -----------------
    # Main loop
    # -----------------

    def run_forever(self) -> None:
        """
        Blocking dispatcher loop.
        """
        self.start()

        try:
            while self._running:
                loop_start = time.monotonic()

                try:
                    self._dispatch_once()
                except Exception as exc:
                    # Absolute containment: never let a heartbeat crash the service.
                    self._dbg(f"WeatherDispatcher: unexpected dispatch error (contained): {exc}")

                elapsed = time.monotonic() - loop_start
                sleep_s = HEARTBEAT_PERIOD_S - elapsed
                if sleep_s > 0:
                    time.sleep(sleep_s)

        except KeyboardInterrupt:
            self._dbg("WeatherDispatcher: KeyboardInterrupt received.")
        finally:
            self.stop()

    def _dispatch_once(self) -> None:
        payload = self.driver_manager.get_payload()

        event: Dict[str, Any] = {
            "event_type": "node_heartbeat",
            "node_id": NODE_ID,
            "subsystem": "weatherstation",
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "payload": payload,
        }

        # Attempt immediate send. If it fails, queue offline.
        try:
            send_over_wifi.send_event(event)
            self._dbg("Heartbeat sent successfully.")

            # If your NodeDatabase supports pending flush, do it opportunistically.
            # Any flush errors should not crash the dispatcher.
            try:
                self.db.flush_pending()
            except Exception as exc:
                self._dbg(f"flush_pending() error (ignored): {exc}")

        except Exception as exc:
            self._dbg(f"Send failed, queueing heartbeat offline: {exc}")
            # Queue the whole event dict. NodeDatabase should pull fields it needs.
            self.db.queue_event(event)

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
    dispatcher = WeatherDispatcher(debug=True)
    _install_signal_handlers(dispatcher)
    dispatcher.run_forever()
