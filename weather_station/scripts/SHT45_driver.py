#!/usr/bin/env python3
"""
SHT45_driver.py

Persistent, long-lived I2C driver for the Sensirion SHT45 (SHT4x family).

Design intent:
- The driver OWNS the sensor for the lifetime of the process.
- It periodically samples temperature and humidity and keeps a "latest snapshot".
- It exposes raw facts; policy decisions belong elsewhere.
"""

from __future__ import annotations

import threading
import time
from typing import Any, Dict, Optional

import board
import busio
import adafruit_sht4x


class SHT45Driver:
    def __init__(self, *, sample_hz: float = 1.0) -> None:
        if sample_hz <= 0:
            raise ValueError("sample_hz must be > 0")

        self.sample_hz = float(sample_hz)

        self._i2c: Optional[busio.I2C] = None
        self._sensor: Optional[adafruit_sht4x.SHT4x] = None

        self._lock = threading.Lock()
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._started = False

        now_mono = time.monotonic()
        self._state: Dict[str, Any] = {
            "driver": "SHT45Driver",
            "sample_hz": self.sample_hz,
            "driver_start_monotonic": None,
            "last_sample_monotonic": None,
            "sample_count": 0,
            "last_error": None,
            "last_error_monotonic": None,
            "temperature_c": None,
            "humidity_rh": None,
            "snapshot_monotonic": now_mono,
        }

    def start(self) -> None:
        if self._started:
            return

        # I2C bus (Pi: I2C1)
        self._i2c = busio.I2C(board.SCL, board.SDA)

        # Sensor
        self._sensor = adafruit_sht4x.SHT4x(self._i2c)

        # Recommended: high precision; heater off by default
        self._sensor.mode = adafruit_sht4x.Mode.NOHEAT_HIGHPRECISION

        with self._lock:
            self._state["driver_start_monotonic"] = time.monotonic()
            self._state["last_error"] = None
            self._state["last_error_monotonic"] = None
            self._state["snapshot_monotonic"] = time.monotonic()

        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, name="SHT45DriverThread", daemon=True)
        self._thread.start()
        self._started = True

    def stop(self) -> None:
        self._stop_event.set()
        t = self._thread
        if t and t.is_alive():
            t.join(timeout=2.0)
        self._thread = None
        self._started = False
        self._sensor = None
        self._i2c = None

    def get_snapshot(self) -> Dict[str, Any]:
        with self._lock:
            snap = dict(self._state)
            snap["snapshot_monotonic"] = time.monotonic()
            return snap

    def is_started(self) -> bool:
        return self._started

    def _run(self) -> None:
        assert self._sensor is not None

        period = 1.0 / self.sample_hz
        while not self._stop_event.is_set():
            try:
                t_c, rh = self._sensor.measurements

                with self._lock:
                    self._state["temperature_c"] = float(t_c) if t_c is not None else None
                    self._state["humidity_rh"] = float(rh) if rh is not None else None
                    self._state["last_sample_monotonic"] = time.monotonic()
                    self._state["sample_count"] += 1
                    self._state["last_error"] = None
                    self._state["last_error_monotonic"] = None
                    self._state["snapshot_monotonic"] = time.monotonic()

            except Exception as e:
                with self._lock:
                    self._state["last_error"] = f"{type(e).__name__}: {e}"
                    self._state["last_error_monotonic"] = time.monotonic()
                    self._state["snapshot_monotonic"] = time.monotonic()

            time.sleep(period)


if __name__ == "__main__":
    d = SHT45Driver(sample_hz=1.0)
    try:
        d.start()
        print("SHT45 driver started. Printing 10 samples.")
        for _ in range(10):
            print(d.get_snapshot())
            time.sleep(1.0)
    finally:
        d.stop()
        print("SHT45 driver stopped.")
