#!/usr/bin/env python3
"""
BMP390_driver.py

Persistent, long-lived I2C driver for the Bosch BMP390 (BMP3xx family).

Design intent:
- The driver OWNS the sensor for the lifetime of the process.
- It periodically samples pressure (and temperature) and keeps a "latest snapshot".
- It exposes raw facts; policy decisions belong elsewhere.
"""

from __future__ import annotations

import threading
import time
from typing import Any, Dict, Optional

import board
import busio
import adafruit_bmp3xx


class BMP390Driver:
    def __init__(self, *, sample_hz: float = 1.0, sea_level_pressure_hpa: float = 1013.25) -> None:
        if sample_hz <= 0:
            raise ValueError("sample_hz must be > 0")

        self.sample_hz = float(sample_hz)
        self.sea_level_pressure_hpa = float(sea_level_pressure_hpa)

        self._i2c: Optional[busio.I2C] = None
        self._sensor: Optional[adafruit_bmp3xx.BMP3XX_I2C] = None

        self._lock = threading.Lock()
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._started = False

        now_mono = time.monotonic()
        self._state: Dict[str, Any] = {
            "driver": "BMP390Driver",
            "sample_hz": self.sample_hz,
            "sea_level_pressure_hpa": self.sea_level_pressure_hpa,
            "driver_start_monotonic": None,
            "last_sample_monotonic": None,
            "sample_count": 0,
            "last_error": None,
            "last_error_monotonic": None,
            "pressure_hpa": None,
            "temperature_c": None,
            "altitude_m": None,
            "snapshot_monotonic": now_mono,
        }

    def start(self) -> None:
        if self._started:
            return

        self._i2c = busio.I2C(board.SCL, board.SDA)

        # Auto-detect address by trying 0x77 then 0x76 (common BMP3xx addresses).
        sensor = None
        last_exc: Optional[Exception] = None
        for addr in (0x77, 0x76):
            try:
                sensor = adafruit_bmp3xx.BMP3XX_I2C(self._i2c, address=addr)
                break
            except Exception as e:
                last_exc = e

        if sensor is None:
            raise RuntimeError(f"Could not initialize BMP390 at 0x77 or 0x76: {last_exc}")

        self._sensor = sensor

        # Reasonable default oversampling + filtering for weather station use.
        self._sensor.pressure_oversampling = 8
        self._sensor.temperature_oversampling = 2
        self._sensor.filter_coefficient = 4

        # Enables altitude calculation; altitude will be meaningful only if sea level pressure is correct.
        self._sensor.sea_level_pressure = self.sea_level_pressure_hpa

        with self._lock:
            self._state["driver_start_monotonic"] = time.monotonic()
            self._state["last_error"] = None
            self._state["last_error_monotonic"] = None
            self._state["snapshot_monotonic"] = time.monotonic()

        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, name="BMP390DriverThread", daemon=True)
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
                # Adafruit library returns pressure in hPa and temperature in °C.
                p_hpa = float(self._sensor.pressure)
                t_c = float(self._sensor.temperature)

                # Altitude is derived from sea-level pressure; it is optional but useful.
                alt_m = float(self._sensor.altitude)

                with self._lock:
                    self._state["pressure_hpa"] = p_hpa
                    self._state["temperature_c"] = t_c
                    self._state["altitude_m"] = alt_m
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
    d = BMP390Driver(sample_hz=1.0, sea_level_pressure_hpa=1013.25)
    try:
        d.start()
        print("BMP390 driver started. Printing 10 samples.")
        for _ in range(10):
            print(d.get_snapshot())
            time.sleep(1.0)
    finally:
        d.stop()
        print("BMP390 driver stopped.")
