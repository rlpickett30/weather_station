WeatherStation (Raspberry Pi)

A modular, hardware-driven weather station designed for Raspberry Pi, built as a standalone peer service alongside BirdStation.
This repository handles environmental sensors, GPS, PPS timing, and system reliability, and is intended to run continuously under systemd.
---
Supported Hardware (Current)

Raspberry Pi (tested on Pi 4 / Pi 5)

BMP390 – Barometric pressure + temperature (I²C)

SHT45 – Temperature + humidity (I²C)

Legacy support retained for SHT31D

Adafruit Ultimate GPS v3 – GPS + PPS (UART + GPIO)
---
Operating System Assumptions

Raspberry Pi OS (Bookworm or newer)

-I²C enabled

-UART enabled

Python 3 (system Python)

Fresh SD card recommended for first install
---
### 1. System Preparation

Update the system and reboot:
```bash
sudo apt update
sudo apt full-upgrade -y
sudo reboot
```

Install required system packages:
```bash
sudo apt install -y \
git \
python3 \
python3-venv \
python3-pip \
i2c-tools \
build-essential \
python3-dev
```
### 2. Enable Interfaces

Run Raspberry Pi configuration:
```bash
sudo raspi-config
```

Enable:

I²C

Serial / UART (disable serial console, enable hardware serial)

Then reboot to apply changes:

sudo reboot

### 3. Clone Repository

From your home directory:
```bash
cd ~
git clone https://github.com/rlpickett30/weather_station.git
cd weather_station
```
### 4. Create Python Virtual Environment

Create and activate a virtual environment at the repository root:
```bash
python3 -m venv .venv
source .venv/bin/activate
```

Upgrade tooling and Python dependencies:
```bash
pip install --upgrade pip setuptools wheel
pip install -r weather_station/scripts/requirements.txt
```
### 5. Sensor Driver Validation (Recommended)

Test each driver individually to confirm wiring and I²C health.

BMP390
```bash
cd weather_station/scripts
python BMP390_driver.py
```

Expected: pressure, temperature, and altitude samples printed once per second.

SHT45
```bash
python SHT45_driver.py
```

Expected: temperature and humidity samples printed once per second.

Press Ctrl+C to exit each test.

### 6. Run Weather Dispatcher Manually

From the repository root:
```bash
cd ~/weather_station
python weather_station/scripts/weather_dispatcher.py
```

You should see periodic status snapshots similar to:

{
  'status': 'ok',
  'time_source': 'pi_clock',
  'gps_link_ok': True,
  'gps_valid': False,
  'pps_present': False,
  'node_time_s': 62345
}


Note:

gps_valid: False is expected indoors.

PPS will report inactive until the GPS is locked and PPS is configured at the kernel level.

### 7. Systemd (Auto-Start on Boot)
Create the service file
```bash
sudo nano /etc/systemd/system/weatherstation-dispatcher.service
```

Paste the following:
```bash
[Unit]
Description=WeatherStation Dispatcher
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=node0
WorkingDirectory=/home/node0/weather_station
Environment=PYTHONUNBUFFERED=1
ExecStart=/home/node0/weather_station/.venv/bin/python /home/node0/weather_station/weather_station/scripts/weather_dispatcher.py
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
```

Save and exit.

Enable and start the service
```bash
sudo systemctl daemon-reload
sudo systemctl enable weatherstation-dispatcher.service
sudo systemctl start weatherstation-dispatcher.service
```
Verify operation
```bash
systemctl status weatherstation-dispatcher.service --no-pager
journalctl -u weatherstation-dispatcher.service -f
```
Architecture Notes

WeatherStation runs as a standalone service

It does not import BirdStation

It is safe to run concurrently with other systemd services

All hardware access is isolated to this repository and venv

Failures restart automatically under systemd
