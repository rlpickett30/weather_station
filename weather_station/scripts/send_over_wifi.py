# -*- coding: utf-8 -*-
"""
send_over_wifi.py

Transport-only module for pushing completed event dictionaries from a node
to another machine over Wi-Fi.

Version 1 transport:
- UDP + JSON
    - Events are serialized to UTF-8 JSON.
    - Sent as a single datagram to DEST_HOST:DEST_PORT.
    - No acknowledgements or retries.

Other code should call:
    send_event(event_dict)
"""

from __future__ import annotations

import base64
import json
import logging
import socket
from datetime import date, datetime
from typing import Any, Dict


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logger = logging.getLogger("birdstation.send_over_wifi")


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DEST_HOST: str = "192.168.1.192"
DEST_PORT: int = 50555

PROTOCOL: str = "udp"


# ---------------------------------------------------------------------------
# JSON helpers
# ---------------------------------------------------------------------------

def _json_default(obj: Any) -> Any:
    """
    JSON serializer fallback for types that are not natively serializable.

    Supports:
    - datetime/date -> ISO 8601 string
    - bytes/bytearray -> base64 string wrapper
    - set/tuple -> list
    - everything else -> string representation
    """
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()

    if isinstance(obj, (bytes, bytearray)):
        b64 = base64.b64encode(bytes(obj)).decode("ascii")
        return {"__bytes_b64__": b64}

    if isinstance(obj, (set, tuple)):
        return list(obj)

    # Last resort: do not crash the sender because one field is “weird”.
    return str(obj)


# ---------------------------------------------------------------------------
# Core send logic
# ---------------------------------------------------------------------------

def _send_udp(payload: bytes) -> None:
    """
    Low-level UDP sender for already-encoded payloads.
    """
    logger.debug(
        "Preparing to send UDP packet to %s:%s (len=%d bytes)",
        DEST_HOST,
        DEST_PORT,
        len(payload),
    )

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sent = sock.sendto(payload, (DEST_HOST, DEST_PORT))
        logger.debug("UDP send successful, bytes sent: %d", sent)
    finally:
        sock.close()


def send_event(event: Dict[str, Any]) -> None:
    """
    Serialize an event dict to JSON and send it over Wi-Fi using the configured
    transport (currently UDP only).

    Args:
        event: A dictionary representing the event. It may contain datetimes;
               this module will safely serialize them.

    Raises:
        ValueError: If PROTOCOL is unsupported.
        OSError: For underlying socket/network errors.
        TypeError/ValueError: If JSON serialization fails in an unrecoverable way.
    """
    logger.debug("send_event() called with event keys: %s", list(event.keys()))

    # Serialize to JSON with safe fallback for datetime and similar types.
    try:
        payload_str = json.dumps(event, ensure_ascii=False, default=_json_default)
    except (TypeError, ValueError) as exc:
        logger.error("Failed to serialize event to JSON: %s", exc)
        raise

    payload_bytes = payload_str.encode("utf-8")
    logger.debug("Event serialized to %d bytes of JSON.", len(payload_bytes))

    # Dispatch based on protocol.
    if PROTOCOL.lower() == "udp":
        _send_udp(payload_bytes)
        return

    logger.error("Unsupported PROTOCOL '%s' in send_over_wifi.py", PROTOCOL)
    raise ValueError(f"Unsupported PROTOCOL '{PROTOCOL}'")


# ---------------------------------------------------------------------------
# Standalone debug/demo
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    logger.info("Running send_over_wifi.py as a script for debug/demo.")
    logger.info(
        "Destination is %s:%s over %s",
        DEST_HOST,
        DEST_PORT,
        PROTOCOL.upper(),
    )

    example_event: Dict[str, Any] = {
        "event_id": "TEST_EVENT_0001",
        "node_id": "yard_station_1",
        "timestamp_utc": datetime.utcnow(),  # intentionally datetime to prove safety
        "weather": None,
        "bird": {"bird_id": "AMRO", "species_name": "American Robin", "confidence": 0.99},
    }

    try:
        send_event(example_event)
        logger.info("Example event sent successfully.")
    except Exception as exc:
        logger.exception("Failed to send example event: %s", exc)
