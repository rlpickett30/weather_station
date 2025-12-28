#!/usr/bin/env python3
"""
node_database.py

Offline event queue for a node. Stores events locally in SQLite and flushes them
when transport becomes available again.

Key properties:
- Queue accepts raw Python dict events (may contain datetime objects).
- JSON serialization is handled here (database boundary), not assumed upstream.
- Flush marks events as sent only after send_func succeeds.
"""

from __future__ import annotations

import base64
import json
import sqlite3
from datetime import date, datetime, timezone
from typing import Any, Callable, Dict, Optional, Tuple


SendFunc = Callable[[Dict[str, Any]], Any]


def _utc_now_iso() -> str:
    # ISO 8601 with timezone (UTC) for unambiguous storage.
    return datetime.now(timezone.utc).isoformat()


def _json_default(obj: Any) -> Any:
    """
    Safe fallback encoder for json.dumps(default=...).

    Supports:
    - datetime/date -> ISO 8601 string
    - bytes/bytearray -> base64 wrapper dict
    - set/tuple -> list
    - fallback -> string representation
    """
    if isinstance(obj, (datetime, date)):
        # If naive datetime slips through, serialize it as-is.
        # Prefer upstream creating timezone-aware timestamps, but do not break.
        return obj.isoformat()

    if isinstance(obj, (bytes, bytearray)):
        b64 = base64.b64encode(bytes(obj)).decode("ascii")
        return {"__bytes_b64__": b64}

    if isinstance(obj, (set, tuple)):
        return list(obj)

    return str(obj)


class NodeDatabase:
    """
    Handles offline storage of events and flushing them later via send_func.
    """

    def __init__(self, db_path: str, *, send_func: Optional[SendFunc] = None) -> None:
        self.db_path = db_path
        self._send_func: Optional[SendFunc] = send_func
        self._ensure_schema()

    # ------------------------------------------------------------------
    # Configuration
    # ------------------------------------------------------------------

    def set_send_func(self, send_func: SendFunc) -> None:
        """
        Set a default send function for flush_pending().

        send_func(event_dict) may:
          - raise on failure, or
          - return False to indicate failure
        """
        self._send_func = send_func

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=10)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        return conn

    def _ensure_schema(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS queued_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    sent INTEGER NOT NULL DEFAULT 0
                )
                """
            )
            conn.commit()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def queue_event(self, event: Dict[str, Any]) -> None:
        """
        Store an event locally when the node cannot reach the server.
        """
        created_at = _utc_now_iso()
        event_type = str(event.get("event_type", "unknown"))

        # Critical fix: ensure JSON safety here.
        payload_json = json.dumps(event, ensure_ascii=False, default=_json_default)

        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO queued_events (created_at, event_type, payload_json, sent)
                VALUES (?, ?, ?, 0)
                """,
                (created_at, event_type, payload_json),
            )
            conn.commit()

    def flush_pending(self, send_func: Optional[SendFunc] = None) -> int:
        """
        Attempt to send all unsent events.

        Behavior:
        - Stops on first failure (network still down), leaving remaining events unsent.
        - Marks events as sent only after send succeeds.
        - Returns the number of events successfully flushed.

        send_func(event_dict) must:
          - send the event to the server (UDP, etc.)
          - raise on failure OR return False on failure
        """
        func = send_func or self._send_func
        if func is None:
            raise ValueError("flush_pending() requires send_func, or call set_send_func(send_func) first.")

        sent_count = 0

        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT id, payload_json
                FROM queued_events
                WHERE sent = 0
                ORDER BY id ASC
                """
            )
            rows: Tuple[Tuple[int, str], ...] = tuple(cur.fetchall())
            if not rows:
                return 0

            for row_id, payload_json in rows:
                event = json.loads(payload_json)

                try:
                    result = func(event)
                    if result is False:
                        break
                except Exception:
                    break

                cur.execute("UPDATE queued_events SET sent = 1 WHERE id = ?", (row_id,))
                sent_count += 1

            conn.commit()

        return sent_count

    def has_pending(self) -> bool:
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM queued_events WHERE sent = 0 LIMIT 1")
            return cur.fetchone() is not None
