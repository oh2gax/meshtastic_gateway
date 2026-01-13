#!/usr/bin/env python3
"""
Meshtastic WiFi (TCP) gateway + SQLite + simple Flask UI
- Inbox/outbox stored in SQLite
- Active nodes list with selectable time windows
- Map with auto-refresh markers + sidebar + per-node track toggles
- Debug terminal with pause/resume, copy-to-clipboard, and filters

Notes:
- SQLite uses WAL mode, so you'll see .db-wal and .db-shm files next to the main db.
"""

import json
import sqlite3
import time
import threading
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional, List, Tuple

from flask import Flask, request, redirect, url_for, render_template_string, abort, jsonify
from meshtastic.tcp_interface import TCPInterface
from pubsub import pub

# -------------------------
# Configuration
# -------------------------
HOST = "192.168.1.211"
DB_PATH = "/home/oh2gax/meshtastic/meshtastic_messages.db"

WEB_HOST = "0.0.0.0"     # "127.0.0.1" for local-only
WEB_PORT = 8000

DEFAULT_ACTIVE_WINDOW = "6h"   # 2h, 6h, 1d, 1w, 1m supported
DEFAULT_CHAT_LIMIT = 20
DEFAULT_MAP_TRACK_POINTS = 60

OUTBOX_MAX_TRIES = 3

DEBUG_BUFFER_MAX = 300         # in-memory packet buffer (not stored in DB)
DEBUG_SHOW_DEFAULT = 40        # show last N entries by default


# -------------------------
# Utilities
# -------------------------
def utc_ts() -> int:
    return int(time.time())


def iso_utc(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def parse_window_to_seconds(window: str) -> int:
    """Parse '2h', '6h', '1d', '1w', '1m' into seconds. Defaults to 6h if unknown."""
    w = (window or "").strip().lower()
    m = {"2h": 2 * 3600, "6h": 6 * 3600, "1d": 24 * 3600, "1w": 7 * 24 * 3600, "1m": 30 * 24 * 3600}
    return m.get(w, 6 * 3600)


def safe_get(d: Dict[str, Any], *path, default=None):
    cur: Any = d
    for p in path:
        if not isinstance(cur, dict) or p not in cur:
            return default
        cur = cur[p]
    return cur


def parse_position_from_node(node: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Extract lat/lon/alt/time from iface.nodes entry if present."""
    if not isinstance(node, dict):
        return None
    pos = node.get("position")
    if not isinstance(pos, dict):
        return None
    lat = pos.get("latitude")
    lon = pos.get("longitude")
    alt = pos.get("altitude")
    t = pos.get("time")
    if lat is None or lon is None:
        return None
    return {"lat": float(lat), "lon": float(lon), "alt": alt, "time": t}


def parse_position_from_packet(packet: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Best-effort: try to detect lat/lon/alt/time in packet.
    Positions are also seeded from iface.nodes on connect.
    """
    if not isinstance(packet, dict):
        return None

    pos = packet.get("position")
    if isinstance(pos, dict) and "latitude" in pos and "longitude" in pos:
        try:
            return {
                "lat": float(pos["latitude"]),
                "lon": float(pos["longitude"]),
                "alt": pos.get("altitude"),
                "time": pos.get("time", packet.get("rxTime")),
            }
        except Exception:
            pass

    decoded = packet.get("decoded")
    if not isinstance(decoded, dict):
        return None

    for candidate in (decoded, decoded.get("position"), decoded.get("payload")):
        if isinstance(candidate, dict):
            lat = candidate.get("latitude")
            lon = candidate.get("longitude")
            if lat is not None and lon is not None:
                try:
                    return {
                        "lat": float(lat),
                        "lon": float(lon),
                        "alt": candidate.get("altitude"),
                        "time": candidate.get("time", packet.get("rxTime")),
                    }
                except Exception:
                    pass

    return None


def packet_type(packet: Dict[str, Any]) -> str:
    """Classify packet into 'text', 'position', or 'other' for debug filtering."""
    try:
        dec = packet.get("decoded", {})
        port = dec.get("portnum")
        if port == "TEXT_MESSAGE_APP" or dec.get("text") is not None:
            return "text"
        if parse_position_from_packet(packet) is not None:
            return "position"
    except Exception:
        pass
    return "other"


def pretty_packet(packet: Dict[str, Any]) -> str:
    """Full raw packet dump for debug terminal (pretty-printed JSON)."""
    try:
        if isinstance(packet, dict):
            return json.dumps(packet, ensure_ascii=False, indent=2, default=str)
        return str(packet)
    except Exception:
        return str(packet)


def node_display(long_name: Optional[str], short_name: Optional[str], node_id: Optional[str]) -> str:
    """
    Format: 'OH2GAX-2 (GAX2)' when available, else fall back to node_id.
    """
    node_id = node_id or ""
    ln = (long_name or "").strip()
    sn = (short_name or "").strip()
    if ln and sn:
        return f"{ln} ({sn})"
    if ln:
        return ln
    if sn:
        return sn
    return node_id




BROADCAST_IDS = {"^all", "!ffffffff", "ffffffff", "0xffffffff"}

def is_broadcast_id(to_id: Optional[str]) -> bool:
    if to_id is None:
        return True
    tid = str(to_id).strip().lower()
    return tid == "" or tid in BROADCAST_IDS


def _find_first_number(obj: Any, keys: List[str]) -> Optional[float]:
    if isinstance(obj, dict):
        for k, v in obj.items():
            lk = str(k).lower()
            if lk in keys:
                if isinstance(v, (int, float)):
                    return float(v)
            found = _find_first_number(v, keys)
            if found is not None:
                return found
    elif isinstance(obj, list):
        for v in obj:
            found = _find_first_number(v, keys)
            if found is not None:
                return found
    return None


def parse_telemetry(packet: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not isinstance(packet, dict):
        return None
    dec = packet.get("decoded")
    if not isinstance(dec, dict):
        return None
    if dec.get("portnum") != "TELEMETRY_APP":
        return None
    blob: Any = packet
    tel = dec.get("telemetry")
    if isinstance(tel, dict):
        blob = {"decoded": dec, "telemetry": tel, "packet": packet}
    temp = _find_first_number(blob, ["temperature", "temperaturec", "temp", "temp_c"])
    lux = _find_first_number(blob, ["lux", "illuminance"])
    voltage = _find_first_number(blob, ["voltage", "batteryvoltage", "vbat"])
    batt = _find_first_number(blob, ["batterylevel", "battery_level", "battery"])
    if temp is None and lux is None and voltage is None and batt is None:
        return None
    return {"temperature_c": temp, "lux": lux, "voltage": voltage, "battery_level": batt}
# -------------------------
# Debug buffer (in-memory)
# -------------------------
@dataclass
class DebugEntry:
    ts: int
    from_id: str
    typ: str         # text / position / other
    text: str        # pretty packet (multiline)


DEBUG_LOCK = threading.Lock()
DEBUG_BUFFER: deque[DebugEntry] = deque(maxlen=DEBUG_BUFFER_MAX)


def debug_add(packet: Dict[str, Any]) -> None:
    try:
        entry = DebugEntry(
            ts=utc_ts(),
            from_id=str(packet.get("fromId") or ""),
            typ=packet_type(packet),
            text=pretty_packet(packet),
        )
    except Exception:
        entry = DebugEntry(ts=utc_ts(), from_id="", typ="other", text=str(packet))

    with DEBUG_LOCK:
        DEBUG_BUFFER.appendleft(entry)


def debug_clear() -> None:
    with DEBUG_LOCK:
        DEBUG_BUFFER.clear()


def debug_filtered(n: int, typ: str, node_id: str) -> List[DebugEntry]:
    n = max(1, min(n, DEBUG_BUFFER_MAX))
    t = (typ or "all").strip().lower()
    node_id = (node_id or "").strip()

    with DEBUG_LOCK:
        entries = list(DEBUG_BUFFER)

    out: List[DebugEntry] = []
    for e in entries:
        if t != "all" and e.typ != t:
            continue
        if node_id and e.from_id != node_id:
            continue
        out.append(e)
        if len(out) >= n:
            break
    return out


# -------------------------
# Database layer
# -------------------------
DB_LOCK = threading.Lock()


def db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def _col_exists(cur: sqlite3.Cursor, table: str, col: str) -> bool:
    cur.execute(f"PRAGMA table_info({table});")
    return any(r[1] == col for r in cur.fetchall())


def init_db(conn: sqlite3.Connection) -> None:
    with DB_LOCK:
        cur = conn.cursor()

        cur.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts INTEGER NOT NULL,
            ts_iso TEXT NOT NULL,
            direction TEXT NOT NULL,          -- 'in' or 'out'
            from_id TEXT,
            to_id TEXT,
            from_num INTEGER,
            to_num INTEGER,
            channel INTEGER,
            text TEXT,
            rx_rssi REAL,
            rx_snr REAL,
            hop_limit INTEGER,
            hop_start INTEGER,
            want_ack INTEGER,
            packet_id INTEGER,
            raw_json TEXT
        );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_ts ON messages(ts);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_dir_ts ON messages(direction, ts);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_from ON messages(from_id, ts);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_to ON messages(to_id, ts);")

        cur.execute("""
        CREATE TABLE IF NOT EXISTS outbox (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_ts INTEGER NOT NULL,
            created_ts_iso TEXT NOT NULL,
            status TEXT NOT NULL,             -- 'queued', 'sent', 'failed'
            to_id TEXT,                       -- '!abcd1234' or NULL for broadcast
            channel INTEGER,
            text TEXT NOT NULL,
            tries INTEGER NOT NULL DEFAULT 0,
            last_error TEXT
        );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_outbox_status ON outbox(status, created_ts);")

        cur.execute("""
        CREATE TABLE IF NOT EXISTS nodes (
            node_id TEXT PRIMARY KEY,         -- '!db29583c'
            node_num INTEGER,
            long_name TEXT,
            short_name TEXT,
            hw_model TEXT,
            macaddr TEXT,
            last_seen_ts INTEGER,
            last_seen_iso TEXT,
            last_rssi REAL,
            last_snr REAL,
            last_lat REAL,
            last_lon REAL,
            last_alt REAL,
            last_pos_ts INTEGER,
            last_pos_iso TEXT
        );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_nodes_last_seen ON nodes(last_seen_ts);")

        # Position history for track lines (last N per node maintained)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            node_id TEXT NOT NULL,
            ts INTEGER NOT NULL,
            ts_iso TEXT NOT NULL,
            lat REAL NOT NULL,
            lon REAL NOT NULL,
            alt REAL
        );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_positions_node_ts ON positions(node_id, ts);")

        # Migrate older DBs: add columns if missing
        for col, typ in [
            ("last_lat", "REAL"),
            ("last_lon", "REAL"),
            ("last_alt", "REAL"),
            ("last_pos_ts", "INTEGER"),
            ("last_pos_iso", "TEXT"),
        ]:
            if not _col_exists(cur, "nodes", col):
                try:
                    cur.execute(f"ALTER TABLE nodes ADD COLUMN {col} {typ};")
                except Exception:
                    pass

        if _col_exists(cur, "nodes", "last_pos_ts"):
            cur.execute("CREATE INDEX IF NOT EXISTS idx_nodes_last_pos ON nodes(last_pos_ts);")

        # Telemetry time-series
        cur.execute("""
        CREATE TABLE IF NOT EXISTS telemetry (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            node_id TEXT NOT NULL,
            ts INTEGER NOT NULL,
            ts_iso TEXT NOT NULL,
            temperature_c REAL,
            lux REAL,
            voltage REAL,
            battery_level REAL,
            raw_json TEXT
        );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_telemetry_node_ts ON telemetry(node_id, ts);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_telemetry_ts ON telemetry(ts);")

        conn.commit()


def upsert_node(conn: sqlite3.Connection,
                node_id: str,
                node_num: Optional[int] = None,
                long_name: Optional[str] = None,
                short_name: Optional[str] = None,
                hw_model: Optional[str] = None,
                macaddr: Optional[str] = None,
                last_seen_ts: Optional[int] = None,
                last_rssi: Optional[float] = None,
                last_snr: Optional[float] = None,
                lat: Optional[float] = None,
                lon: Optional[float] = None,
                alt: Optional[float] = None,
                pos_ts: Optional[int] = None) -> None:
    if not node_id:
        return

    seen_ts = last_seen_ts if last_seen_ts is not None else utc_ts()
    seen_iso = iso_utc(seen_ts)

    if pos_ts is None and (lat is not None and lon is not None):
        pos_ts = seen_ts
    pos_iso = iso_utc(pos_ts) if pos_ts else None

    with DB_LOCK:
        cur = conn.cursor()
        cur.execute("""
        INSERT INTO nodes(node_id, node_num, long_name, short_name, hw_model, macaddr,
                          last_seen_ts, last_seen_iso, last_rssi, last_snr,
                          last_lat, last_lon, last_alt, last_pos_ts, last_pos_iso)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(node_id) DO UPDATE SET
            node_num = COALESCE(excluded.node_num, nodes.node_num),
            long_name = COALESCE(excluded.long_name, nodes.long_name),
            short_name = COALESCE(excluded.short_name, nodes.short_name),
            hw_model = COALESCE(excluded.hw_model, nodes.hw_model),
            macaddr = COALESCE(excluded.macaddr, nodes.macaddr),
            last_seen_ts = COALESCE(excluded.last_seen_ts, nodes.last_seen_ts),
            last_seen_iso = COALESCE(excluded.last_seen_iso, nodes.last_seen_iso),
            last_rssi = COALESCE(excluded.last_rssi, nodes.last_rssi),
            last_snr = COALESCE(excluded.last_snr, nodes.last_snr),
            last_lat = COALESCE(excluded.last_lat, nodes.last_lat),
            last_lon = COALESCE(excluded.last_lon, nodes.last_lon),
            last_alt = COALESCE(excluded.last_alt, nodes.last_alt),
            last_pos_ts = COALESCE(excluded.last_pos_ts, nodes.last_pos_ts),
            last_pos_iso = COALESCE(excluded.last_pos_iso, nodes.last_pos_iso)
        """, (node_id, node_num, long_name, short_name, hw_model, macaddr,
              seen_ts, seen_iso, last_rssi, last_snr,
              lat, lon, alt, pos_ts, pos_iso))
        conn.commit()


def insert_position(conn: sqlite3.Connection, node_id: str, ts: int, lat: float, lon: float, alt: Optional[float],
                    keep_last_n: int) -> None:
    if not node_id:
        return
    keep_last_n = max(5, min(int(keep_last_n), 2000))

    with DB_LOCK:
        cur = conn.cursor()
        cur.execute("""
        INSERT INTO positions(node_id, ts, ts_iso, lat, lon, alt)
        VALUES(?,?,?,?,?,?)
        """, (node_id, ts, iso_utc(ts), lat, lon, alt))
        # prune older positions for that node to last N by ts
        cur.execute("""
        DELETE FROM positions
        WHERE node_id = ? AND id NOT IN (
            SELECT id FROM positions
            WHERE node_id = ?
            ORDER BY ts DESC, id DESC
            LIMIT ?
        )
        """, (node_id, node_id, keep_last_n))
        conn.commit()


def insert_telemetry(conn: sqlite3.Connection, node_id: str, ts: int,
                     temperature_c: Optional[float], lux: Optional[float],
                     voltage: Optional[float], battery_level: Optional[float],
                     raw: Dict[str, Any]) -> None:
    if not node_id:
        return
    with DB_LOCK:
        cur = conn.cursor()
        cur.execute("""
        INSERT INTO telemetry(node_id, ts, ts_iso, temperature_c, lux, voltage, battery_level, raw_json)
        VALUES(?,?,?,?,?,?,?,?)
        """, (node_id, ts, iso_utc(ts), temperature_c, lux, voltage, battery_level,
              json.dumps(raw, ensure_ascii=False, default=str)))
        conn.commit()


def telemetry_nodes(conn: sqlite3.Connection, window_seconds: int) -> List[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    cutoff = utc_ts() - window_seconds
    with DB_LOCK:
        cur = conn.cursor()
        cur.execute("""
        SELECT DISTINCT t.node_id, n.long_name, n.short_name
        FROM telemetry t
        LEFT JOIN nodes n ON n.node_id = t.node_id
        WHERE t.ts >= ?
        ORDER BY t.node_id
        """, (cutoff,))
        return cur.fetchall()


def telemetry_series(conn: sqlite3.Connection, node_id: str, window_seconds: int) -> List[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    cutoff = utc_ts() - window_seconds
    with DB_LOCK:
        cur = conn.cursor()
        cur.execute("""
        SELECT ts, ts_iso, temperature_c, lux, voltage, battery_level
        FROM telemetry
        WHERE node_id=? AND ts >= ?
        ORDER BY ts ASC, id ASC
        """, (node_id, cutoff))
        return cur.fetchall()


def telemetry_latest(conn: sqlite3.Connection, node_id: str) -> Optional[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    with DB_LOCK:
        cur = conn.cursor()
        cur.execute("""
        SELECT ts_iso, temperature_c, lux, voltage, battery_level
        FROM telemetry
        WHERE node_id=?
        ORDER BY ts DESC, id DESC
        LIMIT 1
        """, (node_id,))
        return cur.fetchone()


def log_incoming_text(conn: sqlite3.Connection, packet: Dict[str, Any]) -> None:
    ts = utc_ts()
    from_id = packet.get("fromId")
    to_id = packet.get("toId")
    from_num = packet.get("from")
    to_num = packet.get("to")
    text = safe_get(packet, "decoded", "text")
    channel = safe_get(packet, "channel", default=None)

    rx_rssi = packet.get("rxRssi")
    rx_snr = packet.get("rxSnr")
    rx_time = packet.get("rxTime", ts)

    upsert_node(conn,
                node_id=str(from_id) if from_id else "",
                node_num=from_num,
                last_seen_ts=rx_time,
                last_rssi=rx_rssi,
                last_snr=rx_snr)

    with DB_LOCK:
        cur = conn.cursor()
        cur.execute("""
        INSERT INTO messages(
            ts, ts_iso, direction,
            from_id, to_id, from_num, to_num,
            channel, text,
            rx_rssi, rx_snr, hop_limit, hop_start, want_ack,
            packet_id, raw_json
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            ts, iso_utc(ts), "in",
            from_id, to_id, from_num, to_num,
            channel, text,
            rx_rssi, rx_snr,
            packet.get("hopLimit"), packet.get("hopStart"),
            1 if packet.get("wantAck") else 0,
            packet.get("id"),
            json.dumps(packet, ensure_ascii=False, default=str),
        ))
        conn.commit()


def log_outgoing_message(conn: sqlite3.Connection, to_id: Optional[str], channel: Optional[int],
                        text: str, status: str, err: Optional[str] = None) -> None:
    ts = utc_ts()
    with DB_LOCK:
        cur = conn.cursor()
        cur.execute("""
        INSERT INTO messages(
            ts, ts_iso, direction,
            from_id, to_id, channel, text, raw_json
        ) VALUES(?,?,?,?,?,?,?,?)
        """, (ts, iso_utc(ts), "out", None, to_id, channel, text,
              json.dumps({"status": status, "err": err}, ensure_ascii=False)))
        conn.commit()


def queue_outbox(conn: sqlite3.Connection, to_id: Optional[str], channel: Optional[int], text: str) -> None:
    ts = utc_ts()
    with DB_LOCK:
        cur = conn.cursor()
        cur.execute("""
        INSERT INTO outbox(created_ts, created_ts_iso, status, to_id, channel, text, tries, last_error)
        VALUES(?,?,?,?,?,?,0,NULL)
        """, (ts, iso_utc(ts), "queued", to_id, channel, text))
        conn.commit()


def fetch_one_outbox(conn: sqlite3.Connection) -> Optional[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    with DB_LOCK:
        cur = conn.cursor()
        cur.execute("""
        SELECT * FROM outbox
        WHERE status='queued'
        ORDER BY created_ts ASC
        LIMIT 1
        """)
        return cur.fetchone()


def mark_outbox(conn: sqlite3.Connection, row_id: int, status: str, tries: int, last_error: Optional[str]) -> None:
    with DB_LOCK:
        cur = conn.cursor()
        cur.execute("""
        UPDATE outbox
        SET status=?, tries=?, last_error=?
        WHERE id=?
        """, (status, tries, last_error, row_id))
        conn.commit()


def delete_message(conn: sqlite3.Connection, msg_id: int) -> None:
    with DB_LOCK:
        cur = conn.cursor()
        cur.execute("DELETE FROM messages WHERE id=?", (msg_id,))
        conn.commit()


def delete_outbox(conn: sqlite3.Connection, out_id: int) -> None:
    with DB_LOCK:
        cur = conn.cursor()
        cur.execute("DELETE FROM outbox WHERE id=?", (out_id,))
        conn.commit()


def list_inbox(conn: sqlite3.Connection, limit: int = 50) -> List[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    with DB_LOCK:
        cur = conn.cursor()
        cur.execute("""
        SELECT m.id, m.ts, m.ts_iso, m.from_id, m.to_id, m.text, m.rx_rssi, m.rx_snr, m.hop_limit,
               nf.long_name AS from_long, nf.short_name AS from_short
        FROM messages m
        LEFT JOIN nodes nf ON nf.node_id = m.from_id
        WHERE m.direction='in'
        AND (m.to_id IS NOT NULL AND lower(m.to_id) NOT IN ('^all','!ffffffff','ffffffff','0xffffffff'))
        ORDER BY m.ts DESC
        LIMIT ?
        """, (limit,))
        return cur.fetchall()


def list_outbox(conn: sqlite3.Connection, limit: int = 50) -> List[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    with DB_LOCK:
        cur = conn.cursor()
        cur.execute("""
        SELECT o.id, o.created_ts, o.created_ts_iso, o.status, o.to_id, o.channel, o.text, o.tries, o.last_error,
               nt.long_name AS to_long, nt.short_name AS to_short
        FROM outbox o
        LEFT JOIN nodes nt ON nt.node_id = o.to_id
        WHERE o.to_id IS NOT NULL
        ORDER BY o.created_ts DESC
        LIMIT ?
        """, (limit,))
        return cur.fetchall()


def list_active_nodes(conn: sqlite3.Connection, window_seconds: int) -> List[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    cutoff = utc_ts() - window_seconds
    with DB_LOCK:
        cur = conn.cursor()
        cur.execute("""
        SELECT node_id, node_num, long_name, short_name, hw_model, macaddr,
               last_seen_ts, last_seen_iso, last_rssi, last_snr,
               last_lat, last_lon, last_alt, last_pos_ts, last_pos_iso
        FROM nodes
        WHERE last_seen_ts IS NOT NULL AND last_seen_ts >= ?
        ORDER BY last_seen_ts DESC
        """, (cutoff,))
        return cur.fetchall()


def list_nodes_with_position(conn: sqlite3.Connection, window_seconds: int) -> List[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    cutoff = utc_ts() - window_seconds
    with DB_LOCK:
        cur = conn.cursor()
        cur.execute("""
        SELECT node_id, long_name, short_name, hw_model,
               last_seen_iso, last_rssi, last_snr,
               last_lat, last_lon, last_alt, last_pos_iso, last_seen_ts
        FROM nodes
        WHERE last_lat IS NOT NULL AND last_lon IS NOT NULL
          AND last_seen_ts IS NOT NULL AND last_seen_ts >= ?
        ORDER BY last_seen_ts DESC
        """, (cutoff,))
        return cur.fetchall()


def get_track_points(conn: sqlite3.Connection, node_id: str, limit: int) -> List[Tuple[float, float]]:
    conn.row_factory = sqlite3.Row
    limit = max(5, min(int(limit), 2000))
    with DB_LOCK:
        cur = conn.cursor()
        cur.execute("""
        SELECT lat, lon
        FROM positions
        WHERE node_id=?
        ORDER BY ts ASC, id ASC
        LIMIT ?
        """, (node_id, limit))
        rows = cur.fetchall()
    return [(float(r["lat"]), float(r["lon"])) for r in rows]


def chat_messages(conn: sqlite3.Connection, node_id: str, limit: int) -> List[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    limit = max(5, min(int(limit), 2000))
    with DB_LOCK:
        cur = conn.cursor()
        cur.execute("""
        SELECT m.id, m.ts, m.ts_iso, m.direction, m.from_id, m.to_id, m.text, m.rx_rssi, m.rx_snr,
               nf.long_name AS from_long, nf.short_name AS from_short,
               nt.long_name AS to_long, nt.short_name AS to_short
        FROM messages m
        LEFT JOIN nodes nf ON nf.node_id = m.from_id
        LEFT JOIN nodes nt ON nt.node_id = m.to_id
        WHERE (m.direction='in' AND m.from_id=?)
           OR (m.direction='out' AND m.to_id=?)
        ORDER BY m.ts DESC
        LIMIT ?
        """, (node_id, node_id, limit))
        return cur.fetchall()


def node_label_for_id(conn: sqlite3.Connection, node_id: str) -> str:
    conn.row_factory = sqlite3.Row
    with DB_LOCK:
        cur = conn.cursor()
        cur.execute("SELECT long_name, short_name FROM nodes WHERE node_id=? LIMIT 1", (node_id,))
        r = cur.fetchone()
    if r:
        return node_display(r["long_name"], r["short_name"], node_id)
    return node_id


# -------------------------
# Meshtastic gateway thread
# -------------------------
class Gateway:
    def __init__(self, host: str, conn: sqlite3.Connection):
        self.host = host
        self.db = conn
        self.iface: Optional[TCPInterface] = None
        self.stop_event = threading.Event()

        pub.subscribe(self.on_text, "meshtastic.receive.text")
        pub.subscribe(self.on_any_packet, "meshtastic.receive")

    def connect(self) -> TCPInterface:
        print(f"[GW] Connecting to Meshtastic TCP: {self.host}")
        iface = TCPInterface(hostname=self.host)
        print("[GW] Connected.")
        self.seed_nodes_from_iface(iface)
        return iface

    def seed_nodes_from_iface(self, iface: TCPInterface) -> None:
        try:
            nodes = getattr(iface, "nodes", {}) or {}
        except Exception:
            nodes = {}

        for node_id, node in nodes.items():
            if not isinstance(node, dict):
                continue
            user = node.get("user", {}) if isinstance(node.get("user"), dict) else {}
            pos = parse_position_from_node(node)
            upsert_node(
                self.db,
                node_id=str(user.get("id") or node_id),
                node_num=node.get("num"),
                long_name=user.get("longName"),
                short_name=user.get("shortName"),
                hw_model=user.get("hwModel"),
                macaddr=user.get("macaddr"),
                last_seen_ts=node.get("lastHeard"),
                lat=pos["lat"] if pos else None,
                lon=pos["lon"] if pos else None,
                alt=pos.get("alt") if pos else None,
                pos_ts=pos.get("time") if pos else None,
            )
            if pos and pos.get("time"):
                insert_position(self.db, str(user.get("id") or node_id), int(pos["time"]), pos["lat"], pos["lon"],
                                pos.get("alt"), DEFAULT_MAP_TRACK_POINTS)

    def on_any_packet(self, packet, interface):
        try:
            if isinstance(packet, dict):
                debug_add(packet)
            else:
                debug_add({"raw": str(packet)})
        except Exception:
            pass

        try:
            if not isinstance(packet, dict):
                return

            from_id = packet.get("fromId")
            from_num = packet.get("from")
            rx_rssi = packet.get("rxRssi")
            rx_snr = packet.get("rxSnr")
            rx_time = int(packet.get("rxTime", utc_ts()))

            # STORE_ALL_TELEMETRY_APP: store any TELEMETRY_APP packet (including localStats-only)
            try:
                if isinstance(decoded, dict) and decoded.get("portnum") == "TELEMETRY_APP":
                    node_id = packet.get("fromId") or packet.get("from")
                    ts = int(packet.get("rxTime") or packet.get("rx_time") or utc_ts())
                    tel_fields = parse_telemetry(packet) or {}
                    insert_telemetry(
                        self.db,
                        node_id=str(node_id) if node_id else "",
                        ts=ts,
                        temperature_c=tel_fields.get("temperature_c"),
                        lux=tel_fields.get("lux"),
                        voltage=tel_fields.get("voltage"),
                        battery_level=tel_fields.get("battery_level"),
                        raw=packet,
                    )
            except Exception:
                pass


            pos = parse_position_from_packet(packet)
            upsert_node(
                self.db,
                node_id=str(from_id) if from_id else "",
                node_num=from_num,
                last_seen_ts=rx_time,
                last_rssi=rx_rssi,
                last_snr=rx_snr,
                lat=pos["lat"] if pos else None,
                lon=pos["lon"] if pos else None,
                alt=pos.get("alt") if pos else None,
                pos_ts=int(pos.get("time")) if pos and pos.get("time") else None,
            )

            if pos:
                p_ts = int(pos.get("time") or rx_time)
                insert_position(self.db, str(from_id), p_ts, pos["lat"], pos["lon"], pos.get("alt"),
                                DEFAULT_MAP_TRACK_POINTS)


            # Telemetry parsing & storage
            tel = parse_telemetry(packet)
            if tel and from_id:
                insert_telemetry(self.db, str(from_id), rx_time, tel.get("temperature_c"), tel.get("lux"),
                                 tel.get("voltage"), tel.get("battery_level"), packet)
        except Exception:
            pass

    def on_text(self, packet, interface):
        try:
            if isinstance(packet, dict):
                log_incoming_text(self.db, packet)
                txt = safe_get(packet, "decoded", "text", default="")
                print(f"[GW] IN {packet.get('fromId')} -> {packet.get('toId')}: {txt}")
        except Exception as e:
            print("[GW] Failed to log incoming text:", e)

    def send_text(self, text: str, to_id: Optional[str] = None):
        if not self.iface:
            raise RuntimeError("Not connected to Meshtastic")
        if to_id:
            self.iface.sendText(text, destinationId=to_id)
        else:
            self.iface.sendText(text)

    def run(self):
        while not self.stop_event.is_set():
            try:
                if self.iface is None:
                    self.iface = self.connect()
                    try:
                        self.iface.sendText("Pi gateway online (TCP)")
                    except Exception:
                        pass

                row = fetch_one_outbox(self.db)
                if row:
                    tries = int(row["tries"]) + 1
                    to_id = row["to_id"]
                    text = row["text"]
                    print(f"[GW] OUTBOX id={row['id']} to={to_id} tries={tries}")
                    try:
                        self.send_text(text=text, to_id=to_id)
                        mark_outbox(self.db, row["id"], "sent", tries, None)
                        log_outgoing_message(self.db, to_id, row["channel"], text, status="sent")
                    except Exception as e:
                        err = str(e)
                        new_status = "failed" if tries >= OUTBOX_MAX_TRIES else "queued"
                        mark_outbox(self.db, row["id"], new_status, tries, err)
                        log_outgoing_message(self.db, to_id, row["channel"], text, status="failed", err=err)
                        print("[GW] Send failed:", err)

                time.sleep(1)

            except Exception as e:
                print("[GW] Error, reconnecting soon:", e)
                try:
                    if self.iface:
                        self.iface.close()
                except Exception:
                    pass
                self.iface = None
                time.sleep(3)

    def stop(self):
        self.stop_event.set()
        try:
            if self.iface:
                self.iface.close()
        except Exception:
            pass


# -------------------------
# Flask web UI
# -------------------------
app = Flask(__name__)
DB_CONN = db_connect()
init_db(DB_CONN)

gateway = Gateway(HOST, DB_CONN)


BASE_HTML = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Meshtastic Gateway</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
  <style>
    body { padding-top: 20px; }
    .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace; }
    .smallish { font-size: 0.92rem; }
    pre.debugbox { background: #0b0f14; color: #cfe3ff; padding: 12px; border-radius: 10px; max-height: 70vh; overflow: auto; }
    .sidebar { max-height: 70vh; overflow: auto; }
    .nodeitem { cursor:pointer; }
    .nodeitem:hover { background: #f6f8ff; }
    .chip { display:inline-block; padding: 2px 8px; border-radius: 999px; background: #eef2ff; }
    .dim { color: #6c757d; }
  </style>
  {{ extra_head|safe }}
</head>
<body>
<div class="container">
  <div class="d-flex flex-wrap gap-2 align-items-center mb-3">
    <h3 class="m-0 me-3">Meshtastic Gateway</h3>
    <a class="btn btn-outline-primary btn-sm" href="{{ url_for('inbox') }}">Inbox</a>
    <a class="btn btn-outline-primary btn-sm" href="{{ url_for('outbox_view') }}">Outbox</a>
    <a class="btn btn-outline-primary btn-sm" href="{{ url_for('nodes') }}">Active Nodes</a>
    <a class="btn btn-outline-primary btn-sm" href="{{ url_for('broadcast') }}">Broadcast</a>
    <a class="btn btn-outline-primary btn-sm" href="{{ url_for('telemetry_view') }}">Telemetry</a>
    <a class="btn btn-outline-primary btn-sm" href="{{ url_for('map_view') }}">Map</a>
    <a class="btn btn-outline-secondary btn-sm" href="{{ url_for('debug_view') }}">Debug</a>
    <a class="btn btn-outline-secondary btn-sm" href="{{ url_for('status_view') }}">Status</a>
    <a class="btn btn-primary btn-sm" href="{{ url_for('send_form') }}">Send</a>
  </div>
  {{ body|safe }}
  <hr class="mt-4">
  <div class="text-muted small">TCP radio: <span class="mono">{{ host }}</span></div>
</div>
</body>
</html>
"""


@app.get("/")
def root():
    return redirect(url_for("inbox"))


@app.get("/inbox")
def inbox():
    limit = int(request.args.get("limit", "50"))
    rows = list_inbox(DB_CONN, limit=limit)
    body = render_template_string("""
    <div class="d-flex justify-content-between align-items-center mb-2">
      <h5 class="m-0">Inbox (latest {{limit}})</h5>
      <div class="smallish">
        <a href="{{ url_for('inbox', limit=50) }}">50</a> |
        <a href="{{ url_for('inbox', limit=200) }}">200</a> |
        <a href="{{ url_for('inbox', limit=1000) }}">1000</a>
      </div>
    </div>
    <div class="table-responsive">
    <table class="table table-sm table-striped align-middle">
      <thead><tr>
        <th>Time</th><th>From</th><th>Text</th><th>RSSI</th><th>SNR</th><th></th>
      </tr></thead>
      <tbody>
      {% for r in rows %}
        <tr>
          <td class="mono">{{ r["ts_iso"] }}</td>
          <td>
            <div>{{ node_display(r["from_long"], r["from_short"], r["from_id"]) }}</div>
            <div class="small dim mono">{{ r["from_id"] }}</div>
            <div class="small text-muted">
              <a href="{{ url_for('chat_with', node_id=r['from_id']) }}">Chat</a> ·
              <a href="{{ url_for('send_form', to_id=r['from_id']) }}">Reply</a>
            </div>
          </td>
          <td>{{ r["text"] }}</td>
          <td class="mono">{{ r["rx_rssi"] }}</td>
          <td class="mono">{{ r["rx_snr"] }}</td>
          <td class="text-end">
            <form method="post" action="{{ url_for('message_delete', msg_id=r['id']) }}" onsubmit="return confirm('Delete this message?');">
              <button class="btn btn-sm btn-outline-danger" type="submit">Delete</button>
            </form>
          </td>
        </tr>
      {% endfor %}
      </tbody>
    </table>
    </div>
    """, rows=rows, limit=limit, node_display=node_display)
    return render_template_string(BASE_HTML, body=body, host=HOST, extra_head="")


@app.post("/message/delete/<int:msg_id>")
def message_delete(msg_id: int):
    delete_message(DB_CONN, msg_id)
    return redirect(request.referrer or url_for("inbox"))


@app.get("/outbox")
def outbox_view():
    limit = int(request.args.get("limit", "50"))
    rows = list_outbox(DB_CONN, limit=limit)
    body = render_template_string("""
    <div class="d-flex justify-content-between align-items-center mb-2">
      <h5 class="m-0">Outbox (latest {{limit}})</h5>
      <div class="smallish">
        <a href="{{ url_for('outbox_view', limit=50) }}">50</a> |
        <a href="{{ url_for('outbox_view', limit=200) }}">200</a> |
        <a href="{{ url_for('outbox_view', limit=1000) }}">1000</a>
      </div>
    </div>
    <div class="table-responsive">
    <table class="table table-sm table-striped align-middle">
      <thead><tr>
        <th>Created</th><th>Status</th><th>To</th><th>Text</th><th>Tries</th><th></th>
      </tr></thead>
      <tbody>
      {% for r in rows %}
        <tr>
          <td class="mono">{{ r["created_ts_iso"] }}</td>
          <td class="mono">{{ r["status"] }}</td>
          <td>
            {% if r["to_id"] %}
              <div>{{ node_display(r["to_long"], r["to_short"], r["to_id"]) }}</div>
              <div class="small dim mono">{{ r["to_id"] }}</div>
              <div class="small text-muted"><a href="{{ url_for('chat_with', node_id=r['to_id']) }}">Chat</a></div>
            {% else %}
              <b>BROADCAST</b>
            {% endif %}
          </td>
          <td>{{ r["text"] }}</td>
          <td class="mono">{{ r["tries"] }}</td>
          <td class="text-end">
            <form method="post" action="{{ url_for('outbox_delete', out_id=r['id']) }}" onsubmit="return confirm('Delete this outbox entry?');">
              <button class="btn btn-sm btn-outline-danger" type="submit">Delete</button>
            </form>
          </td>
        </tr>
        {% if r["last_error"] %}
          <tr><td colspan="6" class="small text-danger mono">Error: {{ r["last_error"] }}</td></tr>
        {% endif %}
      {% endfor %}
      </tbody>
    </table>
    </div>
    """, rows=rows, limit=limit, node_display=node_display)
    return render_template_string(BASE_HTML, body=body, host=HOST, extra_head="")


@app.post("/outbox/delete/<int:out_id>")
def outbox_delete(out_id: int):
    delete_outbox(DB_CONN, out_id)
    return redirect(request.referrer or url_for("outbox_view"))





@app.get("/broadcast")
def broadcast():
    limit = int(request.args.get("limit", "50"))
    limit = 50 if limit not in (50, 100, 500) else limit

    DB_CONN.row_factory = sqlite3.Row
    with DB_LOCK:
        cur = DB_CONN.cursor()
        # Broadcast messages are those with to_id NULL or explicit ^all/ffffffff marker
        cur.execute("""
        SELECT m.id, m.ts, m.ts_iso, m.direction, m.from_id, m.to_id, m.text, m.rx_rssi, m.rx_snr,
               nf.long_name AS from_long, nf.short_name AS from_short
        FROM messages m
        LEFT JOIN nodes nf ON nf.node_id = m.from_id
        WHERE (m.to_id IS NULL OR lower(m.to_id) IN ('^all','!ffffffff','ffffffff','0xffffffff'))
        ORDER BY m.ts DESC
        LIMIT ?
        """, (limit,))
        msgs = cur.fetchall()

        cur.execute("""
        SELECT id, created_ts_iso, text
        FROM outbox
        WHERE to_id IS NULL AND status='queued'
        ORDER BY created_ts ASC
        LIMIT 50
        """)
        queued = cur.fetchall()

    msgs = list(reversed(msgs))  # chronological display

    body = render_template_string("""
    <div class="d-flex justify-content-between align-items-center mb-2">
      <h5 class="m-0">Broadcast <span class="chip mono">last={{limit}}</span></h5>
      <div class="smallish">
        <a href="{{ url_for('broadcast', limit=50) }}">50</a> |
        <a href="{{ url_for('broadcast', limit=100) }}">100</a> |
        <a href="{{ url_for('broadcast', limit=500) }}">500</a>
      </div>
    </div>

    {% if queued %}
      <div class="alert alert-warning py-2">
        <b>Queued broadcast messages:</b>
        <ul class="mb-0">
          {% for q in queued %}
            <li class="mono">#{{ q["id"] }} · {{ q["created_ts_iso"] }} · {{ q["text"] }}</li>
          {% endfor %}
        </ul>
      </div>
    {% endif %}

    <div class="card mb-3">
      <div class="card-body" style="max-height:60vh; overflow:auto;">
        {% for m in msgs %}
          <div class="mb-2">
            <div class="small text-muted mono">{{ m["ts_iso"] }} · {{ m["direction"] }}</div>
            <div>
              {% if m["direction"] == "in" %}
                {{ node_display(m["from_long"], m["from_short"], m["from_id"]) }}:
              {% else %}
                <b>Me</b>:
              {% endif %}
              {{ m["text"] }}
              {% if m["direction"] == "in" %}
                <span class="small text-muted mono"> (RSSI {{ m["rx_rssi"] }}, SNR {{ m["rx_snr"] }})</span>
              {% endif %}
            </div>
          </div>
        {% endfor %}
        {% if not msgs %}
          <div class="text-muted">No broadcast messages yet.</div>
        {% endif %}
      </div>
    </div>

    <form method="post" action="{{ url_for('broadcast_send') }}" class="card card-body">
      <div class="mb-2">
        <label class="form-label">Send broadcast</label>
        <textarea class="form-control" name="text" rows="2" maxlength="240" required></textarea>
      </div>
      <button class="btn btn-primary" type="submit">Queue broadcast</button>
    </form>
    """, msgs=msgs, queued=queued, limit=limit, node_display=node_display)
    return render_template_string(BASE_HTML, body=body, host=HOST, extra_head="")


@app.post("/broadcast/send")
@app.post("/broadcast/send")
def broadcast_send():
    text = request.form.get("text", "").strip()
    if not text:
        abort(400, "Missing text")
    if len(text) > 240:
        abort(400, "Text too long")
    queue_outbox(DB_CONN, None, None, text)
    return redirect(url_for("broadcast"))




def get_local_node_row(conn: sqlite3.Connection) -> Optional[sqlite3.Row]:
    """Return a best-effort 'local' node row from the nodes table (schema-safe)."""
    conn.row_factory = sqlite3.Row
    with DB_LOCK:
        cur = conn.cursor()
        # Prefer a favorite node if present, else most recently seen
        try:
            cur.execute("""
            SELECT node_id, long_name, short_name, hw_model, macaddr,
                   last_seen_ts, last_seen_iso, last_rssi, last_snr,
                   last_lat, last_lon, last_alt, last_pos_ts, last_pos_iso
            FROM nodes
            WHERE is_favorite=1
            ORDER BY last_seen_ts DESC
            LIMIT 1
            """)
            row = cur.fetchone()
            if row:
                return row
        except Exception:
            pass

        cur.execute("""
        SELECT node_id, long_name, short_name, hw_model, macaddr,
               last_seen_ts, last_seen_iso, last_rssi, last_snr,
               last_lat, last_lon, last_alt, last_pos_ts, last_pos_iso
        FROM nodes
        ORDER BY last_seen_ts DESC
        LIMIT 1
        """)
        return cur.fetchone()


    conn.row_factory = sqlite3.Row
    with DB_LOCK:
        cur = conn.cursor()
        cur.execute("""
        SELECT ts_iso, raw_json
        FROM telemetry
        WHERE node_id=?
        ORDER BY ts DESC, id DESC
        LIMIT 30
        """, (node_id,))
        rows = cur.fetchall()
    for r in rows:
        try:
            raw = json.loads(r["raw_json"])
            dec = raw.get("decoded") or {}
            tel = dec.get("telemetry") or {}
            ls = tel.get("localStats") or tel.get("local_stats")
            if isinstance(ls, dict) and ls:
                return (ls, r["ts_iso"])
        except Exception:
            continue
    return (None, None)


def _safe_obj(obj: Any) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False, indent=2, default=str)
    except Exception:
        return str(obj)




@app.get("/status")
def status_view():
    iface = getattr(gateway, "iface", None)
    connected = iface is not None

    local = get_local_node_row(DB_CONN)
    local_id = local["node_id"] if local else ""

    live_node = {}
    if connected and local_id:
        try:
            nodes = getattr(iface, "nodes", None)
            if isinstance(nodes, dict) and local_id in nodes and isinstance(nodes[local_id], dict):
                live_node = nodes[local_id]
        except Exception:
            live_node = {}

    info = {}
    if connected:
        for k in ["myInfo", "metadata"]:
            try:
                v = getattr(iface, k, None)
                if v is not None:
                    info[k] = v
            except Exception as e:
                info[k] = f"<error: {e}>"
        try:
            n = getattr(iface, "nodes", None)
            if isinstance(n, dict):
                info["nodes_count"] = len(n)
        except Exception as e:
            info["nodes_count"] = f"<error: {e}>"

    body = render_template_string("""
    <h5>Status</h5>

    <div class="card mb-3"><div class="card-body">
      <div class="mono small">
        <div><b>Host:</b> {{ host }}</div>
        <div><b>Connection:</b> {{ "connected" if connected else "disconnected" }}</div>
      </div>
    </div></div>

    <div class="row g-2 mb-2">
      <div class="col-md-6">
        <div class="card"><div class="card-body">
          <div class="small text-muted mb-1">Local node (from DB)</div>
          {% if local %}
            <div class="mono small">
              <div><b>Node:</b> {{ node_display(local["long_name"], local["short_name"], local["node_id"]) }} — {{ local["node_id"] }}</div>
              <div><b>HW:</b> {{ local["hw_model"] }}  <b>MAC:</b> {{ local["macaddr"] }}</div>
              <div><b>Last seen:</b> {{ local["last_seen_iso"] }}</div>
              <div><b>RSSI/SNR:</b> {{ local["last_rssi"] }} / {{ local["last_snr"] }}</div>
              <div><b>Position:</b> {{ local["last_lat"] }}, {{ local["last_lon"] }}{% if local["last_alt"] is not none %} (alt {{ local["last_alt"] }}){% endif %}</div>
              <div><b>Pos time:</b> {{ local["last_pos_iso"] }}</div>
            </div>
          {% else %}
            <div class="text-muted">No node data yet.</div>
          {% endif %}
        </div></div>
      </div>

      <div class="col-md-12">
        <div class="card"><div class="card-body">
          
      </div>
    </div>

    <div class="row g-2">
      <div class="col-md-12">
        <div class="card"><div class="card-body">
          <div class="small text-muted mb-1">Live node data (best-effort, from interface)</div>
          {% if connected and live_node_json %}
            <pre class="mono small" style="max-height:35vh; overflow:auto;">{{ live_node_json }}</pre>
          {% else %}
            <div class="text-muted">Not available.</div>
          {% endif %}
        </div></div>
      </div>

      <div class="col-md-12">
        <div class="card"><div class="card-body">
          <div class="small text-muted mb-1">Interface summary</div>
          {% if connected %}
            <pre class="mono small" style="max-height:35vh; overflow:auto;">{{ iface_json }}</pre>
          {% else %}
            <div class="text-muted">Not connected.</div>
          {% endif %}
        </div></div>
      </div>
    </div>
    """, host=HOST, connected=connected, local=local,
         live_node_json=_safe_obj(live_node), iface_json=_safe_obj(info),
         node_display=node_display)

    return render_template_string(BASE_HTML, body=body, host=HOST, extra_head="")

@app.get("/nodes")
def nodes():
    window = request.args.get("window", DEFAULT_ACTIVE_WINDOW)
    win_sec = parse_window_to_seconds(window)
    rows = list_active_nodes(DB_CONN, win_sec)

    body = render_template_string("""
    <div class="d-flex justify-content-between align-items-center mb-2">
      <h5 class="m-0">Active nodes <span class="chip mono">window={{window}}</span></h5>
      <div class="d-flex gap-2 align-items-center">
        <div class="smallish">
          <a href="{{ url_for('nodes', window='2h') }}">2h</a> |
          <a href="{{ url_for('nodes', window='6h') }}">6h</a> |
          <a href="{{ url_for('nodes', window='1d') }}">1d</a> |
          <a href="{{ url_for('nodes', window='1w') }}">1w</a> |
          <a href="{{ url_for('nodes', window='1m') }}">1m</a>
        </div>
        <a class="btn btn-sm btn-primary" href="{{ url_for('send_form') }}">Send</a>
      </div>
    </div>

    <div class="table-responsive">
    <table class="table table-sm table-striped align-middle">
      <thead><tr>
        <th>Last seen</th><th>Node</th><th>Name</th><th>HW</th><th>RSSI</th><th>SNR</th><th>Pos</th><th></th>
      </tr></thead>
      <tbody>
      {% for r in rows %}
        <tr>
          <td class="mono">{{ r["last_seen_iso"] }}</td>
          <td class="mono">{{ r["node_id"] }}</td>
          <td>{{ node_display(r["long_name"], r["short_name"], r["node_id"]) }}</td>
          <td class="mono">{{ r["hw_model"] }}</td>
          <td class="mono">{{ r["last_rssi"] }}</td>
          <td class="mono">{{ r["last_snr"] }}</td>
          <td class="mono">
            {% if r["last_lat"] and r["last_lon"] %}
              {{ "%.5f"|format(r["last_lat"]) }}, {{ "%.5f"|format(r["last_lon"]) }}
            {% else %}
              —
            {% endif %}
          </td>
          <td class="text-end small">
            <a href="{{ url_for('chat_with', node_id=r['node_id']) }}">Chat</a> ·
            <a href="{{ url_for('send_form', to_id=r['node_id']) }}">Send</a>
          </td>
        </tr>
      {% endfor %}
      </tbody>
    </table>
    </div>
    """, rows=rows, window=window, node_display=node_display)
    return render_template_string(BASE_HTML, body=body, host=HOST, extra_head="")


@app.get("/send")
def send_form():
    to_id_prefill = (request.args.get("to_id") or "").strip()
    window = request.args.get("window", DEFAULT_ACTIVE_WINDOW)
    win_sec = parse_window_to_seconds(window)
    nodes_for_dropdown = list_active_nodes(DB_CONN, win_sec)

    body = render_template_string("""
    <h5 class="mb-3">Send message</h5>
    <form method="post" action="{{ url_for('send_post') }}" class="card card-body">
      <div class="mb-3">
        <label class="form-label">Destination</label>
        <select class="form-select" name="to_id">
          <option value="" {{ "selected" if not to_id_prefill else "" }}>BROADCAST (primary channel)</option>
          {% for r in rows %}
            <option value="{{ r['node_id'] }}" {{ "selected" if r['node_id']==to_id_prefill else "" }}>
              {{ node_display(r["long_name"], r["short_name"], r["node_id"]) }} — {{ r['node_id'] }} [RSSI {{ r['last_rssi'] }}, SNR {{ r['last_snr'] }}]
            </option>
          {% endfor %}
        </select>
        <div class="form-text">Dropdown uses nodes heard in window={{window}}.</div>
      </div>

      <div class="mb-3">
        <label class="form-label">Message</label>
        <textarea class="form-control" name="text" rows="3" maxlength="240" required></textarea>
      </div>

      <button class="btn btn-primary" type="submit">Queue message</button>
      <a class="btn btn-outline-secondary" href="{{ url_for('outbox_view') }}">Outbox</a>
    </form>
    """, rows=nodes_for_dropdown, to_id_prefill=to_id_prefill, window=window, node_display=node_display)
    return render_template_string(BASE_HTML, body=body, host=HOST, extra_head="")


@app.post("/send")
def send_post():
    to_id = request.form.get("to_id", "").strip()
    text = request.form.get("text", "").strip()
    if not text:
        abort(400, "Missing text")
    if len(text) > 240:
        abort(400, "Text too long")

    queue_outbox(DB_CONN, to_id if to_id else None, None, text)
    return redirect(url_for("outbox_view"))


@app.get("/chat/<path:node_id>")
def chat_with(node_id: str):
    node_id = (node_id or "").strip()
    if not node_id.startswith("!"):
        abort(400, "Invalid node id")

    limit = int(request.args.get("limit", str(DEFAULT_CHAT_LIMIT)))
    msgs = chat_messages(DB_CONN, node_id=node_id, limit=limit)
    msgs = list(reversed(msgs))  # chronological

    title_name = node_label_for_id(DB_CONN, node_id)

    body = render_template_string("""
    <div class="d-flex justify-content-between align-items-center mb-2">
      <h5 class="m-0">Chat with <b>{{ title_name }}</b> <span class="mono dim">{{node_id}}</span></h5>
      <div class="smallish">
        <a href="{{ url_for('chat_with', node_id=node_id, limit=20) }}">20</a> |
        <a href="{{ url_for('chat_with', node_id=node_id, limit=100) }}">100</a> |
        <a href="{{ url_for('chat_with', node_id=node_id, limit=500) }}">500</a>
      </div>
    </div>

    <div class="card mb-3">
      <div class="card-body" style="max-height:60vh; overflow:auto;">
        {% for m in msgs %}
          <div class="mb-2">
            <div class="small text-muted mono">{{ m["ts_iso"] }} · {{ m["direction"] }}</div>
            <div>
              {% if m["direction"] == "in" %}
                <b>{{ node_display(m["from_long"], m["from_short"], m["from_id"]) }}</b>:
              {% else %}
                <b>Me</b> → <b>{{ node_display(m["to_long"], m["to_short"], m["to_id"]) }}</b>:
              {% endif %}
              {{ m["text"] }}
            </div>
          </div>
        {% endfor %}
        {% if not msgs %}
          <div class="text-muted">No messages yet.</div>
        {% endif %}
      </div>
    </div>

    <form method="post" action="{{ url_for('send_post') }}" class="card card-body">
      <input type="hidden" name="to_id" value="{{ node_id }}">
      <div class="mb-2">
        <label class="form-label">Send to {{ title_name }}</label>
        <textarea class="form-control" name="text" rows="2" maxlength="240" required></textarea>
      </div>
      <button class="btn btn-primary" type="submit">Queue</button>
    </form>
    """, node_id=node_id, msgs=msgs, node_display=node_display, title_name=title_name)
    return render_template_string(BASE_HTML, body=body, host=HOST, extra_head="")


@app.get("/debug")
def debug_view():
    extra_head = ""
    debug_nodes = list_active_nodes(DB_CONN, parse_window_to_seconds(DEFAULT_ACTIVE_WINDOW))
    body = render_template_string("""
    <div class="d-flex justify-content-between align-items-center mb-2">
      <h5 class="m-0">Debug terminal</h5>
      <div class="d-flex gap-2 align-items-center">
        <button class="btn btn-sm btn-outline-secondary" id="pauseBtn">Pause</button>
        <button class="btn btn-sm btn-outline-secondary" id="copyBtn">Copy</button>
        <form method="post" action="{{ url_for('debug_clear_post') }}" class="m-0">
          <button class="btn btn-sm btn-outline-danger" type="submit" onclick="return confirm('Clear debug buffer?');">Clear</button>
        </form>
      </div>
    </div>

    <div class="row g-2 mb-2">
      <div class="col-md-3">
        <label class="form-label small text-muted">Type</label>
        <select class="form-select form-select-sm" id="typeSel">
          <option value="all">All</option>
          <option value="text">Text</option>
          <option value="position">Position</option>
          <option value="other">Other</option>
        </select>
      </div>
      <div class="col-md-4">
        <label class="form-label small text-muted">Node</label>
        <select class="form-select form-select-sm mono" id="nodeSel">
          <option value="">All nodes</option>
          {% for n in debug_nodes %}
            <option value="{{ n['node_id'] }}">{{ node_display(n["long_name"], n["short_name"], n["node_id"]) }} — {{ n['node_id'] }}</option>
          {% endfor %}
        </select>
      </div>
      <div class="col-md-2">
        <label class="form-label small text-muted">Last N</label>
        <input class="form-control form-control-sm" id="nSel" type="number" min="1" max="{{maxn}}" value="{{default_n}}">
      </div>
      <div class="col-md-2">
        <label class="form-label small text-muted">Refresh</label>
        <select class="form-select form-select-sm" id="rateSel">
          <option value="1">1s</option>
          <option value="2" selected>2s</option>
          <option value="5">5s</option>
          <option value="10">10s</option>
        </select>
      </div>
    </div>

    <pre class="debugbox" id="debugBox"></pre>

    <script>
      let paused = false;
      let timer = null;

      function params() {
        const n = document.getElementById("nSel").value || "40";
        const typ = document.getElementById("typeSel").value || "all";
        const node = (document.getElementById("nodeSel").value || "").trim();
        return new URLSearchParams({ n, typ, node });
      }

      async function refreshDebug() {
        if (paused) return;
        try {
          const res = await fetch("/debug/data?" + params().toString(), {cache: "no-store"});
          const data = await res.json();
          document.getElementById("debugBox").textContent = data.text;
        } catch (e) {
          document.getElementById("debugBox").textContent = "Fetch error: " + e;
        }
      }

      function setTimer() {
        if (timer) clearInterval(timer);
        const rate = parseInt(document.getElementById("rateSel").value || "2", 10) * 1000;
        timer = setInterval(refreshDebug, rate);
      }

      document.getElementById("pauseBtn").addEventListener("click", () => {
        paused = !paused;
        document.getElementById("pauseBtn").textContent = paused ? "Resume" : "Pause";
      });

      document.getElementById("copyBtn").addEventListener("click", async () => {
        const txt = document.getElementById("debugBox").textContent || "";
        try {
          if (navigator.clipboard && navigator.clipboard.writeText) {
            await navigator.clipboard.writeText(txt);
          } else {
            const ta = document.createElement("textarea");
            ta.value = txt;
            ta.setAttribute("readonly", "");
            ta.style.position = "fixed";
            ta.style.top = "-1000px";
            document.body.appendChild(ta);
            ta.select();
            const ok = document.execCommand("copy");
            document.body.removeChild(ta);
            if (!ok) throw new Error("execCommand copy failed");
          }
          document.getElementById("copyBtn").textContent = "Copied";
          setTimeout(() => document.getElementById("copyBtn").textContent = "Copy", 800);
        } catch (e) {
          alert("Clipboard failed: " + e);
        }
      });for (const id of ["typeSel","nodeSel","nSel"]) {
        document.getElementById(id).addEventListener("change", refreshDebug);
      }
      document.getElementById("rateSel").addEventListener("change", setTimer);

      setTimer();
      refreshDebug();
    </script>
    """, maxn=DEBUG_BUFFER_MAX, default_n=DEBUG_SHOW_DEFAULT, debug_nodes=debug_nodes, node_display=node_display)
    return render_template_string(BASE_HTML, body=body, host=HOST, extra_head=extra_head)


@app.get("/debug/data")
def debug_data():
    n = int(request.args.get("n", str(DEBUG_SHOW_DEFAULT)))
    typ = request.args.get("typ", "all")
    node = request.args.get("node", "")
    entries = debug_filtered(n=n, typ=typ, node_id=node)

    lines: List[str] = []
    for e in entries:
        lines.append(f"=== {iso_utc(e.ts)} | type={e.typ} | from={e.from_id or '-'} ===")
        lines.append(e.text)
        lines.append("")
    return jsonify({"text": "\n".join(lines)})


@app.post("/debug/clear")
def debug_clear_post():
    debug_clear()
    return redirect(url_for("debug_view"))




@app.get("/telemetry")
def telemetry_view():
    window = request.args.get("window", DEFAULT_ACTIVE_WINDOW)
    win_sec = parse_window_to_seconds(window)
    nodes = telemetry_nodes(DB_CONN, win_sec)
    node_id = request.args.get("node_id", "")
    if not node_id and nodes:
        node_id = nodes[0]["node_id"]
    latest = telemetry_latest(DB_CONN, node_id) if node_id else None
    extra_head = '<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>'

    body = render_template_string("""
    <div class="d-flex justify-content-between align-items-center mb-2">
      <h5 class="m-0">Telemetry <span class="chip mono">window={{window}}</span></h5>
      <div class="smallish">
        <a href="{{ url_for('telemetry_view', window='2h', node_id=node_id) }}">2h</a> |
        <a href="{{ url_for('telemetry_view', window='6h', node_id=node_id) }}">6h</a> |
        <a href="{{ url_for('telemetry_view', window='1d', node_id=node_id) }}">1d</a> |
        <a href="{{ url_for('telemetry_view', window='1w', node_id=node_id) }}">1w</a> |
        <a href="{{ url_for('telemetry_view', window='1m', node_id=node_id) }}">1m</a>
      </div>
    </div>

    <div class="row g-2 mb-2">
      <div class="col-md-6">
        <label class="form-label small text-muted">Node</label>
        <select class="form-select form-select-sm mono" id="nodeSel">
          {% for n in nodes %}
            <option value="{{ n['node_id'] }}" {{ "selected" if n['node_id']==node_id else "" }}>
              {{ node_display(n["long_name"], n["short_name"], n["node_id"]) }} — {{ n['node_id'] }}
            </option>
          {% endfor %}
        </select>
      </div>
      <div class="col-md-6">
        <div class="card"><div class="card-body py-2">
          <div class="small text-muted">Latest</div>
          {% if latest %}
            <div class="mono small">
              {{ latest["ts_iso"] }} ·
              T={{ latest["temperature_c"] if latest["temperature_c"] is not none else "—" }} °C ·
              Lux={{ latest["lux"] if latest["lux"] is not none else "—" }} ·
              V={{ latest["voltage"] if latest["voltage"] is not none else "—" }} ·
              Batt={{ latest["battery_level"] if latest["battery_level"] is not none else "—" }}
            </div>
          {% else %}
            <div class="text-muted small">No telemetry for this selection.</div>
          {% endif %}
        </div></div>
      </div>
    </div>

    <div class="row g-2">
      <div class="col-md-12"><div class="card"><div class="card-body">
        <div class="small text-muted mb-1">Temperature (°C)</div>
        <canvas id="tempChart" height="90"></canvas>
      </div></div></div>
      <div class="col-md-12"><div class="card"><div class="card-body">
        <div class="small text-muted mb-1">Lux</div>
        <canvas id="luxChart" height="90"></canvas>
      </div></div></div>
      <div class="col-md-12"><div class="card"><div class="card-body">
        <div class="small text-muted mb-1">Voltage (V)</div>
        <canvas id="voltChart" height="90"></canvas>
      </div></div></div>
    </div>

    <script>
      const windowVal = "{{ window }}";
      function makeLine(ctx, label) {
        return new Chart(ctx, {
          type: "line",
          data: { labels: [], datasets: [{ label, data: [], tension: 0.2, pointRadius: 0 }] },
          options: { responsive: true, animation: false, plugins: { legend: { display: false } },
                     scales: { x: { ticks: { maxRotation: 0 } } } }
        });
      }
      async function refresh() {
        const nodeId = document.getElementById("nodeSel").value || "";
        const res = await fetch("/telemetry/data?" + new URLSearchParams({ node_id: nodeId, window: windowVal }).toString(), {cache:"no-store"});
        const data = await res.json();
        const labels = data.labels || [];
        tempChart.data.labels = labels; tempChart.data.datasets[0].data = data.temperature_c || []; tempChart.update();
        luxChart.data.labels = labels; luxChart.data.datasets[0].data = data.lux || []; luxChart.update();
        voltChart.data.labels = labels; voltChart.data.datasets[0].data = data.voltage || []; voltChart.update();
      }
      document.getElementById("nodeSel").addEventListener("change", () => {
        const nodeId = document.getElementById("nodeSel").value || "";
        const u = new URL(window.location.href);
        u.searchParams.set("node_id", nodeId);
        window.location.href = u.toString();
      });
      const tempChart = makeLine(document.getElementById("tempChart"), "Temperature");
      const luxChart = makeLine(document.getElementById("luxChart"), "Lux");
      const voltChart = makeLine(document.getElementById("voltChart"), "Voltage");
      refresh();
    </script>
    """, window=window, nodes=nodes, node_id=node_id, latest=latest, node_display=node_display)
    return render_template_string(BASE_HTML, body=body, host=HOST, extra_head=extra_head)


@app.get("/telemetry/data")
def telemetry_data():
    window = request.args.get("window", DEFAULT_ACTIVE_WINDOW)
    win_sec = parse_window_to_seconds(window)
    node_id = (request.args.get("node_id") or "").strip()
    if not node_id:
        return jsonify({"labels": [], "temperature_c": [], "lux": [], "voltage": [], "battery_level": []})

    rows = telemetry_series(DB_CONN, node_id=node_id, window_seconds=win_sec)
    labels = [r["ts_iso"].replace(" UTC", "") for r in rows]
    def col(name): return [r[name] for r in rows]
    return jsonify({"labels": labels, "temperature_c": col("temperature_c"), "lux": col("lux"),
                    "voltage": col("voltage"), "battery_level": col("battery_level")})

@app.get("/map")
def map_view():
    window = request.args.get("window", DEFAULT_ACTIVE_WINDOW)
    win_sec = parse_window_to_seconds(window)
    track_n = int(request.args.get("n", str(DEFAULT_MAP_TRACK_POINTS)))
    track_n = max(5, min(track_n, 2000))

    rows = list_nodes_with_position(DB_CONN, win_sec)
    if rows:
        center_lat = float(rows[0]["last_lat"])
        center_lon = float(rows[0]["last_lon"])
    else:
        center_lat, center_lon = 60.1699, 24.9384

    extra_head = """
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/leaflet@1.9.4/dist/leaflet.css">
<script src="https://cdn.jsdelivr.net/npm/leaflet@1.9.4/dist/leaflet.js"></script>
"""

    body = render_template_string("""
    <div class="d-flex justify-content-between align-items-center mb-2">
      <h5 class="m-0">Map <span class="chip mono">window={{window}}</span></h5>
      <div class="d-flex gap-2 align-items-center smallish">
        <div>
          <a href="{{ url_for('map_view', window='2h') }}">2h</a> |
          <a href="{{ url_for('map_view', window='6h') }}">6h</a> |
          <a href="{{ url_for('map_view', window='1d') }}">1d</a> |
          <a href="{{ url_for('map_view', window='1w') }}">1w</a> |
          <a href="{{ url_for('map_view', window='1m') }}">1m</a>
        </div>
        <div class="text-muted">Markers: <span id="markerCount">{{ rows|length }}</span></div>
      </div>
    </div>

    <div class="row g-2">
      <div class="col-md-3">
        <div class="card">
          <div class="card-body sidebar p-2">
            <div class="d-flex justify-content-between align-items-center mb-2">
              <div class="small text-muted">Nodes (click to zoom)</div>
              <button class="btn btn-sm btn-outline-secondary" id="clearTracksBtn">Clear tracks</button>
            </div>
            <div id="nodeList"></div>
          </div>
        </div>
        <div class="small text-muted mt-2">
          Refresh:
          <select id="mapRate" class="form-select form-select-sm d-inline-block" style="width: auto;">
            <option value="2">2s</option>
            <option value="5" selected>5s</option>
            <option value="10">10s</option>
          </select>
          · Track points:
          <input id="trackN" type="number" min="5" max="2000" value="{{ track_n }}"
                 class="form-control form-control-sm d-inline-block" style="width: 90px;">
        </div>
      </div>

      <div class="col-md-9">
        <div id="map" style="height: 70vh; border-radius: 12px; border: 1px solid #ddd;"></div>
      </div>
    </div>

    <script>
      const map = L.map('map').setView([{{ center_lat }}, {{ center_lon }}], {{ 12 if rows|length else 10 }});
      L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
        maxZoom: 19,
        attribution: '&copy; OpenStreetMap contributors'
      }).addTo(map);

      const markerLayer = L.layerGroup().addTo(map);
      const trackLayer = L.layerGroup().addTo(map);

      const markers = new Map(); // node_id -> marker
      let selectedTracks = new Set(); // node_id's with tracks enabled

      function popupHtml(n) {
        const lat = n.lat, lon = n.lon;
        return `
          <div style="min-width: 220px">
            <div><b>${n.name}</b> <span class="mono" style="color:#666">${n.node_id}</span></div>
            <div class="mono" style="font-size:12px; color:#666">${n.hw || ""}</div>
            <hr style="margin:6px 0">
            <div><b>Last seen:</b> ${n.last_seen || ""}</div>
            <div><b>Pos time:</b> ${n.pos_time || ""}</div>
            <div><b>RSSI/SNR:</b> ${n.rssi ?? ""} / ${n.snr ?? ""}</div>
            <div><b>Lat/Lon:</b> ${lat.toFixed(5)}, ${lon.toFixed(5)}</div>
          </div>
        `;
      }

      function setNodeList(nodes) {
        const el = document.getElementById("nodeList");
        el.innerHTML = "";
        for (const n of nodes) {
          const item = document.createElement("div");
          item.className = "p-2 rounded nodeitem";

          const checked = selectedTracks.has(n.node_id) ? "checked" : "";
          item.innerHTML = `
            <div class="d-flex justify-content-between align-items-start gap-2">
              <div>
                <div><b>${n.name}</b></div>
                <div class="small text-muted mono">${n.node_id}</div>
                <div class="small text-muted mono">RSSI ${n.rssi ?? ""} · SNR ${n.snr ?? ""}</div>
              </div>
              <div class="text-end">
                <label class="small text-muted">
                  <input type="checkbox" class="trackBox" data-node="${n.node_id}" ${checked}>
                  track
                </label>
              </div>
            </div>
          `;

          item.addEventListener("click", (ev) => {
            if (ev.target && ev.target.classList && ev.target.classList.contains("trackBox")) return;
            map.setView([n.lat, n.lon], 15);
            const m = markers.get(n.node_id);
            if (m) m.openPopup();
          });

          el.appendChild(item);
        }

        for (const cb of document.querySelectorAll(".trackBox")) {
          cb.addEventListener("change", () => {
            const nodeId = cb.getAttribute("data-node");
            if (!nodeId) return;
            if (cb.checked) selectedTracks.add(nodeId);
            else selectedTracks.delete(nodeId);
            refreshTracks();
          });
        }
      }

      async function refreshMarkersAndList() {
        const res = await fetch("/map/data?window={{ window }}", {cache:"no-store"});
        const data = await res.json();
        document.getElementById("markerCount").textContent = data.nodes.length.toString();

        setNodeList(data.nodes);

        markerLayer.clearLayers();
        for (const n of data.nodes) {
          let m = markers.get(n.node_id);
          if (!m) {
            m = L.marker([n.lat, n.lon]);
            markers.set(n.node_id, m);
          } else {
            m.setLatLng([n.lat, n.lon]);
          }
          m.bindPopup(popupHtml(n));
          m.addTo(markerLayer);
        }

        await refreshTracks();
      }

      async function refreshTracks() {
        trackLayer.clearLayers();
        if (selectedTracks.size === 0) return;

        const trackN = document.getElementById("trackN").value || "{{ track_n }}";
        const ids = Array.from(selectedTracks).join(",");
        const qs = new URLSearchParams({ ids, n: trackN });
        const res = await fetch("/map/tracks?" + qs.toString(), {cache:"no-store"});
        const data = await res.json();

        for (const [nodeId, pts] of Object.entries(data.tracks || {})) {
          if (!pts || pts.length < 2) continue;
          L.polyline(pts, {weight: 3}).addTo(trackLayer);
        }
      }

      let timer = null;
      function setTimer() {
        if (timer) clearInterval(timer);
        const rateMs = parseInt(document.getElementById("mapRate").value || "5", 10) * 1000;
        timer = setInterval(refreshMarkersAndList, rateMs);
      }

      document.getElementById("mapRate").addEventListener("change", setTimer);
      document.getElementById("trackN").addEventListener("change", refreshTracks);
      document.getElementById("clearTracksBtn").addEventListener("click", () => {
        selectedTracks = new Set();
        refreshTracks();
        refreshMarkersAndList();
      });

      setTimer();
      refreshMarkersAndList();
    </script>
    """, rows=rows, center_lat=center_lat, center_lon=center_lon, window=window, track_n=track_n)

    return render_template_string(BASE_HTML, body=body, host=HOST, extra_head=extra_head)


@app.get("/map/data")
def map_data():
    window = request.args.get("window", DEFAULT_ACTIVE_WINDOW)
    win_sec = parse_window_to_seconds(window)

    rows = list_nodes_with_position(DB_CONN, win_sec)
    nodes: List[Dict[str, Any]] = []
    for r in rows:
        name = node_display(r["long_name"], r["short_name"], r["node_id"])
        nodes.append({
            "node_id": r["node_id"],
            "name": name,
            "hw": r["hw_model"],
            "last_seen": r["last_seen_iso"],
            "pos_time": r["last_pos_iso"],
            "rssi": r["last_rssi"],
            "snr": r["last_snr"],
            "lat": float(r["last_lat"]),
            "lon": float(r["last_lon"]),
        })
    return jsonify({"nodes": nodes})


@app.get("/map/tracks")
def map_tracks():
    ids = (request.args.get("ids", "") or "").strip()
    n = int(request.args.get("n", str(DEFAULT_MAP_TRACK_POINTS)))
    n = max(5, min(n, 2000))
    node_ids = [x.strip() for x in ids.split(",") if x.strip().startswith("!")]
    tracks: Dict[str, List[List[float]]] = {}

    node_ids = node_ids[:25]

    for node_id in node_ids:
        pts = get_track_points(DB_CONN, node_id, n)
        if len(pts) >= 2:
            tracks[node_id] = [[lat, lon] for (lat, lon) in pts]

    return jsonify({"tracks": tracks})


def start_gateway_thread() -> threading.Thread:
    t = threading.Thread(target=gateway.run, name="meshtastic-gateway", daemon=True)
    t.start()
    return t


if __name__ == "__main__":
    start_gateway_thread()
    app.run(host=WEB_HOST, port=WEB_PORT, debug=False, threaded=True)
