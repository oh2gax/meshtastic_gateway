#!/usr/bin/env python3
"""
gateway_web_new_stable_v11.py

Changes vs previous monolithic version:
- UI moved to templates/ + static/ (CSS extracted) to prevent accidental UI drift.
- Telemetry page + telemetry logging removed (lighter).
- Node-name / HW refresh fix: periodically re-scan iface.nodes and upsert name/hw fields when they arrive later or change.
- Removed startup "Pi gateway online (TCP)" transmission.
- Broadcast page auto-scrolls to newest messages (UI change requested).

Run:
  python3 gateway_web_new_stable_v11.py

Folder layout (keep together):
  gateway_web_new_stable_v11.py
  meshtastic_messages.db   (optional; default path set in DB_PATH below)
  templates/
  static/
"""

import json
import sqlite3
import time
import threading
import urllib.request
import urllib.error
import re
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional, List, Tuple

from flask import Flask, request, redirect, url_for, render_template, jsonify
from meshtastic.tcp_interface import TCPInterface
from pubsub import pub

# -------------------------
# Configuration
# -------------------------
HOST = "192.168.1.211"

# Default DB path. If you keep meshtastic_messages.db in the same folder, you can set:
# DB_PATH = "./meshtastic_messages.db"
DB_PATH = "/home/oh2gax/meshtastic/meshtastic_messages.db"

WEB_HOST = "0.0.0.0"
WEB_PORT = 8000

DEFAULT_ACTIVE_WINDOW = "2h"   # 2h, 6h, 1d, 1w, 1m supported
DEFAULT_CHAT_LIMIT = 20
DEFAULT_MAP_TRACK_POINTS = 60

# How many position rows to keep per node (separate from the UI 'minutes' filter)
POSITION_KEEP_PER_NODE = 5000

# How often we re-scan iface.nodes to pick up late-arriving / changed names & HW fields
NODE_REFRESH_INTERVAL_SEC = 30

OUTBOX_MAX_TRIES = 3

DEBUG_BUFFER_MAX = 300
DEBUG_SHOW_DEFAULT = 40

BROADCAST_IDS = {"^all", "!ffffffff", "ffffffff", "0xffffffff"}

# -------------------------
# Utilities
# -------------------------
def utc_ts() -> int:
    return int(time.time())

def iso_utc(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def iso_now() -> str:
    return iso_utc(utc_ts())


def derive_node_id(from_id: Optional[str], from_num: Any) -> Optional[str]:
    """Return Meshtastic-style node id like '!db29583c' from fromId or numeric from."""
    if from_id:
        try:
            s = str(from_id).strip()
            return s if s else None
        except Exception:
            return None
    try:
        if isinstance(from_num, int):
            return f"!{from_num & 0xFFFFFFFF:08x}"
        # sometimes numeric is str
        if isinstance(from_num, str) and from_num.isdigit():
            n = int(from_num)
            return f"!{n & 0xFFFFFFFF:08x}"
    except Exception:
        pass
    return None


def parse_window_to_seconds(window: str) -> int:
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


def int_to_node_id(v) -> Optional[str]:
    """Convert a numeric node id to Meshtastic !xxxxxxxx form."""
    try:
        if v is None:
            return None
        i = int(v)
        if i < 0:
            return None
        return f"!{i:08x}"
    except Exception:
        return None


def node_display(long_name: Optional[str], short_name: Optional[str], node_id: Optional[str]) -> str:
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

def is_broadcast_id(to_id: Optional[str]) -> bool:
    if to_id is None:
        return True
    tid = str(to_id).strip().lower()
    return tid == "" or tid in BROADCAST_IDS

def parse_position_from_node(node: Dict[str, Any]) -> Optional[Dict[str, Any]]:
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
    if not isinstance(packet, dict):
        return None
    pos = packet.get("position")
    if isinstance(pos, dict) and "latitude" in pos and "longitude" in pos:
        try:
            return {"lat": float(pos["latitude"]), "lon": float(pos["longitude"]),
                    "alt": pos.get("altitude"), "time": pos.get("time", packet.get("rxTime"))}
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
                    return {"lat": float(lat), "lon": float(lon),
                            "alt": candidate.get("altitude"), "time": candidate.get("time", packet.get("rxTime"))}
                except Exception:
                    pass
    return None

def packet_type(packet: Dict[str, Any]) -> str:
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
    try:
        if isinstance(packet, dict):
            return json.dumps(packet, ensure_ascii=False, indent=2, default=str)
        return str(packet)
    except Exception:
        return str(packet)


# Prevent duplicate logging if both on_any_packet and on_text fire for same packet id
RECENT_TEXT_IDS_LOCK = threading.Lock()
RECENT_TEXT_IDS: Dict[int, int] = {}   # packet_id -> last_seen_ts
RECENT_TEXT_TTL_SEC = 300              # keep 5 minutes

def _seen_text_packet_id(packet_id: Optional[int]) -> bool:
    """Return True if we've already processed this packet_id recently."""
    if packet_id is None:
        return False
    now = utc_ts()
    with RECENT_TEXT_IDS_LOCK:
        # prune old
        old = [pid for pid, ts in RECENT_TEXT_IDS.items() if now - ts > RECENT_TEXT_TTL_SEC]
        for pid in old:
            RECENT_TEXT_IDS.pop(pid, None)

        if packet_id in RECENT_TEXT_IDS:
            return True
        RECENT_TEXT_IDS[packet_id] = now
        return False

# -------------------------

# --------------------------------------------------------------------------------------
# METAR (Services)
# --------------------------------------------------------------------------------------

METAR_BASE_URL = "https://tgftp.nws.noaa.gov/data/observations/metar/stations/{icao}.TXT"
METAR_DELAY_KEY = "metar_delay_sec"
METAR_ENABLED_KEY = "metar_enabled"

def fetch_metar_text(icao: str, timeout_sec: int = 8) -> Optional[str]:
    """Fetch latest METAR for ICAO (e.g. EFHK). Returns the METAR line or None."""
    icao = (icao or "").strip().upper()
    if not re.fullmatch(r"[A-Z]{4}", icao):
        return None
    url = METAR_BASE_URL.format(icao=icao)
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "MeshtasticGateway/1.0"})
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            data = resp.read().decode("utf-8", errors="replace")
    except Exception:
        return None

    lines = [ln.strip() for ln in data.splitlines() if ln.strip()]
    if not lines:
        return None
    metar = lines[1] if len(lines) >= 2 else lines[0]
    return metar.strip()


def compact_metar(metar: str, icao: str) -> str:
    """Remove some unnecessary parts and keep message short."""
    s = (metar or "").strip()
    if not s:
        return ""
    s = re.sub(r"=\s*$", "", s).strip()
    icao = (icao or "").strip().upper()
    if icao and s.upper().startswith(icao + " "):
        s = s[len(icao):].lstrip()
    if s.upper().startswith("METAR "):
        s = s[6:].lstrip()
    if len(s) > 200:
        s = s[:200].rstrip() + "…"
    return s

# Debug buffer
# -------------------------
@dataclass
class DebugEntry:
    ts: int
    from_id: str
    typ: str
    text: str

DEBUG_LOCK = threading.Lock()
DEBUG_BUFFER: deque[DebugEntry] = deque(maxlen=DEBUG_BUFFER_MAX)

# Prevent duplicate logging if both on_any_packet and on_text fire for same packet id


def debug_add(packet: Dict[str, Any]) -> None:
    try:
        entry = DebugEntry(ts=utc_ts(),
                           from_id=str(packet.get("fromId") or ""),
                           typ=packet_type(packet),
                           text=pretty_packet(packet))
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
            direction TEXT NOT NULL,
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
            status TEXT NOT NULL,
            to_id TEXT,
            channel INTEGER,
            text TEXT NOT NULL,
            tries INTEGER NOT NULL DEFAULT 0,
            last_error TEXT
        );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_outbox_status ON outbox(status, created_ts);")

        cur.execute("""
        CREATE TABLE IF NOT EXISTS nodes (
            node_id TEXT PRIMARY KEY,
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

        cur.execute("""
        CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
            """
        )

        cur.execute(
            """
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

        # migrations for older DBs
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

        conn.commit()


def get_setting(conn: sqlite3.Connection, key: str, default: str) -> str:
    """Read a setting from DB. If table doesn't exist yet, create it and return default."""
    conn.row_factory = sqlite3.Row
    with DB_LOCK:
        cur = conn.cursor()
        try:
            cur.execute("SELECT value FROM settings WHERE key=? LIMIT 1", (key,))
            row = cur.fetchone()
        except sqlite3.OperationalError:
            # DB upgraded from older version: create settings table lazily
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
                """
            )
            conn.commit()
            row = None
    if row and row[0] is not None:
        return str(row[0])
    return default


def set_setting(conn: sqlite3.Connection, key: str, value: str) -> None:
    """Write a setting into DB, creating table if needed."""
    with DB_LOCK:
        cur = conn.cursor()
        try:
            cur.execute(
                "INSERT INTO settings(key, value) VALUES(?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, str(value)),
            )
        except sqlite3.OperationalError:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
                """
            )
            cur.execute(
                "INSERT INTO settings(key, value) VALUES(?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, str(value)),
            )
        conn.commit()


def log_service_event(conn: sqlite3.Connection, service: str, from_id: Optional[str], to_id: Optional[str],
                      query: Optional[str], status: str, note: str = "") -> None:
    """Lightweight service usage log (best-effort)."""
    try:
        ts = utc_ts()
        ts_iso = iso_utc(ts)
        with DB_LOCK:
            cur = conn.cursor()
            cur.execute("""CREATE TABLE IF NOT EXISTS service_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts INTEGER NOT NULL,
                ts_iso TEXT NOT NULL,
                service TEXT NOT NULL,
                from_id TEXT,
                to_id TEXT,
                query TEXT,
                status TEXT,
                note TEXT
            );""")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_service_log_ts ON service_log(ts);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_service_log_service_ts ON service_log(service, ts);")
            cur.execute(
                "INSERT INTO service_log(ts, ts_iso, service, from_id, to_id, query, status, note) VALUES(?,?,?,?,?,?,?,?)",
                (ts, ts_iso, service, from_id, to_id, query, status, (note or "")[:200]),
            )
            conn.commit()
    except Exception:
        pass


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
            last_seen_ts = CASE
    WHEN excluded.last_seen_ts IS NULL THEN nodes.last_seen_ts
    WHEN nodes.last_seen_ts IS NULL THEN excluded.last_seen_ts
    WHEN excluded.last_seen_ts > nodes.last_seen_ts THEN excluded.last_seen_ts
    ELSE nodes.last_seen_ts
END,
last_seen_iso = CASE
    WHEN excluded.last_seen_ts IS NULL THEN nodes.last_seen_iso
    WHEN nodes.last_seen_ts IS NULL THEN excluded.last_seen_iso
    WHEN excluded.last_seen_ts > nodes.last_seen_ts THEN excluded.last_seen_iso
    ELSE nodes.last_seen_iso
END,
            last_rssi = COALESCE(excluded.last_rssi, nodes.last_rssi),
            last_snr = COALESCE(excluded.last_snr, nodes.last_snr),
            last_lat = COALESCE(excluded.last_lat, nodes.last_lat),
            last_lon = COALESCE(excluded.last_lon, nodes.last_lon),
            last_alt = COALESCE(excluded.last_alt, nodes.last_alt),
            last_pos_ts = CASE
    WHEN excluded.last_pos_ts IS NULL THEN nodes.last_pos_ts
    WHEN nodes.last_pos_ts IS NULL THEN excluded.last_pos_ts
    WHEN excluded.last_pos_ts > nodes.last_pos_ts THEN excluded.last_pos_ts
    ELSE nodes.last_pos_ts
END,
last_pos_iso = CASE
    WHEN excluded.last_pos_ts IS NULL THEN nodes.last_pos_iso
    WHEN nodes.last_pos_ts IS NULL THEN excluded.last_pos_iso
    WHEN excluded.last_pos_ts > nodes.last_pos_ts THEN excluded.last_pos_iso
    ELSE nodes.last_pos_iso
END
        """, (node_id, node_num, long_name, short_name, hw_model, macaddr,
              seen_ts, seen_iso, last_rssi, last_snr,
              lat, lon, alt, pos_ts, pos_iso))
        conn.commit()

def insert_position(conn: sqlite3.Connection, node_id: str, ts: int, lat: float, lon: float,
                    alt: Optional[float], keep_last_n: int) -> None:
    if not node_id:
        return
    keep_last_n = max(5, min(int(keep_last_n), 2000))
    with DB_LOCK:
        cur = conn.cursor()
        cur.execute("""
        INSERT INTO positions(node_id, ts, ts_iso, lat, lon, alt)
        VALUES(?,?,?,?,?,?)
        """, (node_id, ts, iso_utc(ts), lat, lon, alt))
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
          AND (
                -- normal case: to_id exists and is not broadcast
                (m.to_id IS NOT NULL AND lower(m.to_id) NOT IN ('^all','!ffffffff','ffffffff','0xffffffff'))
                OR
                -- fallback case: to_id missing, use to_num to detect non-broadcast
                (m.to_id IS NULL AND m.to_num IS NOT NULL AND m.to_num != 4294967295)
              )
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

def get_track_points(conn: sqlite3.Connection, node_id: str, minutes: int) -> List[Tuple[float, float]]:
    """Return track points for the last N minutes, limited to the current UTC date."""
    conn.row_factory = sqlite3.Row
    minutes = max(1, min(int(minutes), 24 * 60))  # cap at 24h

    now = utc_ts()
    cutoff = now - minutes * 60

    # limit to current UTC date (00:00 UTC .. now)
    dt_now = datetime.fromtimestamp(now, tz=timezone.utc)
    start_of_day = int(datetime(dt_now.year, dt_now.month, dt_now.day, tzinfo=timezone.utc).timestamp())
    if cutoff < start_of_day:
        cutoff = start_of_day

    with DB_LOCK:
        cur = conn.cursor()
        cur.execute("""
        SELECT lat, lon
        FROM positions
        WHERE node_id = ?
          AND ts >= ?
        ORDER BY ts ASC, id ASC
        """, (node_id, cutoff))
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


def list_debug_nodes(conn: sqlite3.Connection, limit: int = 200) -> List[sqlite3.Row]:
    """Nodes for the Debug dropdown."""
    conn.row_factory = sqlite3.Row
    with DB_LOCK:
        cur = conn.cursor()
        cur.execute("""
        SELECT node_id, long_name, short_name, hw_model, last_seen_ts
        FROM nodes
        ORDER BY COALESCE(last_seen_ts, 0) DESC
        LIMIT ?
        """, (limit,))
        return cur.fetchall()
# -------------------------
# Meshtastic gateway thread
# -------------------------
class Gateway:
    def __init__(self, host: str, conn: sqlite3.Connection):
        self.my_id = None
        self.host = host
        self.db = conn
        self.iface: Optional[TCPInterface] = None
        self.stop_event = threading.Event()
        self._last_node_refresh_ts: int = 0

        pub.subscribe(self.on_text, "meshtastic.receive.text")
        pub.subscribe(self.on_any_packet, "meshtastic.receive")

    def connect(self) -> TCPInterface:
        print(f"[GW] Connecting to Meshtastic TCP: {self.host}")
        iface = TCPInterface(hostname=self.host)
        print("[GW] Connected.")
        self.iface = iface
        self.seed_nodes_from_iface()
        self._last_node_refresh_ts = 0
        self.refresh_nodes_from_iface(force=True)
        return iface

    def seed_nodes_from_iface(self) -> None:
        if not self.iface:
            return
        try:
            nodes = getattr(self.iface, "nodes", {}) or {}
        except Exception:
            nodes = {}

        for node_id, node in nodes.items():
            if not isinstance(node, dict):
                continue
            user = node.get("user", {}) if isinstance(node.get("user"), dict) else {}
            pos = parse_position_from_node(node)

            resolved_id = str(user.get("id") or node_id)
            if not resolved_id:
                continue

            upsert_node(
                self.db,
                node_id=resolved_id,
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
                insert_position(self.db, resolved_id, int(pos["time"]), pos["lat"], pos["lon"], pos.get("alt"), POSITION_KEEP_PER_NODE)

    def refresh_nodes_from_iface(self, force: bool = False) -> None:
        """Refresh long/short name + HW fields from iface.nodes (late-arriving / changed names)."""
        if not self.iface:
            return
        now = utc_ts()
        if not force and (now - self._last_node_refresh_ts) < NODE_REFRESH_INTERVAL_SEC:
            return
        self._last_node_refresh_ts = now

        try:
            nodes = getattr(self.iface, "nodes", {}) or {}
        except Exception:
            nodes = {}

        for node_id, node in nodes.items():
            if not isinstance(node, dict):
                continue
            user = node.get("user", {}) if isinstance(node.get("user"), dict) else {}
            resolved_id = str(user.get("id") or node_id)
            if not resolved_id:
                continue
            pos = parse_position_from_node(node)
            upsert_node(
                self.db,
                node_id=resolved_id,
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

    def on_any_packet(self, packet, interface):
        try:
            if isinstance(packet, dict):
                debug_add(packet)
            else:
                debug_add({"raw": str(packet)})
        except Exception as e:
            print(f"[GW] on_any_packet error: {e}")

        try:
            if not isinstance(packet, dict):
                return

            from_id_raw = packet.get("fromId")
            from_num = packet.get("from")
            from_id = derive_node_id(from_id_raw, from_num)
            rx_rssi = packet.get("rxRssi")
            rx_snr = packet.get("rxSnr")
            rx_time = int(packet.get("rxTime", utc_ts()))

            # refresh node metadata periodically
            try:
                self.refresh_nodes_from_iface()
            except Exception:
                pass

            # Robust text logging: some setups don't emit meshtastic.receive.text
            decoded = packet.get("decoded")
            if isinstance(decoded, dict):
                port = decoded.get("portnum")
                text = decoded.get("text")
                if (port == "TEXT_MESSAGE_APP" or text is not None):
                    pid = packet.get("id")
                    try:
                        pid_int = int(pid) if pid is not None else None
                    except Exception:
                        pid_int = None

                    if not _seen_text_packet_id(pid_int):
                        try:
                            log_incoming_text(self.db, packet)

                            # METAR service: respond to direct messages that start with "wx <ICAO>"
                            try:
                                text_in = safe_get(packet, "decoded", "text", default=None)
                                if text_in:
                                    mmx = re.match(r"^\s*wx\s+([A-Za-z]{4})\s*$", str(text_in), flags=re.IGNORECASE)
                                    if mmx:
                                        enabled = get_setting(self.db, METAR_ENABLED_KEY, "1")
                                        metar_enabled = True if enabled in ("1", "true", "yes", "on") else False
                                        if metar_enabled:
                                            icao = mmx.group(1).upper()
                                            to_id = packet.get("toId") or int_to_node_id(packet.get("to"))
                                            sender_id = packet.get("fromId") or int_to_node_id(packet.get("from"))
                                            if to_id and sender_id and to_id not in ("^all", "!ffffffff"):
                                                # Fetch METAR
                                                log_service_event(self.db, "metar", sender_id, to_id, icao, "req", "")
                                                metar = fetch_metar_text(icao)
                                                if metar:
                                                    reply = compact_metar(metar, icao)
                                                else:
                                                    reply = f"No METAR for {icao}"
                                                # Try immediate send (preferred), fallback to outbox
                                                sent_ok = False
                                                if getattr(self, 'iface', None) is not None:
                                                    try:
                                                        # DM reply; channel index is not required for PKI DMs
                                                        time.sleep(int(get_setting(self.db, METAR_DELAY_KEY, "3") or 3))

                                                        self.iface.sendText(reply, destinationId=sender_id)
                                                        sent_ok = True
                                                        log_service_event(self.db, "metar", sender_id, to_id, icao, "ok", reply)
                                                    except Exception as e_send:
                                                        debug_add({'ts': iso_now(), 'fromId': sender_id, 'type': 'svc', 'note': f'metar sendText failed: {e_send}'})
                                                        log_service_event(self.db, "metar", sender_id, to_id, icao, "fail", f"sendText: {e_send}")
                                                if not sent_ok:
                                                    try:
                                                        ch = packet.get('channel')
                                                        try:
                                                            ch_i = int(ch) if ch is not None else None
                                                        except Exception:
                                                            ch_i = None
                                                        queue_outbox(self.db, sender_id, ch_i, reply)
                                                        log_service_event(self.db, "metar", sender_id, to_id, icao, "queued", reply)
                                                    except Exception as e_q:
                                                        debug_add({'ts': iso_now(), 'fromId': sender_id, 'type': 'svc', 'note': f'metar queue failed: {e_q}'})
                                                        log_service_event(self.db, "metar", sender_id, to_id, icao, "fail", f"queue: {e_q}")
                            except Exception as e:
                                # keep gateway alive but record why it failed
                                try:
                                    debug_add({'ts': iso_now(), 'type': 'svc', 'note': f'metar handler error: {e}'})
                                except Exception:
                                    pass
                        except Exception:
                            pass
                        try:
                            print(f"[GW] IN {packet.get('fromId')} -> {packet.get('toId')}: {text}")
                        except Exception:
                            pass


            # Update last_seen and (if present) position
            if from_id is None:
                return
            pos = parse_position_from_packet(packet)
            upsert_node(
                self.db,
                node_id=from_id or "",
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
                insert_position(self.db, from_id or "", p_ts, pos["lat"], pos["lon"], pos.get("alt"), POSITION_KEEP_PER_NODE)

        except Exception as e:
            print(f"[GW] on_any_packet error: {e}")

    def on_text(self, packet, interface):
        try:
            if isinstance(packet, dict):
                pid = packet.get("id")
                try:
                    pid_int = int(pid) if pid is not None else None
                except Exception:
                    pid_int = None
                if _seen_text_packet_id(pid_int):
                    return
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

                # even during quiet periods, refresh names/HW
                self.refresh_nodes_from_iface()

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
# Flask app
# -------------------------
app = Flask(__name__, template_folder="templates", static_folder="static")

DB_CONN = db_connect()
init_db(DB_CONN)

gateway = Gateway(HOST, DB_CONN)

# Make helper available in templates
app.jinja_env.globals["node_display"] = node_display
app.jinja_env.globals["iso_utc"] = iso_utc

# -------------------------
# Routes
# -------------------------
@app.get("/")
def home():
    return redirect(url_for("nodes"))

@app.get("/inbox")
def inbox():
    limit = int(request.args.get("limit", "50"))
    limit = 50 if limit not in (50, 100, 500) else limit
    msgs = list_inbox(DB_CONN, limit=limit)
    return render_template("inbox.html", host=HOST, extra_head="", rows=msgs, limit=limit)

@app.post("/msg/delete/<int:msg_id>", endpoint="message_delete")
def delete_msg(msg_id: int):
    delete_message(DB_CONN, msg_id)
    return redirect(url_for("inbox"))

@app.get("/outbox")
def outbox_view():
    limit = int(request.args.get("limit", "50"))
    limit = 50 if limit not in (50, 100, 500) else limit
    msgs = list_outbox(DB_CONN, limit=limit)
    return render_template("outbox.html", host=HOST, extra_head="", rows=msgs, limit=limit)

@app.post("/outbox/delete/<int:out_id>")
def outbox_delete(out_id: int):
    delete_outbox(DB_CONN, out_id)
    return redirect(url_for("outbox_view"))

@app.get("/broadcast")
def broadcast():
    limit = int(request.args.get("limit", "50"))
    limit = 50 if limit not in (50, 100, 500) else limit

    DB_CONN.row_factory = sqlite3.Row
    with DB_LOCK:
        cur = DB_CONN.cursor()
        cur.execute("""
        SELECT m.id, m.ts, m.ts_iso, m.direction, m.from_id, m.to_id, m.text, m.rx_rssi, m.rx_snr,
               nf.long_name AS from_long, nf.short_name AS from_short
        FROM messages m
        LEFT JOIN nodes nf ON nf.node_id = m.from_id
        WHERE (
                (m.to_id IS NOT NULL AND lower(m.to_id) IN ('^all','!ffffffff','ffffffff','0xffffffff'))
                OR
                (m.to_id IS NULL AND (m.to_num IS NULL OR m.to_num = 4294967295))
              )
        ORDER BY m.ts DESC, m.id DESC
        LIMIT ?
        """, (limit,))
        msgs = cur.fetchall()
        msgs = list(reversed(msgs))

        cur.execute("""
        SELECT id, created_ts_iso, text
        FROM outbox
        WHERE status='queued' AND (to_id IS NULL OR lower(to_id) IN ('^all','!ffffffff','ffffffff','0xffffffff'))
        ORDER BY created_ts ASC
        LIMIT 10
        """)
        queued = cur.fetchall()

    return render_template("broadcast.html", host=HOST, extra_head="", msgs=msgs, queued=queued, limit=limit)

@app.post("/broadcast/send")
def broadcast_send():
    text = (request.form.get("text", "") or "").strip()
    if not text:
        return redirect(url_for("broadcast"))
    queue_outbox(DB_CONN, None, None, text)
    return redirect(url_for("broadcast"))

@app.get("/send")
def send_form():
    window = request.args.get("window", DEFAULT_ACTIVE_WINDOW)
    win_sec = parse_window_to_seconds(window)
    rows = list_active_nodes(DB_CONN, win_sec)
    return render_template("send.html", host=HOST, extra_head="", rows=rows, window=window)

@app.post("/send")
def send_post():
    to_id = (request.form.get("to_id", "") or "").strip()
    text = (request.form.get("text", "") or "").strip()
    if not text:
        return redirect(url_for("send_form"))
    queue_outbox(DB_CONN, to_id if to_id else None, None, text)
    return redirect(url_for("outbox_view"))

@app.get("/nodes")
def nodes():
    window = request.args.get("window", DEFAULT_ACTIVE_WINDOW)
    win_sec = parse_window_to_seconds(window)
    rows = list_active_nodes(DB_CONN, win_sec)
    return render_template("nodes.html", host=HOST, extra_head="", rows=rows, window=window)

@app.get("/chat/<path:node_id>")
def chat_with(node_id: str):
    node_id = (node_id or "").strip()
    if not node_id:
        return redirect(url_for("nodes"))
    limit = int(request.args.get("limit", str(DEFAULT_CHAT_LIMIT)))
    limit = max(10, min(limit, 2000))
    msgs = chat_messages(DB_CONN, node_id, limit=limit)
    title = node_label_for_id(DB_CONN, node_id)
    return render_template("chat.html", host=HOST, extra_head="", msgs=msgs, node_id=node_id, limit=limit, title=title)

@app.post("/chat/<path:node_id>/send")
def chat_send(node_id: str):
    node_id = (node_id or "").strip()
    text = (request.form.get("text", "") or "").strip()
    if not text:
        return redirect(url_for("chat_with", node_id=node_id))
    queue_outbox(DB_CONN, node_id, None, text)
    return redirect(url_for("chat_with", node_id=node_id))

@app.get("/map")
def map_view():
    window = request.args.get("window", DEFAULT_ACTIVE_WINDOW)
    win_sec = parse_window_to_seconds(window)
    track_n = int(request.args.get("n", str(DEFAULT_MAP_TRACK_POINTS)))
    track_n = max(1, min(track_n, 24 * 60))
    rows = list_nodes_with_position(DB_CONN, win_sec)
    if rows:
        center_lat = float(rows[0]["last_lat"])
        center_lon = float(rows[0]["last_lon"])
    else:
        center_lat, center_lon = 60.1699, 24.9384
    return render_template("map.html", host=HOST, rows=rows, window=window, track_n=track_n,
                           center_lat=center_lat, center_lon=center_lon)

@app.get("/map/data")
def map_data():
    window = request.args.get("window", DEFAULT_ACTIVE_WINDOW)
    win_sec = parse_window_to_seconds(window)
    rows = list_nodes_with_position(DB_CONN, win_sec)
    nodes = []
    for r in rows:
        nodes.append({
            "node_id": r["node_id"],
            "name": node_display(r["long_name"], r["short_name"], r["node_id"]),
            "hw": r["hw_model"],
            "last_seen": r["last_seen_iso"],
            "rssi": r["last_rssi"],
            "snr": r["last_snr"],
            "lat": float(r["last_lat"]),
            "lon": float(r["last_lon"]),
            "last_pos": r["last_pos_iso"],
        })
    return jsonify({"nodes": nodes})

@app.get("/map/tracks")
def map_tracks():
    ids = (request.args.get("ids", "") or "").strip()
    n = int(request.args.get("n", str(DEFAULT_MAP_TRACK_POINTS)))
    n = max(1, min(n, 24 * 60))
    node_ids = [x.strip() for x in ids.split(",") if x.strip().startswith("!")]
    node_ids = node_ids[:25]
    tracks: Dict[str, List[List[float]]] = {}
    for node_id in node_ids:
        pts = get_track_points(DB_CONN, node_id, n)
        if len(pts) >= 2:
            tracks[node_id] = [[lat, lon] for (lat, lon) in pts]
    return jsonify({"tracks": tracks})


@app.get("/services")
def services_view():
    enabled = get_setting(DB_CONN, METAR_ENABLED_KEY, "1")
    metar_enabled = True if enabled in ("1", "true", "yes", "on") else False

    # Latest METAR requests (best-effort)
    DB_CONN.row_factory = sqlite3.Row
    with DB_LOCK:
        cur = DB_CONN.cursor()
        try:
            cur.execute("""
                SELECT ts_iso, from_id, query, status, note
                FROM service_log
                WHERE service='metar'
                ORDER BY id DESC
                LIMIT 50
            """)
            metar_log = [dict(r) for r in cur.fetchall()]
        except Exception:
            metar_log = []

    return render_template("services.html", host=HOST, extra_head="", metar_enabled=metar_enabled, metar_log=metar_log)


@app.post("/services")
def services_post():
    val = (request.form.get("metar_enabled", "0") or "0").strip().lower()
    metar_enabled = val in ("1", "true", "yes", "on", "enabled")
    set_setting(DB_CONN, METAR_ENABLED_KEY, "1" if metar_enabled else "0")
    return redirect(url_for("services_view"))


@app.get("/debug")
def debug_view():
    n = int(request.args.get("n", str(DEBUG_SHOW_DEFAULT)))
    typ = (request.args.get("typ", "all") or "all").strip().lower()

    # UI uses query param 'node'
    node_id = (request.args.get("node", "") or "").strip()

    entries = debug_filtered(n=n, typ=typ, node_id=node_id)
    debug_nodes = list_debug_nodes(DB_CONN, limit=250)

    default_n = n

    return render_template(
        "debug.html",
        host=HOST,
        extra_head="",
        entries=entries,
        debug_nodes=debug_nodes,
        default_n=default_n,
        n=n,
        typ=typ,
        node_id=node_id,
        maxn=DEBUG_BUFFER_MAX,
    )

@app.get("/debug/data")
def debug_data():
    n = int(request.args.get("n", str(DEBUG_SHOW_DEFAULT)))
    typ = (request.args.get("typ", "all") or "all").strip().lower()
    node_id = (request.args.get("node", "") or "").strip()

    entries = debug_filtered(n=n, typ=typ, node_id=node_id)
    out = []
    for e in entries:
        out.append(
            f"# {e.ts} ({iso_utc(e.ts)}) from {e.from_id} type={e.typ}\n{e.text}\n"
        )

    return jsonify({"text": "\n".join(out)})

@app.post("/debug/clear")
def debug_clear_post():
    debug_clear()
    return redirect(url_for("debug_view"))

@app.get("/status")
def status_view():
    connected = gateway.iface is not None

    live_node = None
    iface_summary = None

    if connected:
        try:
            live_node = gateway.iface.getMyNodeInfo()
        except Exception:
            try:
                ln = getattr(gateway.iface, "localNode", None)
                live_node = ln if isinstance(ln, dict) else (ln.__dict__ if ln else None)
            except Exception:
                live_node = None

        try:
            nodes_obj = getattr(gateway.iface, "nodes", {}) or {}
            iface_summary = {
                "nodes_count": len(nodes_obj),
            }
            try:
                iface_summary["node_ids"] = list(nodes_obj.keys())[:50]
            except Exception:
                pass
        except Exception:
            iface_summary = None

    local = None
    try:
        local_id = None
        if isinstance(live_node, dict):
            local_id = (live_node.get("user") or {}).get("id") or live_node.get("id")
        if local_id:
            DB_CONN.row_factory = sqlite3.Row
            with DB_LOCK:
                cur = DB_CONN.cursor()
                cur.execute("SELECT * FROM nodes WHERE node_id=? LIMIT 1", (str(local_id),))
                local = cur.fetchone()
    except Exception:
        local = None

    live_node_json = json.dumps(live_node, indent=2, ensure_ascii=False, default=str) if live_node else ""
    iface_json = json.dumps(iface_summary, indent=2, ensure_ascii=False, default=str) if iface_summary else ""

    return render_template(
        "status.html",
        host=HOST,
        extra_head="",
        connected=connected,
        local=local,
        live_node_json=live_node_json,
        iface_json=iface_json,
    )

def start_gateway_thread() -> threading.Thread:
    t = threading.Thread(target=gateway.run, name="meshtastic-gateway", daemon=True)
    t.start()
    return t

if __name__ == "__main__":
    start_gateway_thread()
    app.run(host=WEB_HOST, port=WEB_PORT, debug=False, threaded=True)
