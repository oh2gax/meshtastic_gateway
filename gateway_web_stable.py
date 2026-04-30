#!/usr/bin/env python3
"""
gateway_web_stable.py

Web-based Meshtastic gateway: Flask UI + SQLite store, connecting to a
Meshtastic device over TCP/WiFi.

Highlights:
- UI uses templates/ + static/ (CSS extracted) to prevent accidental UI drift.
- Light/dark theme toggle persisted in localStorage; map tiles follow the theme.
- Multi-channel broadcast support; broadcasts kept separate from Inbox/Outbox.
- ACK tracking for outgoing direct messages (Y / retries / pending / unknown).
- Map: filled circles, freshness coloring (green <= 15 min, blue older),
  per-node track lines, distance-from-gateway in the side list.
- Services: METAR weather replies to direct messages like "wx EFHK", with
  configurable reply delay and a service log. Service-command DMs are
  filtered out of the Inbox.
- Inbox: unread DMs are highlighted with a "New message" indicator in the
  navbar; per-message and bulk Mark-read controls.
- Lightweight schema migrations on startup, so upgrading the script
  generally does not require touching the database.

Run:
  python3 gateway_web_stable.py

Folder layout (keep together):
  gateway_web_stable.py
  meshtastic_messages.db   (optional; default path set in DB_PATH below)
  templates/
  static/
"""

import json
import math
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
HOST = "192.168.1.197"

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

def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance between two points (km)."""
    R = 6371.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

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

# TAF (Terminal Aerodrome Forecast) — same NOAA TGFTP host, different path.
TAF_BASE_URL = "https://tgftp.nws.noaa.gov/data/forecasts/taf/stations/{icao}.TXT"
TAF_MAX_LEN = 220  # truncate to fit a single Meshtastic DM payload comfortably

# Solar / propagation summary from hamqsl.com (XML, free, no auth).
SOLAR_BASE_URL = "https://www.hamqsl.com/solarxml.php"
SOLAR_MAX_LEN = 220

# Pattern(s) for messages that the gateway treats as service commands.
# Service commands are NOT stored in the inbox.
SERVICE_COMMAND_PATTERNS = (
    re.compile(r"^\s*wx\s+[A-Za-z]{4}\s*$", re.IGNORECASE),   # METAR: "wx EFHK"
    re.compile(r"^\s*taf\s+[A-Za-z]{4}\s*$", re.IGNORECASE),  # TAF:   "taf EFHK"
    re.compile(r"^\s*cmd\s+solar\s*$", re.IGNORECASE),        # Solar: "cmd solar"
)

def is_service_command(text: Optional[str]) -> bool:
    """Return True if the text matches any known service-command pattern."""
    if not text:
        return False
    s = str(text)
    for pat in SERVICE_COMMAND_PATTERNS:
        if pat.match(s):
            return True
    return False


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

def fetch_taf_text(icao: str, timeout_sec: int = 8) -> Optional[str]:
    """Fetch latest TAF for ICAO (e.g. EFHK). Returns the raw TAF body
    (without the leading 'YYYY/MM/DD HH:MM' timestamp line and with
    multiline forecasts collapsed to a single line) or None on failure."""
    icao = (icao or "").strip().upper()
    if not re.fullmatch(r"[A-Z]{4}", icao):
        return None
    url = TAF_BASE_URL.format(icao=icao)
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "MeshtasticGateway/1.0"})
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            data = resp.read().decode("utf-8", errors="replace")
    except Exception:
        return None

    lines = [ln.strip() for ln in data.splitlines() if ln.strip()]
    if not lines:
        return None
    # First line is the issue timestamp "YYYY/MM/DD HH:MM"; drop it.
    if re.match(r"^\d{4}/\d{2}/\d{2}\s+\d{2}:\d{2}$", lines[0]):
        body_lines = lines[1:]
    else:
        body_lines = lines
    if not body_lines:
        return None
    # Collapse continuation lines (BECMG/TEMPO blocks etc.) into one line.
    body = " ".join(part.strip() for part in body_lines if part.strip())
    body = re.sub(r"\s+", " ", body).strip()
    return body or None


def compact_taf(taf: str, icao: str) -> str:
    """Strip the 'TAF EFHK' prefix to save bytes (the user already knows
    the airport from their query) but keep the ddhhmmZ issuance timestamp
    that follows it. Truncate to TAF_MAX_LEN chars with an ellipsis."""
    s = (taf or "").strip()
    if not s:
        return ""
    s = re.sub(r"=\s*$", "", s).strip()
    icao = (icao or "").strip().upper()
    # Drop a leading "TAF " or "TAF AMD " / "TAF COR ".
    s = re.sub(r"^TAF(?:\s+(?:AMD|COR))?\s+", "", s, flags=re.IGNORECASE)
    # Drop the ICAO that immediately follows the keyword.
    if icao and s.upper().startswith(icao + " "):
        s = s[len(icao):].lstrip()
    if len(s) > TAF_MAX_LEN:
        s = s[:TAF_MAX_LEN].rstrip() + "…"
    return s


def fetch_solar_data(timeout_sec: int = 10) -> Optional[Dict[str, str]]:
    """Fetch hamqsl solar XML and return a dict of the relevant tag values.
    Values are stripped of whitespace; tags whose value is "No Report" or
    empty are omitted. Returns None on transport failure."""
    try:
        req = urllib.request.Request(SOLAR_BASE_URL, headers={"User-Agent": "MeshtasticGateway/1.0"})
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            data = resp.read().decode("utf-8", errors="replace")
    except Exception:
        return None

    # Tags we care about. The hamqsl XML uses these names verbatim.
    wanted = (
        "solarflux", "sunspots", "xray",
        "aindex", "kindex", "aurora",
        "magneticfield", "solarwind",
        "protonflux", "electonflux",
    )
    out: Dict[str, str] = {}
    for tag in wanted:
        m = re.search(r"<" + tag + r">\s*([^<]+?)\s*</" + tag + r">",
                      data, flags=re.IGNORECASE)
        if not m:
            continue
        val = m.group(1).strip()
        if not val or val.lower() == "no report":
            continue
        out[tag] = val
    return out or None


def compact_solar(data: Optional[Dict[str, str]]) -> str:
    """Build a compact one-line propagation summary from hamqsl XML data.
    Field order (only included when present):
      SFI SN X A K AU BZE SW Pf Ef"""
    if not data:
        return ""
    parts: List[str] = []
    def add(label: str, key: str) -> None:
        v = data.get(key)
        if v:
            parts.append(f"{label} {v}")
    add("SFI", "solarflux")
    add("SN",  "sunspots")
    add("X",   "xray")
    add("A",   "aindex")
    add("K",   "kindex")
    add("AU",  "aurora")
    add("BZE", "magneticfield")
    add("SW",  "solarwind")
    add("Pf",  "protonflux")
    add("Ef",  "electonflux")
    s = " ".join(parts)
    if len(s) > SOLAR_MAX_LEN:
        s = s[:SOLAR_MAX_LEN].rstrip() + "…"
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
            channel_name TEXT,
            text TEXT,
            rx_rssi REAL,
            rx_snr REAL,
            hop_limit INTEGER,
            hop_start INTEGER,
            want_ack INTEGER,
            packet_id INTEGER,
            raw_json TEXT,
            is_read INTEGER DEFAULT 0
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
            channel_name TEXT,
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
            last_pos_iso TEXT,
            last_hops INTEGER
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
            ("last_hops", "INTEGER"),
        ]:
            if not _col_exists(cur, "nodes", col):
                try:
                    cur.execute(f"ALTER TABLE nodes ADD COLUMN {col} {typ};")
                except Exception:
                    pass

        # migrate channel_name onto messages/outbox for older DBs
        for tbl in ("messages", "outbox"):
            if not _col_exists(cur, tbl, "channel_name"):
                try:
                    cur.execute(f"ALTER TABLE {tbl} ADD COLUMN channel_name TEXT;")
                except Exception:
                    pass

        # migrate is_read onto messages for older DBs; mark existing inbox
        # rows as already read so the user isn't flooded with stale unreads.
        if not _col_exists(cur, "messages", "is_read"):
            try:
                cur.execute("ALTER TABLE messages ADD COLUMN is_read INTEGER DEFAULT 0;")
                cur.execute("UPDATE messages SET is_read = 1 WHERE direction = 'in';")
            except Exception:
                pass
        cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_is_read ON messages(is_read, direction);")

        # ACK tracking on outbox
        for col, typ in [
            ("packet_id",   "INTEGER"),
            ("ack_status",  "TEXT"),
            ("ack_ts",      "INTEGER"),
            ("ack_ts_iso",  "TEXT"),
            ("sent_ts",     "INTEGER"),
            ("sent_ts_iso", "TEXT"),
        ]:
            if not _col_exists(cur, "outbox", col):
                try:
                    cur.execute(f"ALTER TABLE outbox ADD COLUMN {col} {typ};")
                except Exception:
                    pass
        cur.execute("CREATE INDEX IF NOT EXISTS idx_outbox_packet_id ON outbox(packet_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_outbox_ack_status ON outbox(ack_status);")

        # per-device channel map (index <-> friendly name, scoped to the gateway radio)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS channels (
            device_id TEXT NOT NULL,
            ch_index INTEGER NOT NULL,
            name TEXT,
            role TEXT,
            updated_ts INTEGER,
            PRIMARY KEY (device_id, ch_index)
        );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_channels_device ON channels(device_id);")

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
                pos_ts: Optional[int] = None,
                last_hops: Optional[int] = None) -> None:
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
                          last_lat, last_lon, last_alt, last_pos_ts, last_pos_iso,
                          last_hops)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
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
END,
            last_hops = COALESCE(excluded.last_hops, nodes.last_hops)
        """, (node_id, node_num, long_name, short_name, hw_model, macaddr,
              seen_ts, seen_iso, last_rssi, last_snr,
              lat, lon, alt, pos_ts, pos_iso,
              last_hops))
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

def _is_broadcast_target(to_id: Optional[str], to_num: Optional[int]) -> bool:
    """Match the broadcast markers used by list_inbox so DMs and broadcasts
    are classified consistently here and on the inbox page."""
    if to_id is not None:
        if str(to_id).lower() in ("^all", "!ffffffff", "ffffffff", "0xffffffff"):
            return True
        return False
    if to_num is not None:
        return to_num == 4294967295
    return True  # unknown -> treat as non-DM


def log_incoming_text(conn: sqlite3.Connection, packet: Dict[str, Any],
                      channel_name: Optional[str] = None) -> None:
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

    # Direct messages start unread; broadcasts/unknown targets are pre-read so
    # they never trigger the "New message" indicator.
    is_read_val = 0 if not _is_broadcast_target(to_id, to_num) else 1

    with DB_LOCK:
        cur = conn.cursor()
        cur.execute("""
        INSERT INTO messages(
            ts, ts_iso, direction,
            from_id, to_id, from_num, to_num,
            channel, channel_name, text,
            rx_rssi, rx_snr, hop_limit, hop_start, want_ack,
            packet_id, raw_json, is_read
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            ts, iso_utc(ts), "in",
            from_id, to_id, from_num, to_num,
            channel, channel_name, text,
            rx_rssi, rx_snr,
            packet.get("hopLimit"), packet.get("hopStart"),
            1 if packet.get("wantAck") else 0,
            packet.get("id"),
            json.dumps(packet, ensure_ascii=False, default=str),
            is_read_val,
        ))
        conn.commit()

def log_outgoing_message(conn: sqlite3.Connection, to_id: Optional[str], channel: Optional[int],
                        text: str, status: str, err: Optional[str] = None,
                        channel_name: Optional[str] = None) -> None:
    ts = utc_ts()
    with DB_LOCK:
        cur = conn.cursor()
        cur.execute("""
        INSERT INTO messages(
            ts, ts_iso, direction,
            from_id, to_id, channel, channel_name, text, raw_json
        ) VALUES(?,?,?,?,?,?,?,?,?)
        """, (ts, iso_utc(ts), "out", None, to_id, channel, channel_name, text,
              json.dumps({"status": status, "err": err}, ensure_ascii=False)))
        conn.commit()

def queue_outbox(conn: sqlite3.Connection, to_id: Optional[str], channel: Optional[int], text: str,
                 channel_name: Optional[str] = None) -> None:
    ts = utc_ts()
    with DB_LOCK:
        cur = conn.cursor()
        cur.execute("""
        INSERT INTO outbox(created_ts, created_ts_iso, status, to_id, channel, channel_name, text, tries, last_error)
        VALUES(?,?,?,?,?,?,?,0,NULL)
        """, (ts, iso_utc(ts), "queued", to_id, channel, channel_name, text))
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

def mark_outbox(conn: sqlite3.Connection, row_id: int, status: str, tries: int, last_error: Optional[str],
                packet_id: Optional[int] = None, ack_status: Optional[str] = None,
                sent_ts: Optional[int] = None) -> None:
    sent_iso = iso_utc(sent_ts) if sent_ts else None
    with DB_LOCK:
        cur = conn.cursor()
        # COALESCE keeps the existing value when the new one is None
        cur.execute("""
        UPDATE outbox
        SET status=?, tries=?, last_error=?,
            packet_id   = COALESCE(?, packet_id),
            ack_status  = COALESCE(?, ack_status),
            sent_ts     = COALESCE(?, sent_ts),
            sent_ts_iso = COALESCE(?, sent_ts_iso)
        WHERE id=?
        """, (status, tries, last_error, packet_id, ack_status, sent_ts, sent_iso, row_id))
        conn.commit()


def update_outbox_ack_by_packet(conn: sqlite3.Connection, packet_id: int, ack_status: str) -> int:
    """Called from the ACK response callback. Returns number of rows updated."""
    if packet_id is None:
        return 0
    ts = utc_ts()
    with DB_LOCK:
        cur = conn.cursor()
        cur.execute("""
        UPDATE outbox
        SET ack_status = ?, ack_ts = ?, ack_ts_iso = ?
        WHERE packet_id = ?
        """, (ack_status, ts, iso_utc(ts), int(packet_id)))
        conn.commit()
        return cur.rowcount or 0


def sweep_stale_pending_acks(conn: sqlite3.Connection, max_age_sec: int = 180) -> int:
    """Mark any pending outbox rows older than max_age_sec as 'no_ack'. Returns rows affected."""
    cutoff = utc_ts() - max_age_sec
    with DB_LOCK:
        cur = conn.cursor()
        cur.execute("""
        UPDATE outbox
        SET ack_status = 'no_ack'
        WHERE ack_status = 'pending'
          AND COALESCE(sent_ts, created_ts) < ?
        """, (cutoff,))
        conn.commit()
        return cur.rowcount or 0

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
               COALESCE(m.is_read, 0) AS is_read,
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


def count_unread_dm(conn: sqlite3.Connection) -> int:
    with DB_LOCK:
        cur = conn.cursor()
        cur.execute("""
        SELECT COUNT(*) FROM messages
        WHERE direction='in'
          AND COALESCE(is_read, 0) = 0
          AND (
                (to_id IS NOT NULL AND lower(to_id) NOT IN ('^all','!ffffffff','ffffffff','0xffffffff'))
                OR
                (to_id IS NULL AND to_num IS NOT NULL AND to_num != 4294967295)
              )
        """)
        row = cur.fetchone()
        return int(row[0] if row else 0)


def mark_message_read(conn: sqlite3.Connection, msg_id: int) -> None:
    with DB_LOCK:
        cur = conn.cursor()
        cur.execute("UPDATE messages SET is_read=1 WHERE id=?", (msg_id,))
        conn.commit()


def mark_all_inbox_read(conn: sqlite3.Connection) -> None:
    with DB_LOCK:
        cur = conn.cursor()
        cur.execute("""
        UPDATE messages SET is_read=1
        WHERE direction='in' AND COALESCE(is_read, 0) = 0
        """)
        conn.commit()

def list_outbox(conn: sqlite3.Connection, limit: int = 50) -> List[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    with DB_LOCK:
        cur = conn.cursor()
        cur.execute("""
        SELECT o.id, o.created_ts, o.created_ts_iso, o.status, o.to_id, o.channel, o.channel_name,
               o.text, o.tries, o.last_error,
               o.packet_id, o.ack_status, o.ack_ts_iso, o.sent_ts_iso,
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
               last_lat, last_lon, last_alt, last_pos_ts, last_pos_iso,
               last_hops
        FROM nodes
        WHERE last_seen_ts IS NOT NULL AND last_seen_ts >= ?
        ORDER BY last_seen_ts DESC
        """, (cutoff,))
        return cur.fetchall()


# ---------------------------------------------------------------------------
# Unknown-node cleanup
# ---------------------------------------------------------------------------
# A node is "unknown" if we have its node_num but never received a NodeInfo
# packet that would tell us its long/short name. Such rows accumulate when we
# overhear traffic from distant nodes whose NodeInfo doesn't reach us.

UNKNOWN_NODE_AUTO_CLEAN_AGE_SEC = 24 * 3600           # 24 hours
UNKNOWN_NODE_AUTO_CLEAN_INTERVAL_SEC = 30 * 60        # check twice an hour


def find_unknown_node_rows(conn: sqlite3.Connection,
                           min_age_seconds: Optional[int] = None) -> List[sqlite3.Row]:
    """Return rows for nodes with no long_name and no short_name. If
    min_age_seconds is given, only rows older than that (by last_seen_ts)
    are returned."""
    conn.row_factory = sqlite3.Row
    with DB_LOCK:
        cur = conn.cursor()
        sql = """
        SELECT node_id, node_num, last_seen_ts
        FROM nodes
        WHERE (long_name IS NULL OR long_name = '')
          AND (short_name IS NULL OR short_name = '')
        """
        params: List[Any] = []
        if min_age_seconds is not None:
            sql += " AND last_seen_ts IS NOT NULL AND last_seen_ts <= ?"
            params.append(utc_ts() - int(min_age_seconds))
        cur.execute(sql, params)
        return cur.fetchall()


def list_all_nodes_for_picker(conn: sqlite3.Connection) -> List[sqlite3.Row]:
    """Return every node row, most recently seen first, suitable for a
    UI dropdown. Includes id, num, names and last-seen for display."""
    conn.row_factory = sqlite3.Row
    with DB_LOCK:
        cur = conn.cursor()
        cur.execute("""
        SELECT node_id, node_num, long_name, short_name, last_seen_iso, last_seen_ts
        FROM nodes
        ORDER BY last_seen_ts DESC NULLS LAST
        """)
        return cur.fetchall()


def remove_node_from_device(node_num: Optional[int],
                            node_id: Optional[str] = None) -> Tuple[bool, str]:
    """Send an admin command to the local Meshtastic device asking it to
    remove the given node from its NodeDB. Also evict the node from the
    Python library's in-memory caches (iface.nodes / iface.nodesByNum) so
    the gateway's periodic refresh doesn't immediately re-create the row.
    Returns (ok, error_msg)."""
    try:
        if gateway.iface is None:
            return False, "no interface"
        if node_num is None:
            return False, "missing node_num"
        ln = getattr(gateway.iface, "localNode", None)
        if ln is None:
            return False, "no localNode"
        try:
            ln.removeNode(int(node_num))
        except AttributeError:
            # Older library versions used deleteNode
            ln.deleteNode(int(node_num))  # type: ignore[attr-defined]
        # Evict from the library's caches so refresh_nodes_from_iface
        # doesn't resurrect the node a few seconds later.
        try:
            iface_nodes = getattr(gateway.iface, "nodes", None)
            if isinstance(iface_nodes, dict):
                if node_id:
                    iface_nodes.pop(node_id, None)
                # Also clean any cache entry that just matches by num,
                # in case the key form differs.
                for k, v in list(iface_nodes.items()):
                    try:
                        if isinstance(v, dict) and v.get("num") == int(node_num):
                            iface_nodes.pop(k, None)
                    except Exception:
                        pass
        except Exception:
            pass
        try:
            iface_nodes_by_num = getattr(gateway.iface, "nodesByNum", None)
            if isinstance(iface_nodes_by_num, dict):
                iface_nodes_by_num.pop(int(node_num), None)
        except Exception:
            pass
        return True, ""
    except Exception as e:
        return False, str(e)


def delete_node_from_db(conn: sqlite3.Connection, node_id: str) -> None:
    if not node_id:
        return
    with DB_LOCK:
        cur = conn.cursor()
        cur.execute("DELETE FROM nodes WHERE node_id = ?", (node_id,))
        conn.commit()


def remove_unknown_nodes(min_age_seconds: Optional[int] = None) -> Dict[str, int]:
    """Find unknown nodes and remove each from the device's NodeDB and the
    gateway's local DB. Refuses to touch the gateway's own node. Returns a
    summary dict with counts."""
    rows = find_unknown_node_rows(DB_CONN, min_age_seconds=min_age_seconds)
    own = getattr(gateway, "my_id", None)
    removed = failed = skipped = 0
    for r in rows:
        node_id = r["node_id"]
        node_num = r["node_num"]
        if node_id and own and str(node_id) == str(own):
            skipped += 1
            continue
        ok, err = remove_node_from_device(node_num, node_id=node_id)
        if ok:
            delete_node_from_db(DB_CONN, node_id)
            removed += 1
            try:
                debug_add({"ts": iso_now(), "type": "admin",
                           "note": f"removed unknown node {node_id} (num={node_num})"})
            except Exception:
                pass
        else:
            failed += 1
            try:
                debug_add({"ts": iso_now(), "type": "admin",
                           "note": f"remove failed for {node_id}: {err}"})
            except Exception:
                pass
    return {"scanned": len(rows), "removed": removed,
            "failed": failed, "skipped": skipped}


def _unknown_node_cleanup_loop() -> None:
    """Background loop: periodically purge unknown nodes older than the
    configured age threshold."""
    while True:
        try:
            time.sleep(UNKNOWN_NODE_AUTO_CLEAN_INTERVAL_SEC)
            res = remove_unknown_nodes(min_age_seconds=UNKNOWN_NODE_AUTO_CLEAN_AGE_SEC)
            if res.get("removed") or res.get("failed"):
                print(f"[GW] unknown-cleanup: {res}")
        except Exception as e:
            print(f"[GW] unknown-cleanup loop error: {e}")


def start_unknown_cleanup_thread() -> threading.Thread:
    t = threading.Thread(target=_unknown_node_cleanup_loop,
                         name="unknown-cleanup", daemon=True)
    t.start()
    return t

def list_nodes_with_position(conn: sqlite3.Connection, window_seconds: int) -> List[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    cutoff = utc_ts() - window_seconds
    with DB_LOCK:
        cur = conn.cursor()
        cur.execute("""
        SELECT node_id, long_name, short_name, hw_model,
               last_seen_iso, last_rssi, last_snr,
               last_lat, last_lon, last_alt, last_pos_iso, last_seen_ts,
               last_hops
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


# --------------------------------------------------------------------------------------
# Channels (per-device channel map)
# --------------------------------------------------------------------------------------
def upsert_channel(conn: sqlite3.Connection, device_id: str, ch_index: int,
                   name: Optional[str], role: Optional[str]) -> None:
    if not device_id:
        return
    ts = utc_ts()
    with DB_LOCK:
        cur = conn.cursor()
        cur.execute("""
        INSERT INTO channels(device_id, ch_index, name, role, updated_ts)
        VALUES(?,?,?,?,?)
        ON CONFLICT(device_id, ch_index) DO UPDATE SET
            name = excluded.name,
            role = excluded.role,
            updated_ts = excluded.updated_ts
        """, (device_id, int(ch_index), (name or ""), (role or ""), ts))
        conn.commit()


def list_channels_for_device(conn: sqlite3.Connection, device_id: Optional[str]) -> List[sqlite3.Row]:
    if not device_id:
        return []
    conn.row_factory = sqlite3.Row
    with DB_LOCK:
        cur = conn.cursor()
        cur.execute("""
        SELECT ch_index, name, role
        FROM channels
        WHERE device_id = ?
        ORDER BY ch_index ASC
        """, (device_id,))
        return cur.fetchall()


def channel_name_for(conn: sqlite3.Connection, device_id: Optional[str], ch_index: Any) -> Optional[str]:
    """Return the friendly channel name for (device_id, ch_index), or None."""
    if not device_id or ch_index is None:
        return None
    try:
        idx = int(ch_index)
    except Exception:
        return None
    conn.row_factory = sqlite3.Row
    with DB_LOCK:
        cur = conn.cursor()
        cur.execute("""
        SELECT name FROM channels
        WHERE device_id = ? AND ch_index = ?
        LIMIT 1
        """, (device_id, idx))
        row = cur.fetchone()
    if row and row["name"]:
        return str(row["name"])
    return None


def _channel_label(idx: int, name: str, role: str) -> str:
    """Render a short label like '0 · OH2AAV' or '0 · Primary' or 'ch 3'."""
    name = (name or "").strip()
    role_u = (role or "").strip().upper()
    if name:
        return f"{idx} · {name}"
    if role_u == "PRIMARY":
        return f"{idx} · Primary"
    return f"ch {idx}"


def build_channel_options(conn: sqlite3.Connection, device_id: Optional[str]) -> List[Dict[str, Any]]:
    """Return a list of channel dicts suitable for template dropdowns.
    Disabled slots are hidden. Falls back to a single primary entry if the
    device's channel list is not yet known."""
    opts: List[Dict[str, Any]] = []
    for r in list_channels_for_device(conn, device_id):
        role = (r["role"] or "").upper()
        if role == "DISABLED":
            continue
        idx = int(r["ch_index"])
        name = r["name"] or ""
        opts.append({
            "index": idx,
            "name": name,
            "role": role,
            "label": _channel_label(idx, name, role),
        })
    if not opts:
        opts = [{"index": 0, "name": "", "role": "PRIMARY", "label": "0 · Primary"}]
    return opts
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
        # ROUTING_APP packets carry ACK / MAX_RETRANSMIT / NAK for our sent DMs.
        # This pubsub path is more reliable across meshtastic-python versions than
        # the onResponse= kwarg to sendText, so we use it as the primary ACK route.
        try:
            pub.subscribe(self.on_routing, "meshtastic.receive.routing")
        except Exception as e:
            print(f"[GW] routing pubsub subscribe failed: {e}")

    def connect(self) -> TCPInterface:
        print(f"[GW] Connecting to Meshtastic TCP: {self.host}")
        iface = TCPInterface(hostname=self.host)
        print("[GW] Connected.")
        self.iface = iface
        self._resolve_my_id()
        self.seed_nodes_from_iface()
        self._last_node_refresh_ts = 0
        self.refresh_nodes_from_iface(force=True)
        self.refresh_channels_from_iface()
        return iface

    def _resolve_my_id(self) -> None:
        """Resolve and cache this gateway radio's own node id (e.g. '!db29583c')."""
        if not self.iface:
            return
        try:
            info = self.iface.getMyNodeInfo()
        except Exception:
            info = None
        my_id = None
        try:
            if isinstance(info, dict):
                my_id = (info.get("user") or {}).get("id") or info.get("id")
            if not my_id:
                num = getattr(self.iface, "myInfo", None)
                if num is not None:
                    num_val = getattr(num, "my_node_num", None)
                    if num_val is not None:
                        my_id = int_to_node_id(num_val)
        except Exception:
            pass
        if my_id:
            self.my_id = str(my_id)
            print(f"[GW] Local device id: {self.my_id}")

    def refresh_channels_from_iface(self) -> None:
        """Read channel list from the connected device and upsert into the channels table."""
        if not self.iface:
            return
        if not self.my_id:
            self._resolve_my_id()
        device_id = self.my_id
        if not device_id:
            return
        try:
            local = getattr(self.iface, "localNode", None)
            channels = getattr(local, "channels", None) if local else None
            if not channels:
                return
        except Exception:
            return

        for ch in channels:
            try:
                idx = int(getattr(ch, "index", 0))
                role_val = getattr(ch, "role", 0)
                if hasattr(role_val, "name"):
                    role_str = role_val.name
                else:
                    role_str = {0: "DISABLED", 1: "PRIMARY", 2: "SECONDARY"}.get(int(role_val), str(role_val))
                settings = getattr(ch, "settings", None)
                name = ""
                if settings is not None:
                    name = getattr(settings, "name", "") or ""
                upsert_channel(self.db, device_id, idx, name, role_str)
            except Exception as e:
                print(f"[GW] channel refresh error: {e}")

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
                            ch_idx_val = safe_get(packet, "channel", default=None)
                            ch_name = channel_name_for(self.db, self.my_id, ch_idx_val)
                            # Skip storing service-command DMs (e.g. "wx EFHK") in the Inbox.
                            if not is_service_command(text):
                                log_incoming_text(self.db, packet, channel_name=ch_name)

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

                                # TAF service: respond to direct messages "taf <ICAO>".
                                # The Services on/off setting (METAR_ENABLED_KEY) and the
                                # reply delay (METAR_DELAY_KEY) are shared with METAR; this
                                # is one combined "METAR & TAF" service.
                                mtaf = re.match(r"^\s*taf\s+([A-Za-z]{4})\s*$", str(text_in), flags=re.IGNORECASE)
                                if mtaf:
                                    enabled_t = get_setting(self.db, METAR_ENABLED_KEY, "1")
                                    taf_enabled = True if enabled_t in ("1", "true", "yes", "on") else False
                                    if taf_enabled:
                                        icao = mtaf.group(1).upper()
                                        to_id = packet.get("toId") or int_to_node_id(packet.get("to"))
                                        sender_id = packet.get("fromId") or int_to_node_id(packet.get("from"))
                                        if to_id and sender_id and to_id not in ("^all", "!ffffffff"):
                                            log_service_event(self.db, "taf", sender_id, to_id, icao, "req", "")
                                            taf = fetch_taf_text(icao)
                                            if taf:
                                                reply_t = compact_taf(taf, icao)
                                            else:
                                                reply_t = f"No TAF for {icao}"
                                            sent_ok_t = False
                                            if getattr(self, 'iface', None) is not None:
                                                try:
                                                    time.sleep(int(get_setting(self.db, METAR_DELAY_KEY, "3") or 3))
                                                    self.iface.sendText(reply_t, destinationId=sender_id)
                                                    sent_ok_t = True
                                                    log_service_event(self.db, "taf", sender_id, to_id, icao, "ok", reply_t)
                                                except Exception as e_send_t:
                                                    debug_add({'ts': iso_now(), 'fromId': sender_id, 'type': 'svc', 'note': f'taf sendText failed: {e_send_t}'})
                                                    log_service_event(self.db, "taf", sender_id, to_id, icao, "fail", f"sendText: {e_send_t}")
                                            if not sent_ok_t:
                                                try:
                                                    ch_t = packet.get('channel')
                                                    try:
                                                        ch_i_t = int(ch_t) if ch_t is not None else None
                                                    except Exception:
                                                        ch_i_t = None
                                                    queue_outbox(self.db, sender_id, ch_i_t, reply_t)
                                                    log_service_event(self.db, "taf", sender_id, to_id, icao, "queued", reply_t)
                                                except Exception as e_q_t:
                                                    debug_add({'ts': iso_now(), 'fromId': sender_id, 'type': 'svc', 'note': f'taf queue failed: {e_q_t}'})
                                                    log_service_event(self.db, "taf", sender_id, to_id, icao, "fail", f"queue: {e_q_t}")

                                # Solar / propagation service: respond to "cmd solar".
                                # Shares the METAR_ENABLED on/off and reply-delay settings.
                                msolar = re.match(r"^\s*cmd\s+solar\s*$", str(text_in), flags=re.IGNORECASE)
                                if msolar:
                                    enabled_s = get_setting(self.db, METAR_ENABLED_KEY, "1")
                                    solar_enabled = True if enabled_s in ("1", "true", "yes", "on") else False
                                    if solar_enabled:
                                        to_id = packet.get("toId") or int_to_node_id(packet.get("to"))
                                        sender_id = packet.get("fromId") or int_to_node_id(packet.get("from"))
                                        if to_id and sender_id and to_id not in ("^all", "!ffffffff"):
                                            log_service_event(self.db, "solar", sender_id, to_id, "solar", "req", "")
                                            sdata = fetch_solar_data()
                                            if sdata:
                                                reply_s = compact_solar(sdata)
                                            else:
                                                reply_s = "Solar data unavailable"
                                            sent_ok_s = False
                                            if getattr(self, 'iface', None) is not None:
                                                try:
                                                    time.sleep(int(get_setting(self.db, METAR_DELAY_KEY, "3") or 3))
                                                    self.iface.sendText(reply_s, destinationId=sender_id)
                                                    sent_ok_s = True
                                                    log_service_event(self.db, "solar", sender_id, to_id, "solar", "ok", reply_s)
                                                except Exception as e_send_s:
                                                    debug_add({'ts': iso_now(), 'fromId': sender_id, 'type': 'svc', 'note': f'solar sendText failed: {e_send_s}'})
                                                    log_service_event(self.db, "solar", sender_id, to_id, "solar", "fail", f"sendText: {e_send_s}")
                                            if not sent_ok_s:
                                                try:
                                                    ch_s = packet.get('channel')
                                                    try:
                                                        ch_i_s = int(ch_s) if ch_s is not None else None
                                                    except Exception:
                                                        ch_i_s = None
                                                    queue_outbox(self.db, sender_id, ch_i_s, reply_s)
                                                    log_service_event(self.db, "solar", sender_id, to_id, "solar", "queued", reply_s)
                                                except Exception as e_q_s:
                                                    debug_add({'ts': iso_now(), 'fromId': sender_id, 'type': 'svc', 'note': f'solar queue failed: {e_q_s}'})
                                                    log_service_event(self.db, "solar", sender_id, to_id, "solar", "fail", f"queue: {e_q_s}")
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
            # Hops travelled = hopStart - hopLimit (only when both present and consistent).
            hop_start_v = packet.get("hopStart")
            hop_limit_v = packet.get("hopLimit")
            hops_v = None
            try:
                if hop_start_v is not None and hop_limit_v is not None:
                    hs = int(hop_start_v)
                    hl = int(hop_limit_v)
                    if hs >= hl >= 0:
                        hops_v = hs - hl
            except Exception:
                hops_v = None
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
                last_hops=hops_v,
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
                ch_idx_val = safe_get(packet, "channel", default=None)
                ch_name = channel_name_for(self.db, self.my_id, ch_idx_val)
                txt = safe_get(packet, "decoded", "text", default="")
                # Skip storing service-command DMs (e.g. "wx EFHK") in the Inbox.
                if not is_service_command(txt):
                    log_incoming_text(self.db, packet, channel_name=ch_name)
                print(f"[GW] IN {packet.get('fromId')} -> {packet.get('toId')}: {txt}")
        except Exception as e:
            print("[GW] Failed to log incoming text:", e)

    def send_text(self, text: str, to_id: Optional[str] = None, channel: Optional[int] = None,
                  want_ack: bool = False, on_response=None):
        """Send a text packet. Returns the library's MeshPacket (has .id) if available.
        ACKs are primarily caught via the meshtastic.receive.routing pubsub subscription,
        so if the installed meshtastic-python version doesn't accept onResponse=, we
        silently fall back and still work."""
        if not self.iface:
            raise RuntimeError("Not connected to Meshtastic")
        base_kwargs: Dict[str, Any] = {"wantAck": bool(want_ack)}
        if to_id:
            # Direct message: channel index is irrelevant for PKI DMs.
            base_kwargs["destinationId"] = to_id
        else:
            # Broadcast: send on the selected channel slot (defaults to primary).
            try:
                base_kwargs["channelIndex"] = int(channel) if channel is not None else 0
            except Exception:
                base_kwargs["channelIndex"] = 0

        if on_response is not None:
            try:
                return self.iface.sendText(text, onResponse=on_response, **base_kwargs)
            except TypeError as e:
                # Older meshtastic-python: no onResponse kwarg. Fall back — the
                # pubsub routing handler will still catch ACKs.
                print(f"[GW] sendText onResponse unsupported, falling back: {e}")
        return self.iface.sendText(text, **base_kwargs)

    def _handle_ack_packet(self, packet, source: str) -> None:
        """Shared ACK/NAK handling for both onResponse callback and pubsub routing events."""
        try:
            if not isinstance(packet, dict):
                return
            decoded = packet.get("decoded") or {}
            request_id = decoded.get("requestId")
            if request_id is None:
                # Some library versions expose it at top level
                request_id = packet.get("requestId")
            if request_id is None:
                return
            routing = decoded.get("routing") or {}
            err = routing.get("errorReason", "NONE")
            # Normalise enum-or-string to string
            err_s = err.name if hasattr(err, "name") else str(err)
            err_s = (err_s or "NONE").upper()

            if err_s in ("NONE", "0"):
                new_status = "acked"
            elif err_s in ("MAX_RETRANSMIT", "TIMEOUT"):
                new_status = "no_ack"
            else:
                new_status = "nacked"

            try:
                rid = int(request_id)
            except Exception:
                return
            n = update_outbox_ack_by_packet(self.db, rid, new_status)
            print(f"[GW] ACK via {source} for packet_id={rid}: {new_status} ({err_s}) rows_updated={n}")
        except Exception as e:
            print(f"[GW] _handle_ack_packet error: {e}")

    def _outbox_on_response(self, packet):
        """Callback invoked by meshtastic-python when a ROUTING_APP response
        arrives for one of our sent packets (ACK / MAX_RETRANSMIT / NAK)."""
        self._handle_ack_packet(packet, source="onResponse")

    def on_routing(self, packet, interface):
        """Pubsub handler for ROUTING_APP packets. Fires for every routing packet
        the radio delivers to us — including ACKs for our own sent DMs. This is
        more reliable across library versions than the onResponse kwarg."""
        self._handle_ack_packet(packet, source="pubsub")

    def run(self):
        while not self.stop_event.is_set():
            try:
                if self.iface is None:
                    self.iface = self.connect()

                # even during quiet periods, refresh names/HW + channel map
                self.refresh_nodes_from_iface()
                try:
                    self.refresh_channels_from_iface()
                except Exception:
                    pass
                # flip long-pending ACKs to no_ack (handles restarts, dropped callbacks)
                try:
                    sweep_stale_pending_acks(self.db)
                except Exception:
                    pass

                row = fetch_one_outbox(self.db)
                if row:
                    tries = int(row["tries"]) + 1
                    to_id = row["to_id"]
                    text = row["text"]
                    row_channel = row["channel"] if "channel" in row.keys() else None
                    ch_name = channel_name_for(self.db, self.my_id, row_channel) if row_channel is not None else None
                    # ACKs are only meaningful for DMs; broadcasts stay fire-and-forget.
                    want_ack = bool(to_id)
                    initial_ack = "pending" if want_ack else "na"
                    print(f"[GW] OUTBOX id={row['id']} to={to_id} ch={row_channel} tries={tries} wantAck={want_ack}")
                    try:
                        pkt = self.send_text(
                            text=text, to_id=to_id, channel=row_channel,
                            want_ack=want_ack,
                            on_response=(self._outbox_on_response if want_ack else None),
                        )
                        # capture the firmware packet id so the ACK callback can match
                        pkt_id = None
                        try:
                            if isinstance(pkt, dict):
                                pkt_id = int(pkt.get("id", 0)) or None
                            elif pkt is not None:
                                pkt_id = int(getattr(pkt, "id", 0)) or None
                        except Exception:
                            pkt_id = None
                        now_ts = utc_ts()
                        mark_outbox(self.db, row["id"], "sent", tries, None,
                                    packet_id=pkt_id, ack_status=initial_ack, sent_ts=now_ts)
                        log_outgoing_message(self.db, to_id, row_channel, text, status="sent",
                                             channel_name=ch_name)
                        print(f"[GW] OUTBOX id={row['id']} SENT pkt_id={pkt_id} ack={initial_ack} want_ack={want_ack}")
                    except Exception as e:
                        err = str(e)
                        new_status = "failed" if tries >= OUTBOX_MAX_TRIES else "queued"
                        # Hand-off failure — ACK is moot; mark 'na' so UI doesn't show 'pending' forever.
                        mark_outbox(self.db, row["id"], new_status, tries, err,
                                    ack_status="na")
                        log_outgoing_message(self.db, to_id, row_channel, text, status="failed",
                                             err=err, channel_name=ch_name)
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


@app.get("/api/unread_count")
def api_unread_count():
    try:
        n = count_unread_dm(DB_CONN)
    except Exception:
        n = 0
    return {"unread": n}


@app.post("/msg/read/<int:msg_id>", endpoint="message_mark_read")
def msg_mark_read(msg_id: int):
    try:
        mark_message_read(DB_CONN, msg_id)
    except Exception:
        pass
    return redirect(url_for("inbox"))


@app.post("/inbox/mark_all_read", endpoint="inbox_mark_all_read")
def inbox_mark_all_read_post():
    try:
        mark_all_inbox_read(DB_CONN)
    except Exception:
        pass
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

    # Channel selector: default to primary (index 0) on first visit.
    try:
        selected_channel = int(request.args.get("channel", "0"))
    except Exception:
        selected_channel = 0

    channels = build_channel_options(DB_CONN, gateway.my_id)
    # If the current selection is not in the valid list, fall back to the first option.
    valid_idx = {c["index"] for c in channels}
    if selected_channel not in valid_idx:
        selected_channel = channels[0]["index"]
    selected_label = next((c["label"] for c in channels if c["index"] == selected_channel),
                          _channel_label(selected_channel, "", ""))

    DB_CONN.row_factory = sqlite3.Row
    with DB_LOCK:
        cur = DB_CONN.cursor()
        cur.execute("""
        SELECT m.id, m.ts, m.ts_iso, m.direction, m.from_id, m.to_id, m.text, m.rx_rssi, m.rx_snr,
               m.channel, m.channel_name,
               nf.long_name AS from_long, nf.short_name AS from_short
        FROM messages m
        LEFT JOIN nodes nf ON nf.node_id = m.from_id
        WHERE (
                (m.to_id IS NOT NULL AND lower(m.to_id) IN ('^all','!ffffffff','ffffffff','0xffffffff'))
                OR
                (m.to_id IS NULL AND (m.to_num IS NULL OR m.to_num = 4294967295))
              )
          AND COALESCE(m.channel, 0) = ?
        ORDER BY m.ts DESC, m.id DESC
        LIMIT ?
        """, (selected_channel, limit))
        msgs = cur.fetchall()
        msgs = list(reversed(msgs))

        cur.execute("""
        SELECT id, created_ts_iso, text, channel, channel_name
        FROM outbox
        WHERE status='queued'
          AND (to_id IS NULL OR lower(to_id) IN ('^all','!ffffffff','ffffffff','0xffffffff'))
          AND COALESCE(channel, 0) = ?
        ORDER BY created_ts ASC
        LIMIT 10
        """, (selected_channel,))
        queued = cur.fetchall()

    return render_template("broadcast.html", host=HOST, extra_head="",
                           msgs=msgs, queued=queued, limit=limit,
                           channels=channels,
                           selected_channel=selected_channel,
                           selected_label=selected_label)

@app.post("/broadcast/send")
def broadcast_send():
    text = (request.form.get("text", "") or "").strip()
    try:
        ch = int(request.form.get("channel", "0"))
    except Exception:
        ch = 0
    if not text:
        return redirect(url_for("broadcast", channel=ch))
    ch_name = channel_name_for(DB_CONN, gateway.my_id, ch)
    queue_outbox(DB_CONN, None, ch, text, channel_name=ch_name)
    return redirect(url_for("broadcast", channel=ch))

@app.get("/send")
def send_form():
    window = request.args.get("window", DEFAULT_ACTIVE_WINDOW)
    win_sec = parse_window_to_seconds(window)
    rows = list_active_nodes(DB_CONN, win_sec)
    to_id_prefill = (request.args.get("to_id", "") or "").strip()
    channels = build_channel_options(DB_CONN, gateway.my_id)
    return render_template("send.html", host=HOST, extra_head="",
                           rows=rows, window=window,
                           to_id_prefill=to_id_prefill,
                           channels=channels,
                           selected_channel=0)

@app.post("/send")
def send_post():
    to_id = (request.form.get("to_id", "") or "").strip()
    text = (request.form.get("text", "") or "").strip()
    try:
        ch = int(request.form.get("channel", "0"))
    except Exception:
        ch = 0
    if not text:
        return redirect(url_for("send_form"))
    if to_id:
        # Direct message: channel index isn't meaningful for the worker,
        # but log the current channel name for reference.
        queue_outbox(DB_CONN, to_id, None, text, channel_name=None)
    else:
        ch_name = channel_name_for(DB_CONN, gateway.my_id, ch)
        queue_outbox(DB_CONN, None, ch, text, channel_name=ch_name)
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

    # Gateway's own last known position (for distance calc).
    own_lat = own_lon = None
    if gateway.my_id:
        try:
            DB_CONN.row_factory = sqlite3.Row
            with DB_LOCK:
                cur = DB_CONN.cursor()
                cur.execute(
                    "SELECT last_lat, last_lon FROM nodes WHERE node_id = ? LIMIT 1",
                    (gateway.my_id,),
                )
                me = cur.fetchone()
            if me and me["last_lat"] is not None and me["last_lon"] is not None:
                own_lat = float(me["last_lat"])
                own_lon = float(me["last_lon"])
        except Exception:
            pass

    now_ts = utc_ts()
    RECENT_SEC = 15 * 60  # mark green if heard within last 15 minutes

    nodes = []
    for r in rows:
        last_seen_ts = r["last_seen_ts"]
        try:
            age_sec = now_ts - int(last_seen_ts) if last_seen_ts is not None else None
        except Exception:
            age_sec = None
        recent = (age_sec is not None and age_sec >= 0 and age_sec <= RECENT_SEC)

        try:
            n_lat = float(r["last_lat"])
            n_lon = float(r["last_lon"])
        except Exception:
            n_lat = n_lon = None

        dist_km = None
        if own_lat is not None and own_lon is not None and n_lat is not None and n_lon is not None:
            try:
                dist_km = round(haversine_km(own_lat, own_lon, n_lat, n_lon), 1)
            except Exception:
                dist_km = None

        nodes.append({
            "node_id": r["node_id"],
            "name": node_display(r["long_name"], r["short_name"], r["node_id"]),
            "hw": r["hw_model"] or "",
            "lat": n_lat,
            "lon": n_lon,
            "alt": r["last_alt"],
            "rssi": r["last_rssi"],
            "snr": r["last_snr"],
            "last_seen": r["last_seen_iso"] or "",
            "pos_time": r["last_pos_iso"] or "",
            "last_seen_ts": last_seen_ts,
            "age_sec": age_sec,
            "recent": recent,
            "distance_km": dist_km,
            "hops": r["last_hops"],
        })
    return jsonify({"nodes": nodes})


@app.get("/map/tracks")
def map_tracks():
    """Return polyline points for one or more node ids."""
    ids_raw = request.args.get("ids", "") or ""
    try:
        n = int(request.args.get("n", str(DEFAULT_MAP_TRACK_POINTS)))
    except Exception:
        n = DEFAULT_MAP_TRACK_POINTS
    n = max(1, min(n, 24 * 60))

    ids = [s.strip() for s in ids_raw.split(",") if s.strip()]
    tracks: Dict[str, List[Tuple[float, float]]] = {}
    for node_id in ids:
        try:
            pts = get_track_points(DB_CONN, node_id, n)
        except Exception:
            pts = []
        tracks[node_id] = pts
    return jsonify({"tracks": tracks})


@app.get("/services")
def services_view():
    metar_enabled = get_setting(DB_CONN, METAR_ENABLED_KEY, "1")
    metar_delay = get_setting(DB_CONN, METAR_DELAY_KEY, "3")
    metar_log = []
    try:
        DB_CONN.row_factory = sqlite3.Row
        with DB_LOCK:
            cur = DB_CONN.cursor()
            cur.execute("""
                SELECT ts_iso, service, from_id, query, status, note
                FROM service_log
                WHERE service IN ('metar', 'taf', 'solar')
                ORDER BY ts DESC LIMIT 50
            """)
            metar_log = cur.fetchall()
    except Exception:
        metar_log = []
    return render_template(
        "services.html", host=HOST, extra_head="",
        metar_enabled=(metar_enabled in ("1", "true", "yes", "on")),
        metar_delay=metar_delay,
        metar_log=metar_log,
    )


@app.post("/services", endpoint="services_post")
def services_save():
    enabled = request.form.get("metar_enabled", "1")
    enabled = "1" if str(enabled).strip() in ("1", "true", "yes", "on") else "0"
    try:
        delay = int(request.form.get("metar_delay", "3") or 3)
    except Exception:
        delay = 3
    delay = max(0, min(delay, 60))
    set_setting(DB_CONN, METAR_ENABLED_KEY, enabled)
    set_setting(DB_CONN, METAR_DELAY_KEY, str(delay))
    return redirect(url_for("services_view"))


@app.get("/debug")
def debug_view():
    debug_nodes = list_debug_nodes(DB_CONN, limit=200)
    return render_template(
        "debug.html", host=HOST, extra_head="",
        debug_nodes=debug_nodes,
        default_n=DEBUG_SHOW_DEFAULT,
        maxn=DEBUG_BUFFER_MAX,
    )


@app.get("/debug/data")
def debug_data():
    try:
        n = int(request.args.get("n", str(DEBUG_SHOW_DEFAULT)))
    except Exception:
        n = DEBUG_SHOW_DEFAULT
    typ = (request.args.get("typ", "all") or "all").strip().lower()
    node_id = (request.args.get("node", "") or request.args.get("node_id", "") or "").strip()
    n = max(1, min(n, DEBUG_BUFFER_MAX))
    entries = debug_filtered(n, typ, node_id)
    lines = []
    for e in entries:
        try:
            lines.append(f"[{iso_utc(e.ts)}] {e.typ:8s} from={e.from_id}\n{e.text}\n")
        except Exception:
            lines.append(str(e))
    return jsonify({"text": "\n".join(lines) if lines else "(no entries)"})


@app.post("/debug/clear")
def debug_clear_post():
    debug_clear()
    return redirect(url_for("debug_view"))


@app.get("/status")
def status_view():
    connected = gateway.iface is not None

    # local node summary from DB
    local = None
    try:
        my_id = gateway.my_id
        if my_id:
            DB_CONN.row_factory = sqlite3.Row
            with DB_LOCK:
                cur = DB_CONN.cursor()
                cur.execute("""
                    SELECT node_id, node_num, long_name, short_name, hw_model, macaddr,
                           last_seen_ts, last_seen_iso, last_rssi, last_snr,
                           last_lat, last_lon, last_alt, last_pos_ts, last_pos_iso
                    FROM nodes
                    WHERE node_id = ?
                    LIMIT 1
                """, (my_id,))
                local = cur.fetchone()
    except Exception:
        local = None

    # live node JSON from iface (best-effort)
    live_node_json = ""
    iface_json = ""
    if connected:
        try:
            info = gateway.iface.getMyNodeInfo()
            live_node_json = json.dumps(info, ensure_ascii=False, indent=2, default=str)
        except Exception as e:
            live_node_json = f"(unavailable: {e})"

        try:
            nodes = getattr(gateway.iface, "nodes", {}) or {}
            summary = {
                "host": HOST,
                "my_id": gateway.my_id,
                "node_count": len(nodes),
                "time": iso_now(),
            }
            iface_json = json.dumps(summary, ensure_ascii=False, indent=2, default=str)
        except Exception as e:
            iface_json = f"(unavailable: {e})"

    # Cleanup result (set when redirected here from /status/remove_unknown).
    cleanup = None
    try:
        if request.args.get("cleanup") == "1":
            cleanup = {
                "scanned": int(request.args.get("scanned", "0") or 0),
                "removed": int(request.args.get("removed", "0") or 0),
                "failed": int(request.args.get("failed", "0") or 0),
                "skipped": int(request.args.get("skipped", "0") or 0),
            }
    except Exception:
        cleanup = None

    # Manual-remove result (set when redirected here from /status/remove_node).
    manual_remove = None
    try:
        if request.args.get("manual_remove") == "1":
            manual_remove = {
                "node_id": request.args.get("node_id", "") or "",
                "ok": (request.args.get("ok") == "1"),
                "msg": request.args.get("msg", "") or "",
            }
    except Exception:
        manual_remove = None

    # Full node list for the picker dropdown.
    try:
        all_nodes = list_all_nodes_for_picker(DB_CONN)
    except Exception:
        all_nodes = []

    return render_template(
        "status.html", host=HOST, extra_head="",
        connected=connected, local=local,
        live_node_json=live_node_json,
        iface_json=iface_json,
        cleanup=cleanup,
        manual_remove=manual_remove,
        all_nodes=all_nodes,
        my_id=(gateway.my_id or ""),
        unknown_auto_age_hours=UNKNOWN_NODE_AUTO_CLEAN_AGE_SEC // 3600,
    )


@app.post("/status/remove_unknown", endpoint="status_remove_unknown")
def status_remove_unknown():
    """Manual cleanup: remove all nodes with no long/short name from the
    device's NodeDB and the gateway's DB. No age filter — the user clicked
    the button knowing what they're doing."""
    try:
        res = remove_unknown_nodes(min_age_seconds=None)
    except Exception as e:
        try:
            debug_add({"ts": iso_now(), "type": "admin",
                       "note": f"manual cleanup error: {e}"})
        except Exception:
            pass
        res = {"scanned": 0, "removed": 0, "failed": 0, "skipped": 0}
    qs = (
        f"cleanup=1"
        f"&scanned={int(res.get('scanned', 0))}"
        f"&removed={int(res.get('removed', 0))}"
        f"&failed={int(res.get('failed', 0))}"
        f"&skipped={int(res.get('skipped', 0))}"
    )
    return redirect(url_for("status_view") + "?" + qs)


@app.post("/status/remove_node", endpoint="status_remove_node")
def status_remove_node():
    """Remove a single, specific node chosen from the Status-page dropdown.
    Refuses to remove the gateway's own node."""
    node_id = (request.form.get("node_id", "") or "").strip()
    ok = False
    msg = ""
    try:
        if not node_id:
            msg = "no node selected"
        elif gateway.my_id and str(node_id) == str(gateway.my_id):
            msg = "refusing to remove this gateway's own node"
        else:
            # Look up node_num for the admin command.
            DB_CONN.row_factory = sqlite3.Row
            with DB_LOCK:
                cur = DB_CONN.cursor()
                cur.execute("SELECT node_num FROM nodes WHERE node_id = ? LIMIT 1",
                            (node_id,))
                row = cur.fetchone()
            node_num = int(row["node_num"]) if row and row["node_num"] is not None else None
            ok_dev, err = remove_node_from_device(node_num, node_id=node_id)
            # Always remove from gateway DB so the UI clears, even if the
            # device-side admin call failed (e.g. node already gone there).
            try:
                delete_node_from_db(DB_CONN, node_id)
            except Exception as e:
                msg = f"db delete failed: {e}"
            if ok_dev:
                ok = True
                if not msg:
                    msg = "removed"
            else:
                msg = msg or f"device removal failed: {err or 'unknown'}"
            try:
                debug_add({"ts": iso_now(), "type": "admin",
                           "note": f"manual remove {node_id}: dev_ok={ok_dev} msg='{msg}'"})
            except Exception:
                pass
    except Exception as e:
        msg = f"error: {e}"
    qs = (
        "manual_remove=1"
        f"&node_id={node_id}"
        f"&ok={'1' if ok else '0'}"
        f"&msg={msg.replace(' ', '+')}"
    )
    return redirect(url_for("status_view") + "?" + qs)


# -------------------------
# Entry point
# -------------------------
def start_gateway_thread() -> threading.Thread:
    t = threading.Thread(target=gateway.run, name="meshtastic-gateway", daemon=True)
    t.start()
    return t


if __name__ == "__main__":
    gw_thread = start_gateway_thread()
    cleanup_thread = start_unknown_cleanup_thread()
    try:
        app.run(host=WEB_HOST, port=WEB_PORT, debug=False, use_reloader=False, threaded=True)
    finally:
        gateway.stop()
