#!/usr/bin/env python3
"""
guide_shots.py -- screenshots for the Meshtastic Gateway user guide.

Builds a small DEMO database (invented nodes, messages and tracks -- never the
station's real database), starts the gateway's web UI on it without a radio,
and photographs every page with a headless browser into doc/src/guide_img/.

Run from the repository root:

    pip install playwright && playwright install chromium
    python3 doc/src/guide_shots.py

Options:
    --chrome PATH   use this Chrome/Chromium binary instead of Playwright's
    --assets DIR    serve cdn.jsdelivr.net files from DIR/<pkg>-<ver>/package/
                    (for machines without internet access)

The map has no tiles in the screenshots: a plain outline of the coast
(doc/src/guide_map_outline.geojson, Natural Earth, public domain) is drawn
instead, as in the WSPR Logger guide.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
import threading
import time
import types
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
IMG = os.path.join(HERE, "guide_img")
DEMO_DB = os.path.join(HERE, "demo_guide.db")
PORT = 8799

NOW = int(time.time())


def ago(minutes: float) -> int:
    return int(NOW - minutes * 60)


# ---------------------------------------------------------------------------
# Demo data (all invented)
# ---------------------------------------------------------------------------
GW = "!a1b2c3d0"
NODES = [
    # id, long, short, hw, lat, lon, rssi, snr, hops, minutes ago, num
    (GW,          "OH2GAX Gateway",  "GAX0", "HELTEC_V3",        60.2050, 24.6560, None,  None,  0,    0.5),
    ("!a1b2c3d2", "OH2GAX Pocket",   "GAX2", "HELTEC_V4",        60.1985, 24.6710, -38,   13.5,  0,    2),
    ("!a1b2c3d4", "OH2GAX Car",      "GAX4", "TRACKER_T1000_E",  60.2005, 24.6905, -71,   6.25,  0,    3),
    ("!5e10a001", "Harbour Relay",   "HRB1", "RAK4631",          60.1610, 24.9540, -97,  -4.25,  1,    6),
    ("!5e10a002", "Hill Top Router", "HILL", "STATION_G2",       60.2930, 24.5790, -84,   2.5,   0,    9),
    ("!5e10a003", "Forest Cabin",    "FRST", "HELTEC_V3",        60.3580, 24.4120, -112, -9.5,   2,   12),
    ("!5e10a004", "Lake Base",       "LAKE", "HELTEC_V4",        60.2610, 24.8590, -90,  -1.0,   1,   35),
    ("!5e10a005", "City Roof",       "CITY", "SEEED_XIAO_S3",    60.1705, 24.9395, -101, -6.75,  2,   48),
    ("!5e10a006", "Island Solar",    "ISLE", "RAK4631",          60.1120, 24.7310, -105, -7.5,   3,   70),
    ("!5e10a007", "Bike Tracker",    "BIKE", "TRACKER_T1000_E",  60.2240, 24.7980, -88,  -2.25,  3,   95),
    ("!5e10a008", "Kiosk Node",      "KIOS", "HELTEC_V3",        None,    None,    -109, -8.0,   4,   15),
    ("!5e10a009", None,              None,   None,               None,    None,    -118, -12.5,  None, 50),
]

# (minutes ago, from, text, read)
DMS_IN = [
    (190, "!a1b2c3d2", "Testing the gateway from the pocket node", 1),
    (140, "!5e10a001", "Hi from the harbour! I hear you via HILL.", 1),
    (95,  "!5e10a002", "Good evening, battery 87 %, all fine up here.", 1),
    (25,  "!a1b2c3d4", "On the way home, ETA 20 min", 0),
    (8,   "!5e10a003", "Signal report please?", 0),
]
# (minutes ago, to, text, ack_status)
DMS_OUT = [
    (135, "!5e10a001", "Thanks! You are -97 dBm, SNR -4 here. 73", "acked"),
    (92,  "!5e10a002", "Great, 73!", "acked"),
    (300, "!5e10a006", "Is the solar node still on?", "no_ack"),
    (60,  "!5e10a005", "Testing through the relay", "implicit_ack"),
    (24,  "!a1b2c3d4", "Drive safely", "acked"),
    (1,   "!5e10a003", "Report: -112 dBm, SNR -9.5, 2 hops", "pending"),
]
# (minutes ago, from or None for me, text, channel)
BROADCASTS = [
    (180, "!5e10a005", "Anyone hearing me from downtown?", 0),
    (175, "!5e10a004", "Loud and clear from the lake, 2 hops", 0),
    (120, None,        "Good evening mesh, gateway OH2GAX online", 0),
    (80,  "!5e10a002", "Weather up here: clear sky, -3 °C", 0),
    (40,  "!5e10a007", "Test from the bike, moving north", 0),
    (15,  "!5e10a006", "Solar node check-in, battery 64 %", 0),
    (55,  "!a1b2c3d2", "Club channel test", 1),
]
SERVICE_LOG = [
    (70, "metar", "!5e10a001", "EFHK", "req", ""),
    (70, "metar", "!5e10a001", "EFHK", "ok", "151850Z 21008KT 9999 FEW020 M03/M06 Q1021 NOSIG"),
    (50, "taf", "!a1b2c3d2", "EFHK", "req", ""),
    (50, "taf", "!a1b2c3d2", "EFHK", "ok", "151720Z 1518/1618 20008KT 9999 FEW020 TEMPO 1600/1608 4000 -SN BKN012"),
    (30, "solar", "!5e10a002", "SOLAR", "req", ""),
    (30, "solar", "!5e10a002", "SOLAR", "ok", "SFI 143 SN 160 X B9.7 A 4 K 1 AU 4 BZE -4.0 SW 377.2 Pf 14 Ef 2360"),
    (12, "help", "!5e10a007", "HELP", "req", ""),
    (12, "help", "!5e10a007", "HELP", "ok", "Services: wx ICAO, cmd solar, cmd wspr, cmd trace. Send as a private message."),
    (8, "taf", "!5e10a007", "ESSA", "off", ""),
    (5, "wspr", "!a1b2c3d4", "WSPR", "req", ""),
    (5, "wspr", "!a1b2c3d4", "WSPR", "ok",
     "WSPR 20m 10:15Z: Very good & DX. 60 rptr/60m (3 >6000km), 7d typ 99 (90-139) BELOW. MUF 21.3 Loc KP20jg"),
]


def car_track():
    """A drive from the city centre to home over the last hour (lat, lon, minutes ago)."""
    pts = []
    a = (60.1700, 24.9400)
    b = (60.2005, 24.6905)
    n = 30
    for i in range(n + 1):
        f = i / n
        lat = a[0] + (b[0] - a[0]) * f + 0.010 * (1 - abs(2 * f - 1)) * (1 if i % 7 else 0.6)
        lon = a[1] + (b[1] - a[1]) * f
        pts.append((round(lat, 5), round(lon, 5), 63 - 2 * i))
    return pts


def build_demo(g) -> None:
    """Fill the demo database through the gateway's own helpers."""
    conn = g.DB_CONN
    for nid, ln, sn, hw, lat, lon, rssi, snr, hops, mins in NODES:
        g.upsert_node(conn, node_id=nid, node_num=int(nid[1:], 16), long_name=ln, short_name=sn,
                      hw_model=hw, last_seen_ts=ago(mins), last_rssi=rssi, last_snr=snr,
                      lat=lat, lon=lon, pos_ts=ago(mins) if lat else None, last_hops=hops)
        if lat:
            g.insert_position(conn, nid, ago(mins), lat, lon, None, 2000)
    for lat, lon, mins in car_track():
        g.insert_position(conn, "!a1b2c3d4", ago(mins), lat, lon, None, 2000)

    g.upsert_channel(conn, GW, 0, "", "PRIMARY")
    g.upsert_channel(conn, GW, 1, "OH2GAX", "SECONDARY")
    g.upsert_channel(conn, GW, 2, "", "DISABLED")

    def ins_msg(ts, direction, frm, to, text, ch=None, hops=None, rssi=None, snr=None, read=1):
        with g.DB_LOCK:
            conn.execute(
                "INSERT INTO messages(ts, ts_iso, direction, from_id, to_id, from_num, to_num, channel,"
                " text, rx_rssi, rx_snr, hop_limit, hop_start, is_read, raw_json)"
                " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (ts, g.iso_utc(ts), direction, frm, to, None, None, ch, text, rssi, snr,
                 (3 - hops) if hops is not None else None, 3 if hops is not None else None, read, "{}"))
            conn.commit()

    node = {n[0]: n for n in NODES}
    for mins, frm, text, read in DMS_IN:
        n = node[frm]
        ins_msg(ago(mins), "in", frm, GW, text, hops=n[8], rssi=n[6], snr=n[7], read=read)
    for mins, to, text, ack in DMS_OUT:
        ts = ago(mins)
        with g.DB_LOCK:
            conn.execute(
                "INSERT INTO outbox(created_ts, created_ts_iso, status, to_id, channel, text, tries,"
                " packet_id, ack_status, sent_ts, sent_ts_iso) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (ts, g.iso_utc(ts), "sent", to, None, text, 1, 1000 + mins, ack, ts, g.iso_utc(ts)))
            conn.commit()
        ins_msg(ts, "out", None, to, text)
        if ack == "acked":           # hops and signal of the returning ACK
            n = node[to]
            with g.DB_LOCK:
                conn.execute("UPDATE outbox SET ack_hops=?, ack_rssi=?, ack_snr=? WHERE packet_id=?",
                             (n[8], n[6], n[7], 1000 + mins))
                conn.commit()
    for mins, frm, text, ch in BROADCASTS:
        ts = ago(mins)
        if frm is None:
            ins_msg(ts, "out", None, None, text, ch=ch)
            with g.DB_LOCK:
                conn.execute("INSERT INTO outbox(created_ts, created_ts_iso, status, to_id, channel, text,"
                             " tries, ack_status) VALUES(?,?,?,?,?,?,?,?)",
                             (ts, g.iso_utc(ts), "sent", None, ch, text, 1, "na"))
                conn.commit()
        else:
            n = node[frm]
            ins_msg(ts, "in", frm, "^all", text, ch=ch, hops=n[8], rssi=n[6], snr=n[7])
    for mins, svc, frm, q, st, note in SERVICE_LOG:
        g.log_service_event(conn, svc, frm, GW, q, st, note)
        with g.DB_LOCK:   # put the log rows at the right time
            conn.execute("UPDATE service_log SET ts=?, ts_iso=? WHERE id=(SELECT MAX(id) FROM service_log)",
                         (ago(mins), g.iso_utc(ago(mins))))
            conn.commit()
    # Traceroutes: (minutes ago, target, hop limit, source, status, route, snr_towards, route_back, snr_back, error)
    U = 0xFFFFFFFF
    num = lambda nid: int(nid[1:], 16)
    TRACES = [
        (120, "!5e10a004", 3, "web", "done", [], [38], [], [34], None),
        (90,  "!5e10a006", 3, "web", "timeout", None, None, None, None, "no answer"),
        (45,  "!a1b2c3d4", 3, "cmd", "done", [num("!5e10a002")], [30, 10], [num("!5e10a002")], [12, 22], None),
        (20,  "!5e10a001", 4, "web", "done", [num("!5e10a002")], [26, -17], [num("!5e10a005"), U], [-28, -128, -38], None),
    ]
    for mins, tgt, hl, src, st, rt, sto, rb, sb, err in TRACES:
        tid = g.tr_insert(conn, tgt, hl, source=src, requested_by=tgt if src == "cmd" else None)
        ts = ago(mins)
        fields = {"status": st, "created_ts": ts, "created_ts_iso": g.iso_utc(ts), "sent_ts": ts,
                  "done_ts": ts + 9, "done_ts_iso": g.iso_utc(ts + 9), "error": err}
        if st == "done":
            rd = g.parse_route_discovery({"decoded": {"traceroute": {
                "route": rt, "snrTowards": sto, "routeBack": rb, "snrBack": sb}}})
            fields.update(route_json=json.dumps(rd["route"]), snr_towards_json=json.dumps(rd["snr_towards"]),
                          route_back_json=json.dumps(rd["route_back"]), snr_back_json=json.dumps(rd["snr_back"]))
        g.tr_update(conn, tid, **fields)
    g.set_setting(conn, g.TRACEROUTE_HOPS_KEY, "4")
    g.set_setting(conn, "svc_taf_enabled", "0")          # one service switched off
    g.set_setting(conn, g.METAR_ENABLED_KEY, "1")
    g.set_setting(conn, g.METAR_DELAY_KEY, "3")
    g.set_setting(conn, g.BROADCAST_LAST_VIEWED_KEY, str(ago(30)))   # one new broadcast -> red Broadcast

    # A few packets for the Debug page
    g.debug_add({"from": 0x5e10a001, "fromId": "!5e10a001", "toId": "^all", "rxTime": ago(1), "rxSnr": -4.25,
                 "rxRssi": -97, "hopLimit": 2, "hopStart": 3,
                 "decoded": {"portnum": "POSITION_APP", "position": {"latitude": 60.161, "longitude": 24.954,
                                                                     "altitude": 12, "time": ago(1)}}})
    g.debug_add({"from": 0x5e10a003, "fromId": "!5e10a003", "toId": GW, "rxTime": ago(0.5), "rxSnr": -9.5,
                 "rxRssi": -112, "hopLimit": 1, "hopStart": 3, "wantAck": True,
                 "decoded": {"portnum": "TEXT_MESSAGE_APP", "text": "Signal report please?"}})


class FakeIface:
    """Just enough of a TCPInterface for the Status page."""
    def __init__(self):
        self.nodes = {n[0]: {"num": int(n[0][1:], 16)} for n in NODES}

    def getMyNodeInfo(self):
        return {"num": int(GW[1:], 16),
                "user": {"id": GW, "longName": "OH2GAX Gateway", "shortName": "GAX0",
                         "hwModel": "HELTEC_V3", "role": "CLIENT"},
                "position": {"latitude": 60.205, "longitude": 24.656, "altitude": 18},
                "deviceMetrics": {"batteryLevel": 101, "voltage": 4.2, "channelUtilization": 7.4,
                                  "airUtilTx": 0.9, "uptimeSeconds": 412345},
                "lastHeard": NOW}

    def sendText(self, *a, **k):
        return None


# ---------------------------------------------------------------------------
# Server + browser
# ---------------------------------------------------------------------------
def start_app():
    for f in (DEMO_DB, DEMO_DB + "-wal", DEMO_DB + "-shm"):
        if os.path.exists(f):
            os.remove(f)
    # Point the gateway at the demo database (never the real one) before importing it.
    sys.modules["local_config"] = types.SimpleNamespace(DB_PATH=DEMO_DB, WEB_PORT=PORT)
    sys.path.insert(0, ROOT)
    import gateway_web_stable as g          # noqa: E402
    assert os.path.abspath(g.DB_PATH) == DEMO_DB
    build_demo(g)
    g.gateway.my_id = GW
    g.gateway.iface = FakeIface()
    th = threading.Thread(target=lambda: g.app.run(host="127.0.0.1", port=PORT, use_reloader=False,
                                                   threaded=True), daemon=True)
    th.start()
    time.sleep(1.5)
    return g


async def shoot(chrome: str | None, assets: str | None) -> None:
    from playwright.async_api import async_playwright
    os.makedirs(IMG, exist_ok=True)
    outline = open(os.path.join(HERE, "guide_map_outline.geojson"), encoding="utf-8").read()
    base = f"http://127.0.0.1:{PORT}"

    def local(url):
        if not assets:
            return None
        m = re.match(r"https://cdn\.jsdelivr\.net/npm/([^@]+)@([^/]+)/([^?#]*)", url)
        if not m:
            return None
        p = os.path.join(assets, f"{m.group(1)}-{m.group(2)}", "package", m.group(3))
        return p if os.path.exists(p) else None

    async with async_playwright() as p:
        kw = {"executable_path": chrome, "args": ["--no-sandbox"]} if chrome else {}
        browser = await p.chromium.launch(**kw)

        async def page(width=1440, height=900, theme="light", scale=1.5):
            ctx = await browser.new_context(viewport={"width": width, "height": height},
                                            device_scale_factor=scale)
            await ctx.add_init_script(f"try{{localStorage.setItem('mg-theme','{theme}')}}catch(e){{}}")
            # show the lists sorted by distance in the figures
            await ctx.add_init_script("try{if(!localStorage.getItem('mg-nodes-sort'))localStorage.setItem('mg-nodes-sort',"
                                      "JSON.stringify({key:'dist',dir:'asc'}));if(!localStorage.getItem('mg-map-sort'))"
                                      "localStorage.setItem('mg-map-sort',JSON.stringify({key:'dist',dir:'asc'}))}catch(e){}")

            async def route(r):
                url = r.request.url
                f = local(url)
                if f:
                    ct = ("font/woff2" if f.endswith(".woff2") else "text/css" if f.endswith(".css")
                          else "application/javascript" if f.endswith(".js") else None)
                    await r.fulfill(path=f, content_type=ct)
                elif url.startswith(base) or not assets:
                    if "tile.openstreetmap.org" in url or "basemaps.cartocdn.com" in url:
                        await r.abort()          # plain outline instead of map tiles
                    else:
                        await r.continue_()
                else:
                    await r.abort()
            await ctx.route("**/*", route)
            pg = await ctx.new_page()
            errs = []
            pg.on("pageerror", lambda e: errs.append(str(e)))
            return ctx, pg, errs

        async def snap(pg, name, full=True, clip=None):
            path = os.path.join(IMG, name + ".jpg")
            await pg.screenshot(path=path, full_page=full and clip is None, clip=clip, type="jpeg", quality=88)
            print("  ", name)

        # ---- light theme, desktop ----
        # A low window so that full-page shots end where the content ends.
        ctx, pg, errs = await page(height=480)
        shots = [
            ("nodes", "/nodes?window=1d"), ("inbox", "/inbox"), ("outbox", "/outbox"),
            ("chat", "/chat/!5e10a001"), ("broadcast", "/broadcast"),
            ("send", "/send?to_id=!5e10a003"), ("services", "/services"),
            ("status", "/status"), ("debug", "/debug"), ("traceroute", "/traceroute?id=4"),
        ]
        for name, url in shots:
            await pg.goto(base + url)
            await pg.wait_for_timeout(900)
            if name == "send":
                await pg.fill("textarea[name=text]", "Report: -112 dBm, SNR -9.5, 2 hops. 73")
            if name == "debug":
                await pg.wait_for_timeout(1500)
            await snap(pg, name)
        # navbar close-up
        await pg.goto(base + "/nodes")
        await pg.wait_for_timeout(800)
        await snap(pg, "navbar", clip={"x": 0, "y": 0, "width": 1440, "height": 62})

        # map with the coast outline, the car's track and a popup
        await pg.set_viewport_size({"width": 1440, "height": 900})
        await pg.goto(base + "/map?window=1d")
        await pg.wait_for_timeout(1500)
        await pg.evaluate("""(gj) => {
            const el = document.getElementById('map'); el.style.background = '#dcebf7';
            L.geoJSON(JSON.parse(gj), {style: {color: '#9aa5b1', weight: 1, fillColor: '#f6f3ec', fillOpacity: 1}})
             .addTo(map).bringToBack();
        }""", outline)
        await pg.check('input.trackBox[data-node="!a1b2c3d4"]')
        await pg.wait_for_timeout(1200)
        await pg.evaluate("""() => {
            const b = L.latLngBounds([[60.10, 24.40], [60.37, 24.97]]); map.fitBounds(b, {padding: [10, 10]});
        }""")
        await pg.wait_for_timeout(800)
        await pg.evaluate("""() => { const m = markers.get('!5e10a002'); if (m) m.openPopup(); }""")
        await pg.wait_for_timeout(600)
        await snap(pg, "map", full=False)

        # route of traceroute 4 on the map
        await pg.goto(base + "/map?window=1d&trace=4")
        await pg.wait_for_timeout(1500)
        await pg.evaluate("""(gj) => {
            const el = document.getElementById('map'); el.style.background = '#dcebf7';
            L.geoJSON(JSON.parse(gj), {style: {color: '#9aa5b1', weight: 1, fillColor: '#f6f3ec', fillOpacity: 1}})
             .addTo(map).bringToBack();
        }""", outline)
        await pg.wait_for_timeout(800)
        await snap(pg, "map_trace", full=False)

        # Active Nodes: the traceroute button with its hop-limit menu open
        await pg.set_viewport_size({"width": 1440, "height": 700})
        await pg.goto(base + "/nodes?window=1d")
        await pg.wait_for_timeout(800)
        await pg.click('form:has(input[value="!5e10a001"]) .dropdown-toggle')
        await pg.wait_for_timeout(500)
        box = await pg.eval_on_selector('form:has(input[value="!5e10a001"])', "e => { const r = e.getBoundingClientRect(); return {x: r.x, y: r.y}; }")
        await snap(pg, "nodes_trace_menu", clip={"x": 300, "y": max(0, box["y"] - 70), "width": 1140, "height": 380})
        await pg.set_viewport_size({"width": 1440, "height": 900})

        # What's new / Guides / About window
        await pg.goto(base + "/nodes")
        await pg.wait_for_timeout(700)
        await snap(pg, "whatsnew_button", clip={"x": 1040, "y": 0, "width": 400, "height": 62})
        await pg.click("#whatsNewBtn")
        await pg.wait_for_timeout(1000)
        await snap(pg, "whatsnew", full=False)
        await pg.click("#guidesTab")
        await pg.wait_for_timeout(900)
        await snap(pg, "guides", full=False)
        if errs:
            print("JS errors:", errs)
        await ctx.close()

        # ---- dark theme ----
        ctx, pg, errs = await page(theme="dark")
        await pg.goto(base + "/chat/!5e10a001")
        await pg.wait_for_timeout(900)
        await snap(pg, "dark_chat")
        await ctx.close()

        # ---- phone ----
        ctx, pg, errs = await page(width=390, height=760, scale=2)
        await pg.goto(base + "/outbox")
        await pg.wait_for_timeout(900)
        await snap(pg, "phone_outbox", full=False)
        await pg.click(".navbar-toggler")
        await pg.wait_for_timeout(700)
        await snap(pg, "phone_menu", full=False)
        await ctx.close()
        await browser.close()


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--chrome")
    ap.add_argument("--assets")
    a = ap.parse_args()
    start_app()
    print("Screenshots ->", IMG)
    asyncio.run(shoot(a.chrome, a.assets))
    for f in (DEMO_DB, DEMO_DB + "-wal", DEMO_DB + "-shm"):
        try:
            os.remove(f)
        except OSError:
            pass


if __name__ == "__main__":
    main()
