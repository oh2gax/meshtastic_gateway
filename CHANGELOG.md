# Changelog

All notable changes to Meshtastic Gateway are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
User-facing notes are in `doc/whats_new.md` (shown in the What's new window).

---

## [2026-11-16]

### Added
- **Traceroute** — `traceroutes` table (created on start-up, old databases keep working); requests are
  queued and sent from the gateway loop with `sendData(RouteDiscovery, portNum=TRACEROUTE_APP,
  wantResponse=True, onResponse=…, hopLimit=1..7)` (the library's own `sendTraceRoute` blocks and prints,
  so it is not used). Answers are matched by `requestId` via `onResponse`, with `on_any_packet` as a
  backup; relay ACKs are ignored, routing errors (`NO_RESPONSE`, …) end the request as failed, no answer
  within 30 s + 15 s × hop limit as timeout. `parse_route_discovery()` reads `decoded.traceroute` or the raw
  payload; `0xFFFFFFFF` = unknown relay, SNR `-128` = unknown, SNR in quarter-dB.
- One traceroute at a time, at least `TRACEROUTE_MIN_INTERVAL_SEC` (30 s) between sends.
- `/traceroute` (start form, result chain, history), `POST /traceroute/start`, `/traceroute/<id>.json`;
  Active Nodes one-click button + hop-limit dropdown (last used limit stored as setting
  `traceroute_hop_limit`); links from Chat, Map popup and Status.
- Map overlay `/map?trace=<id>`: towards (solid orange) and back (dashed purple), labelled relays, legend
  with the chains and nodes without a position.
- **`cmd trace`** service: traceroute back to the sender (hop limit = hops of the request + 1, at least 3);
  the reply is queued to the service worker when the answer arrives or the request times out; "busy"
  reply when one is already running. Ready-made replies go through `_queue_service_reply()`.
- **Per-service switches** — registry `SERVICES` (name, label, icon, command, example, description, source,
  help) renders the Services page; switch per service in setting `svc_<name>_enabled` (missing = on, so
  updating changes nothing); a service answers when the master switch (`metar_enabled`, kept) and its own
  switch are on. `POST /services/toggle` (`name` = service or `_master`); `POST /services` now saves only
  the fields it gets (delay).
- A command for a switched-off service is not answered and is logged with status `off`.
- **`cmd help`** — always answers with the commands of the services that are on (`help_text()`), or
  "Gateway services are off at the moment."; logged as service `help`.
- Services page: one card per service (switch, command, description, source, requests today, last
  request), shared settings card, shared log table (latest 100, node names, status badges, `?log=<name>`
  filter).
- **ACK hops** — outbox columns `ack_hops`, `ack_rssi`, `ack_snr` (added on start-up); `_handle_ack_packet`
  stores `hopStart - hopLimit`, `rxRssi` and `rxSnr` of the routing packet when it is an end-to-end ACK
  (`acked`). Outbox and Chat show `Y n hops` with the signal in the tooltip.
- `update_outbox_ack_by_packet` never downgrades `acked` (a late implicit ACK from a relay used to turn
  Y back into S).
- Broadcast page shows the hops of received broadcasts (`hop_start - hop_limit`, as in Inbox and Chat).
- **Broadcast "All channels"** — `/broadcast?channel=all` (the default) lists every channel; each message
  gets a channel tag (current channel map label, falling back to the stored `channel_name`, colour by
  index). Sending from the All view uses a channel picker; `back` keeps the view after sending.
- **Active Nodes distance** — `distance_km` from the gateway's own last position (haversine, as on the
  map); new Distance column. Client-side sorting on Last seen, Name, RSSI, SNR, Hops and Distance
  (`th.sortable`, `data-*` values on the rows, empty values last, state in `localStorage['mg-nodes-sort']`).
- **Map list sorting** — Sort selector (last seen / distance / name / hops) and direction button,
  applied on every 5-second refresh, state in `localStorage['mg-map-sort']`.
- User guide 1.6 with Traceroute, the new Services page, the ACK hops and broadcast hops; demo data and screenshots.

## [2026-11-15]

### Added
- **`cmd wspr` service** — reads WSPR Logger's public JSON API (`/api/latest`, `/api/typical_day?days=7`;
  base URL `WSPR_API_BASE`, overridable in `local_config.py`, window `WSPR_TYPICAL_DAYS`). Reply: level
  on the WSPR Logger scale (`wspr_condition`, `& DX` with `dx6 >= 2`), `prop.live_n` reporters in 60 min,
  and ABOVE (> p75) / BELOW (< p25) / SAME against the typical-day profile at the current UTC hour, with
  WSPR Logger's small-number rule (difference < max(2, sqrt(median)) = SAME). No spot within 60 min →
  "No propagation" / "No propagation or TX Fail" (`tx_suspect` or `busy_now`); `wspr_down` →
  "wspr.live unreachable"; `warming` and `tx_suspect` are passed through. `build_wspr_reply()` is pure
  (testable without network).
- **What's new window** — `doc/whats_new.md` (WSPR Logger format), `/api/whatsnew`, ✨ button with the
  version badge (`v.DDMMYYYY` of the newest heading) and an unseen dot (`localStorage['mg-seen-version']`);
  What's new / Guides / About tabs.
- **Guides** — `/api/guides` and `/doc/<name>` (plain `*.pdf` names in `doc/` only), titles and
  descriptions from `doc/guides.json`; same logic as WSPR Logger's `core/guides.py`.
- **User guide** — `doc/Meshtastic_Gateway_User_Guide.pdf` (15 pages), LaTeX source
  `doc/src/user_guide.tex` in the WSPR Logger guide style. Screenshots in `doc/src/guide_img/` are made by
  `doc/src/guide_shots.py` from an invented demo database (never the real one) with a Natural Earth
  coastline instead of map tiles.
- **`update.sh`** — fetch, stop service, back up DB + `api_keys.py` + `local_config.py` to
  `~/meshtastic_backups/` (keep 5), `git reset --hard origin/main`, restore DB, pip install when
  `requirements.txt` changed (or `--deps`), `ast` check with automatic rollback, start, show log.
  Refuses to overwrite local edits to tracked files unless `--force`; sets `core.fileMode false`.
- **`meshtastic-gateway.service`** (systemd, user `oh2gax`), `requirements.txt`,
  `local_config.py.example` (HOST / WEB_HOST / WEB_PORT / DB_PATH / WSPR_API_BASE overrides).

### Changed
- **UI refresh** — Bootstrap Icons, sticky navbar with active page, cards, table headers, `_macros.html`
  (`ack_badge`, `seg`, `refresh_select`); refresh dropdowns handled once in `base.html` (same
  `localStorage` keys); auto-refresh skips while a text field has content; byte counters on message boxes.
- **Service commands** run on a `service-worker` thread fed by `Gateway.svc_queue` instead of inside the
  pubsub receive callback (HTTP fetch + reply delay no longer block packet reception). Commands are a
  table (`SERVICE_COMMANDS`) + `SERVICE_REPLY_BUILDERS`; `SERVICE_COMMAND_PATTERNS` is derived.
- `on_text` and the text branch of `on_any_packet` share `_handle_text_packet()`.
- Map track length is minutes from **local** midnight (was UTC midnight).
- `POSITION_KEEP_PER_NODE` 5000 → 2000 (the value `insert_position` actually allowed).
- Database files are no longer tracked in git (`.gitignore`).
- Unread-DM badge is compact (`✉ N new`); navbar collapses below 1200 px and hides its icons below
  1600 px, so it never overflows.

### Fixed
- **Reply could be sent as a broadcast** — `/send?to_id=` for a node outside the active window left the
  dropdown on BROADCAST; the target is now always added and selected.
- **Chat header / label empty** — template used `title_name`, route passes `title`.
- **Services delay reset to 3 s** — input was hard-coded `value="3"`; now shows the saved value (max 60).
- **Inbox / Outbox 200 and 1000 links** fell back to 50 (route accepted only 50/100/500).
- **Map XSS** — node names (set by anyone on the mesh) were inserted unescaped into the map list,
  popups and tooltips.
- Status page "remove node" redirect now URL-encodes its message.
