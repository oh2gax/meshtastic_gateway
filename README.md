# meshtastic_gateway

Web-based Meshtastic messaging, mapping & debugging application.

Main_Screen_1.png
# Meshtastic WiFi Gateway + Web UI (Flask + SQLite)

This project runs on a Raspberry Pi (or any Linux host) and connects to a Meshtastic node over WiFi (TCP) using the Meshtastic Python library. It stores messages and node information in SQLite, and provides a lightweight Flask web UI for viewing and sending messages, monitoring active nodes, viewing positions on a map, and inspecting raw packets.

It also includes a small set of opt-in **Services** that can reply automatically to direct messages — for example, replying with the latest METAR weather report when a node sends a private message like `wx EFHK`.


## What it does

### Gateway (WiFi/TCP)

Connects to a Meshtastic device via TCP (WiFi) and subscribes to incoming packets, processing:

- Direct messages
- Broadcast messages on any configured channel (multi-channel)
- Node / user updates (long & short names, hardware model, etc.)
- Positions (for the map and per-node track lines)
- Routing / ACK packets (used to confirm delivery of outgoing direct messages)
- Traffic debug stream
- Message services (e.g. METAR)

### SQLite database

Stores:

- Inbox / Outbox (direct messages, with unread state)
- Broadcast chat (separate from Inbox/Outbox, per channel)
- Nodes (latest known info, RSSI/SNR, last position)
- Positions (history, used for map markers and tracklines)
- Service log (e.g. recent METAR queries, status, replies)

The database uses WAL mode, so you will see three files:

```
meshtastic_messages.db
meshtastic_messages.db-wal
meshtastic_messages.db-shm
```

The `-wal` and `-shm` files are normal and required while the database is in WAL mode.

The gateway performs lightweight schema migrations automatically on startup, so upgrading between versions of the script generally does not require touching the database.


## Web UI pages

- **Inbox** — received direct messages, with unread highlighting and per-message *Mark read* / *Delete* buttons (see *New message notifications* below).
- **Outbox** — sent direct messages and send queue status, including ACK status per outgoing message.
- **Active Nodes** — list of heard nodes, selectable time windows.
- **Broadcast** — chat-style view of broadcast traffic. A channel selector at the top lets you view any of the channels configured on your Meshtastic device, and you can broadcast a message to the currently selected channel from the same page. Broadcast traffic is kept separate from Inbox/Outbox.
- **Map** — last known positions of all heard nodes. Filled circles show node location (green = recent, blue = older). Optional per-node track lines, configurable last *N* points. The map automatically uses light or dark tiles depending on the theme setting.
- **Debug** — terminal-style view of raw JSON packets, with pause/copy/filter controls.
- **Status** — best-effort live node info from the interface plus the gateway's own connection state.
- **Services** — enable/disable services (currently METAR), configure reply delay, and see the latest service-traffic log.
- **Send** — per-node chat / send-message view (direct messages), with ACK feedback.

There is also a global **Theme** toggle in the navbar (light / dark) and a **New message** indicator that shows in the top right when there are unread direct messages.

See `Meshtastic_Gateway.pdf` for a UI overview and screenshots.


## Requirements

- Raspberry Pi OS (or Debian / Ubuntu)
- Python 3.9+ recommended (3.11 has been tested)
- A Meshtastic device with WiFi enabled and reachable on your LAN


## Meshtastic device setup (WiFi)

On the Meshtastic device:

- Enable WiFi and connect it to your LAN (DHCP is fine).
- Confirm you can reach it from the Pi (`ping <device-ip>`).
- The Meshtastic Python library connects using the device's TCP "API" interface over WiFi.


## Installation (Raspberry Pi 4)

### 1) System packages

```
sudo apt update
sudo apt install -y python3 python3-venv python3-pip
```

### 2) Clone the repo

```
git clone https://github.com/oh2gax/meshtastic_gateway.git
cd meshtastic_gateway
```

### 3) Create virtual environment + install dependencies

```
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install meshtastic flask pypubsub
```

If you prefer, create a `requirements.txt`:

```
meshtastic
flask
pypubsub
```

and install with:

```
pip install -r requirements.txt
```

### 4) Configure the gateway

Edit the main script (e.g. `gateway_web_new_stable_v11.py`) and update the configuration section near the top.

Meshtastic device IP address:

```
HOST = "192.168.1.100"
```

SQLite database file path:

```
DB_PATH = "/home/pi/meshtastic_gateway/meshtastic_messages.db"
```

Web UI bind address + port:

```
WEB_HOST = "0.0.0.0"
WEB_PORT = 8000
```

Notes:

- `HOST` must be the IP of your Meshtastic device on WiFi (DHCP or static).
- `WEB_HOST = "0.0.0.0"` makes it accessible from other machines on your LAN. Use `"127.0.0.1"` for local-only access.

### 5) Run it

```
source venv/bin/activate
python3 gateway_web_new_stable_v11.py
```

To run in the background:

```
nohup python3 gateway_web_new_stable_v11.py > /dev/null 2>&1 &
ps -ef
```

To kill the background process:

```
ps -ef
kill <process number>
```

Open in a browser:

```
http://<raspberry-pi-ip>:8000
```


## Common operations

### Inbox — received direct messages

All received direct messages with date/time, RSSI/SNR, and per-message actions:

- **Unread highlighting:** newly arrived direct messages are highlighted (light yellow in light mode, dark amber in dark mode), with a small red dot before the timestamp.
- **Mark read:** each unread row has a *Mark read* button that clears the highlight and removes that message from the unread count.
- **Mark all read:** a button at the top of the page marks every unread direct message as read in one click.
- **Delete:** removes the message from the database.

Broadcast messages and incoming service commands (see *Services*) are not stored in the Inbox.

### Outbox — sent direct messages and queue status

All sent direct messages, their destination, send status, and an **ACK** column showing whether the message was acknowledged by the recipient. Possible ACK display values include:

- `Y` — acknowledged
- A small number (e.g. `3`) — number of retransmission attempts so far
- `…` — pending ACK
- `—` — no ACK information available (e.g. for older entries)

Each entry has a *Delete* button to remove it from the queue / log.

### Active Nodes

All heard nodes with timestamp, RSSI/SNR and position information. Filtered by a selectable time window.

### Broadcast messages

Chat-style view of broadcast traffic. Use the channel selector at the top of the page to switch between any channels configured on your Meshtastic device. You can broadcast a message to the currently selected channel from the same page. Broadcast traffic is kept separate from Inbox/Outbox.

### Map

- Shows last known positions for nodes that have published positions.
- Each node is rendered as a filled circle:
  - **Green** — node was heard recently (within ~15 minutes).
  - **Blue** — node was heard earlier than that.
- Clicking a circle (or a node entry in the side list) shows details: name, hardware, last-seen time, RSSI/SNR, distance, and coordinates.
- The side list shows each node's RSSI/SNR and the **distance from the gateway** (great-circle, in km).
- Track lines can be enabled per node. Track length is configurable via the *Track points* setting on the page (default 60).
- The map switches between standard OSM tiles and a dark CARTO basemap automatically when you toggle the global Theme button.

### Debug terminal

- Shows raw incoming JSON packet data.
- Supports pause/resume, copy-to-clipboard, and basic filtering.

### Status

Basic status of the gateway and the Meshtastic device it's connected to (connection state, local node summary, best-effort live node info).

### Services

The Services page lets you enable or disable services that are integrated into the gateway and tweak their settings. Currently implemented:

- **METAR weather report.** Send a private direct message to the gateway with text like `wx EFHK` (Helsinki-Vantaa) and the latest METAR for that airport is sent back to your node. Any ICAO airport code worldwide can be queried by changing the four-letter code.
- **Reply delay.** A configurable delay (seconds) before the METAR reply is sent, useful for nodes that are slow to switch RX/TX state.
- **Service log.** The Services page also shows the most recent METAR requests with status (request / ok / queued / fail), so you can quickly verify that the service is working.

Service-command messages (e.g. `wx EFHK`) are **not** stored in the Inbox or counted toward the unread count — they are treated as commands rather than chat.

### Send a direct message

Use the web UI's *Send* page (or per-node *Chat* page from the active node list) to send direct messages. The Outbox shows ACK status as the message progresses.

### Theme (dark / light)

Use the **Theme** button in the navbar to switch between light and dark UI modes. The choice is remembered across sessions and is also applied to the map tiles.

### New message notifications

Whenever there are unread direct messages, a **New message** indicator appears in the top-right of the navbar. The indicator is a link to the Inbox; clicking it takes you straight there. The page polls the unread count every few seconds, so the indicator updates without needing a manual refresh. Broadcasts and service-command messages do not trigger the indicator.

To clear the indicator, mark the relevant messages as read on the Inbox page (per message or all at once).


## Running as a service (optional)

Create a systemd unit to start the gateway on boot.

Example: `/etc/systemd/system/meshtastic-gateway.service`

```
[Unit]
Description=Meshtastic WiFi Gateway Web UI
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=pi
WorkingDirectory=/home/pi/<your-repo>
ExecStart=/home/pi/<your-repo>/venv/bin/python3 /home/pi/<your-repo>/gateway_web_new_stable_v11.py
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
```

Enable + start:

```
sudo systemctl daemon-reload
sudo systemctl enable meshtastic-gateway
sudo systemctl start meshtastic-gateway
sudo systemctl status meshtastic-gateway
```


## Database files

```
meshtastic_messages.db
meshtastic_messages.db-wal
meshtastic_messages.db-shm
```

That's expected. SQLite WAL mode uses these for performance and safe concurrency while the app is running. Do not delete them while the gateway is running.


## Security notes

The web UI has no authentication by default. If exposed beyond your LAN, put it behind a reverse proxy with authentication, or bind it to `127.0.0.1` and access it through an SSH tunnel.
