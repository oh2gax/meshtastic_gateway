# meshtastic_gateway
Web based Meshtastic messaging, mapping &amp; debugging application


# Meshtastic WiFi Gateway + Web UI (Flask + SQLite)

This project runs on a Raspberry Pi (or any Linux host) and connects to a Meshtastic node over WiFi (TCP) using the Meshtastic Python library. It stores messages and node information in SQLite, and provides a lightweight Flask web UI for viewing and sending messages, monitoring active nodes, viewing positions on a map, and inspecting raw packets.

What it does
## Gateway (WiFi/TCP)

Connects to a Meshtastic device via TCP (WiFi).

Subscribes to incoming packets and processes:

* Direct messages
* Broadcast messages
* Node/user updates (long/short names, hardware model, etc.)
* Positions (for map + tracks)
* Telemetry (basic support)

## SQLite database

### Stores:

* Inbox / Outbox (direct messages)
* Broadcast chat (separate from Inbox/Outbox)
* Nodes (latest known info)
* Positions (for map markers + optional tracklines)
* Telemetry time-series (temperature/lux/voltage/battery when present)

Uses WAL mode, so you will see:
```
meshtastic_messages.db
meshtastic_messages.db-wal
meshtastic_messages.db-shm
```
These -wal and -shm files are normal and required while the database is in WAL mode.

### Web UI pages

* Inbox: received direct messages
* Outbox: sent direct messages and send queue status
* Broadcast: chat-style view of broadcast channel messages (separate from Inbox/Outbox)
* Active Nodes: list of heard nodes (selectable time windows)
* Map: markers + optional per-node track lines (configurable last N points)
* Chat: per-node chat view (direct messages)
* Telemetry: view and graph telemetry per node and timeframe
* Debug: “terminal” view of raw JSON packets with pause/copy/filtering
* Status: shows latest “best-effort” live node stats from the interface (no history)

## Requirements

Raspberry Pi OS (or Debian/Ubuntu)

Python 3.9+ recommended (you’re using 3.11, which is fine)

A Meshtastic device with WiFi enabled and reachable on your LAN

## Installation (Raspberry Pi 4)
#### 1) System packages
`sudo apt update`
`sudo apt install -y python3 python3-venv python3-pip`

#### 2) Clone the repo
`git clone https://github.com/<your-user>/<your-repo>.git
cd <your-repo>
`
#### 3) Create virtual environment + install dependencies
`python3 -m venv venv`
`source venv/bin/activate`
`pip install --upgrade pip`
`pip install meshtastic flask pypubsub`


If you prefer, create a requirements.txt:

meshtastic
flask
pypubsub


and install with:

`pip install -r requirements.txt`

## 4) Configure the gateway

Edit gateway_web.py and update the configuration section near the top:

#### Meshtastic device IP address:
`HOST = "192.168.1.211"`

#### SQLite database file path:
`DB_PATH = "/home/pi/meshtastic/meshtastic_messages.db"`

#### Web UI bind address + port:
`WEB_HOST = "0.0.0.0"`
`WEB_PORT = 8000`


Notes

HOST must be the IP of your Meshtastic device on WiFi (DHCP or static).

WEB_HOST = "0.0.0.0" makes it accessible from other machines on your LAN.

Use "127.0.0.1" if you want local-only access.

## 5) Run it
`source venv/bin/activate`
`python gateway_web.py`


##### Open in a browser:

`http://<raspberry-pi-ip>:8000`

Meshtastic device setup (WiFi)

On the Meshtastic device:

Enable WiFi and connect it to your LAN (DHCP is fine).

Confirm you can reach it from the Pi (ping its IP).

The Meshtastic Python library connects using its TCP interface (Meshtastic “API” over WiFi).

## Common operations

##### Send a direct message

Use the web UI Outbox/Compose (or per-node Chat page) and select a node from the active list.

##### Broadcast messages

Broadcast messages appear in the Broadcast page (chat style) and do not appear in Inbox/Outbox.

##### Map

* Shows last known positions for nodes that have positions.
* Track lines can be enabled per node from the map UI.
* Track length defaults to DEFAULT_MAP_TRACK_POINTS.

##### Debug terminal

* Shows raw incoming JSON packet data.
* Supports pause/resume + copy-to-clipboard + basic filtering.

##### Telemetry

Stores and graphs basic telemetry if present:

* temperature (°C)
* lux
* voltage
* battery level

## Running as a service (optional)

Create a systemd unit to start on boot.

Example: /etc/systemd/system/meshtastic-gateway.service

```
[Unit]
Description=Meshtastic WiFi Gateway Web UI
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=pi
WorkingDirectory=/home/pi/<your-repo>
ExecStart=/home/pi/<your-repo>/venv/bin/python /home/pi/<your-repo>/gateway_web.py
Restart=always
RestartSec=3
```
[Install]
WantedBy=multi-user.target
```
Enable + start:

sudo systemctl daemon-reload
sudo systemctl enable meshtastic-gateway
sudo systemctl start meshtastic-gateway
sudo systemctl status meshtastic-gateway
```
Database files (.db / .db-wal / .db-shm)

#### If you see:

meshtastic_messages.db-wal

meshtastic_messages.db-shm

That’s expected. SQLite WAL mode uses these for performance and safe concurrency while the app is running. Do not delete them while the gateway is running.

#### Security notes

The web UI has no authentication by default.

If exposed beyond your LAN, put it behind a reverse proxy + authentication (or bind to 127.0.0.1 and use SSH tunnel).
