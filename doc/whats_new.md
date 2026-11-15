# What's new in Meshtastic Gateway

Short notes for users of the web page, newest first. Each version is one day of development
(v.DDMMYYYY). The full technical history is in CHANGELOG.md. The newest heading gives the
version shown in the navigation bar.

## v.15112026 — A fresh look, WSPR propagation on the mesh, and easy updates on the Pi

- **A fresh, modern look** — all pages have a new top bar with icons, cards, coloured ACK badges (Y / S / … / —) and chat bubbles in Chat and Broadcast. Everything is where it was, and light / dark mode works as before. On a phone the menu folds into ☰.
- **`cmd wspr`** — send `cmd wspr` as a private message to the gateway and it replies with the current WSPR propagation from the OH2GAX WSPR Logger: the level (*Very low* … *Exceptional*, *& DX* when two or more stations beyond 6000 km hear the car), how many stations heard it in the last 60 minutes, and whether that is *ABOVE*, *SAME* or *BELOW* the typical day of the last 7 days at this UTC hour. When nobody has heard the transmitter for an hour the reply says *No propagation*, or *No propagation or TX Fail* at an hour when the band is usually open.
- **What's new, Guides and About** — this window. The ✨ button in the top bar has a red dot until you have read the newest notes; the version is also in the footer. The *Guides* tab has the documents of the gateway as PDF files to open or download.
- **User guide** — a new *Meshtastic Gateway – User guide* (PDF, under *Guides*) explains every page: Meshtastic in a nutshell, the delivery marks Y / S / … / —, the map, the services with `cmd wspr`, maintenance, running on a Raspberry Pi and a quick reference.
- **Reply never turns into a broadcast** — *Reply* on an older Inbox message now always pre-selects that node, even if it hasn't been heard recently. Before, the Send page could fall back to *BROADCAST*.
- **Chat shows the node name again** — the Chat header and the message box show who you are talking to.
- **Services remember the reply delay** — the delay you save is shown on the page and no longer resets to 3 seconds when you change the on/off setting.
- **Faster radio while services reply** — METAR, TAF, Solar and WSPR replies are prepared in the background, so incoming messages and ACKs are no longer held up while a reply is fetched.
- **Inbox and Outbox 200 / 1000** — the 200 and 1000 links now show that many rows.
- **Map tracks in minutes** — the map's *Track min* box sets how many minutes of track to draw, counted from local midnight.
- **Message size counter** — message boxes show how many of the 240 bytes you have used, and auto-refresh waits while you are typing.
- **Compact unread badge** — unread direct messages show as a small red *✉ 2 new* badge, so the top bar always fits on one line.
- **Runs as a service, updates with one command** — on the Raspberry Pi the gateway runs as a systemd service and starts on boot; `./update.sh` fetches the newest version from GitHub, backs up the database and restarts.
