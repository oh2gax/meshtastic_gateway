# What's new in Meshtastic Gateway

Short notes for users of the web page, newest first. Each version is one day of development
(v.DDMMYYYY). The full technical history is in CHANGELOG.md. The newest heading gives the
version shown in the navigation bar.

## v.16112026 — Traceroute: see the path to any node, on the page and on the map

- **Traceroute from Active Nodes** — the green route button on each row starts a traceroute with one click; the small ▾ next to it lets you choose the hop limit (1–7) for that run. The button remembers the hop limit you used last.
- **Traceroute page** — shows the path in both directions as a chain of nodes, with the SNR on every hop in colour (green good, amber fair, red weak), a countdown while the answer is awaited, and a history of earlier traceroutes. *Repeat* runs the same one again. It can also be opened from Chat, from a node's popup on the Map and from the Status page.
- **Route on the map** — *Show on map* draws the way there in orange and the way back as a dashed purple line, with the relays labelled; nodes without a known position are listed in the corner.
- **`cmd trace`** — send `cmd trace` as a private message to the gateway and it runs a traceroute back to you and replies with the route in both directions, for example `Trace 1 hop: GAX0>HILL(6.5)>GAX4(2.5) | back: GAX4>HILL(3)>GAX0(5.5)`.
- **Gentle on the mesh** — only one traceroute runs at a time and at least 30 seconds apart, as in the Meshtastic apps. Every traceroute is relayed by the nodes on the way, so it is a tool for when you need it.
- **A switch for each service** — the Services page now has a card for each service (METAR, TAF, Solar, WSPR, Traceroute) with its own on/off switch, the command, where the data comes from and how many times it was asked today. *All services* still switches everything off at once, and the reply delay is shared.
- **Switched-off services stay quiet** — a command for a service that is off gets no reply (no airtime used), but the log shows it as *off*, so you can see that someone asked.
- **`cmd help`** — send `cmd help` to the gateway and it replies with the commands of the services that are on, for example `Services: wx ICAO, taf ICAO, cmd solar, cmd wspr, cmd trace.`
- **One log for all services** — the latest 100 requests in a table with the asking node's name, coloured results and a filter per service.
- **User guide 1.2** — new Traceroute section, the new Services page, and `cmd trace` and `cmd help` in the quick reference.

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
