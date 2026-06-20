# revenant-ws

Plain **Node.js `ws`** WebSocket server — a drop-in port of the original Revenant
PartyKit (Cloudflare Durable Objects) co-op game server. The game client uses a
**RAW browser WebSocket** and keeps working byte-for-byte: same URL shape, same
JSON message types, same `{t:"state"}` snapshot, same enemy roster and AI.

- **One global in-memory room.** The room id in the path is ignored — everyone
  shares `"global"`.
- **Forwarder-agnostic.** Upgrades are accepted on **any** path. The client
  connects to `/parties/main/global?wallet=...&name=...`, but any path works.
- **Server-authoritative enemies.** The same fixed roster of **19** enemies
  (5 skulls, 5 skeletons, 5 zombies, 4 dragons) with the same per-tick AI,
  hit/death and 25s respawn logic as the original.
- **No build step.** CommonJS, single file, one dependency (`ws`).

## Wire protocol (unchanged)

Endpoint: `wss://<host>/parties/main/<roomId>?wallet=<pubkey>&name=<displayName>`
(path/room is ignored; everyone shares the global room).

Client -> server:

```json
{ "t": "input", "v": 1, "x": 0, "y": 2, "z": 0, "yaw": 0, "weapon": 0 }
{ "t": "hit",   "v": 1, "id": "Zombie1", "dmg": 25 }
```

Server -> client:

```json
{ "t": "welcome", "v": 1, "id": "<wallet-or-generated-id>" }
{ "t": "state",   "v": 1,
  "players": [ { "id": "...", "x": 0, "y": 2, "z": 0, "yaw": 0, "name": "anon", "weapon": 0 } ],
  "enemies": [ { "id": "Zombie1", "type": "zombie", "x": -74.19, "y": 0, "z": -40.28, "yaw": 0, "hp": 150, "maxHp": 150, "alive": true } ] }
{ "t": "join",    "v": 1, "id": "...", "name": "..." }
{ "t": "leave",   "v": 1, "id": "..." }
{ "t": "full",    "v": 1 }
```

- State is broadcast at **~15 Hz** (`Math.round(1000/15)` = 67 ms interval).
- The state snapshot always includes **all 19 enemies**, dead ones included, so
  the client can disable/re-enable meshes on death/respawn.
- Enemy `x,y,z,yaw` are rounded to 2 decimals; player poses are sent as-is
  (matching the original server).
- Player cap is **8**. A 9th connection receives `{t:"full",v:1}` and is closed.

## Run locally

Requires **Node >= 18**.

```bash
npm install
PORT=2000 node server.js
# -> [revenant-ws] listening on 0.0.0.0:2000 (state every 67ms, cap 8, 19 enemies)
```

If `PORT` is unset it defaults to `1999` (the PartyKit dev port), so the client
pointed at `127.0.0.1:1999` works with zero config:

```bash
node server.js
```

Point the client's host at `127.0.0.1:2000` (or whatever `PORT` you used). The
client picks `ws://` for `localhost`/`127.*` hosts and `wss://` otherwise, so no
client change is needed.

There is also a plain HTTP `GET /` health endpoint that returns
`revenant-ws ok` — handy for uptime pingers and Render health checks.

## Deploy on Render (git-push)

This repo ships a [Render Blueprint](https://render.com/docs/blueprint-spec)
(`render.yaml`) describing one free **web** service.

1. Push this directory to a Git repo (GitHub/GitLab/Bitbucket):

   ```bash
   git init
   git add .
   git commit -m "revenant-ws: plain Node ws co-op server"
   git branch -M main
   git remote add origin <your-repo-url>
   git push -u origin main
   ```

2. In the [Render dashboard](https://dashboard.render.com) click
   **New +** -> **Blueprint**, connect the repo, and Render reads `render.yaml`:
   - type `web`, env `node`, plan `free`
   - build: `npm install`
   - start: `npm start`
   - `autoDeploy: true` — every push to the connected branch redeploys.

   (Or skip the Blueprint and create a **Web Service** manually with the same
   build/start commands — Render auto-detects Node from `package.json`.)

3. Render assigns a public URL like `https://revenant-ws.onrender.com`. Render
   sets `PORT` automatically; the server binds `0.0.0.0:$PORT`. WebSocket
   upgrades (`wss://`) work over the same port — no extra config.

4. Point the client host at `revenant-ws.onrender.com` (no scheme, no path — the
   client builds `wss://<host>/parties/main/<room>?wallet=...&name=...`).

### Free-tier note

Render's free web services **sleep after ~15 min idle** and cold-start on the
next request (a few seconds). The first connection after a sleep relies on the
client's built-in auto-reconnect (it retries with backoff) — this is the
recommended free-tier behavior.

⚠️ An external uptime pinger hitting `GET /` to force always-on is **not free**
in practice: continuous pings burn the free instance-hours allowance (which is
monthly-capped), and once exhausted the service is suspended for the rest of the
cycle. True always-on requires a paid plan. For dev/launch, accept the cold
start and let the client reconnect.

## Deviations from the original

The wire protocol is identical. Two internal-only differences (invisible to the
client) versus `src/server.ts`:

- **One process-wide room** instead of a Durable Object per room id. The path's
  room id is ignored; everyone shares the single global room. Enemies are seeded
  once at process start instead of per-DO construction.
- **`id` fallback** when no `?wallet=` is supplied is a generated `anon-...`
  string (PartyKit used the connection's `conn.id`). The original's
  `wallet`/`name` slicing (64 / 32 chars) and `"anon"` name default are kept.
