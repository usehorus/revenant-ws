/**
 * server.js — Plain Node.js `ws` port of the Revenant PartyKit co-op server.
 *
 * Replicates the PartyKit Durable Object server (src/server.ts) byte-for-byte on
 * the wire so the unchanged RAW-WebSocket client (theasis/src/net.js) keeps
 * working. ONE global in-memory room; the path is ignored (forwarder-agnostic).
 *
 * Wire protocol (must match client net.js + server types.ts):
 *   Client -> server:
 *     { t:"input", v:1, x, y, z, yaw, weapon }
 *     { t:"hit",   v:1, id, dmg }
 *   Server -> client:
 *     { t:"welcome", v:1, id }
 *     { t:"state",   v:1, players:[{id,x,y,z,yaw,name,weapon}],
 *                         enemies:[{id,type,x,y,z,yaw,hp,maxHp,alive}] }
 *     { t:"join",    v:1, id, name }
 *     { t:"leave",   v:1, id }
 *     { t:"full",    v:1 }
 *
 * Run locally:  PORT=2000 node server.js
 */

"use strict";

const http = require("http");
const { WebSocketServer } = require("ws");

// -----------------------------------------------------------------------------
// Protocol constants (mirrors src/types.ts)
// -----------------------------------------------------------------------------

/** Protocol version. Every message carries `v`. */
const PROTOCOL_VERSION = 1;

/** Hard cap on players per room. A 9th connection gets {t:"full"} and is closed. */
const ROOM_CAP = 8;

/** Server -> client full-snapshot broadcast rate (Hz) -> interval in ms. */
const STATE_HZ = 15;
const STATE_INTERVAL_MS = Math.round(1000 / STATE_HZ); // ~67ms (Math.round(66.66))

// -----------------------------------------------------------------------------
// Enemy roster + AI tuning (mirrors src/server.ts)
// -----------------------------------------------------------------------------

/**
 * Per-type AI tuning. skull never moves; y handling differs per type.
 * `aggro` = max horizontal distance at which an enemy will chase a player
 * (mirrors the original single-player game's move() distance checks). Outside
 * aggro the enemy drifts back to its spawn anchor instead of clustering.
 */
const ENEMY_TUNING = {
  skull: { stopDist: 4, speed: 0, flies: false, moves: false, aggro: 0 },
  zombie: { stopDist: 4, speed: 0.12, flies: false, moves: true, aggro: 40 },
  skeleton: { stopDist: 4, speed: 0.24, flies: false, moves: true, aggro: 25 },
  dragon: { stopDist: 4, speed: 0.4, flies: true, moves: true, aggro: 40 },
};

/** Seconds (ms) a dead enemy stays down before respawning at its anchor. */
const RESPAWN_DELAY_MS = 25000;

/** Max damage a single "hit" message may apply. */
const MAX_HIT_DMG = 1000;

/** Highest valid weapon index a client may select. */
const MAX_WEAPON_INDEX = 7;

/**
 * HARD positional separation (de-overlap) tuning. The old velocity-based push
 * lost to the chase: every tick chase pulled all enemies back to the same stop
 * point and the weak push couldn't overcome it, so ~5 enemies clustered on a
 * player piled to ~0.5 units apart (overlapping). Instead we now run an
 * iterative positional solver AFTER chase/return each tick that DIRECTLY pushes
 * overlapping pairs apart on x/z. Because it runs after movement and mutates
 * positions (not velocities), the chase cannot undo it within the same tick, so
 * a cluster resolves into a clean ring ~MIN_SPACING apart within a couple ticks.
 *
 * MIN_SPACING       — desired minimum horizontal distance between any two alive
 *                     enemies. Pairs closer than this get pushed apart.
 * SEPARATION_PASSES — relaxation passes per tick (a simple iterative solver);
 *                     more passes converge a tight pile faster.
 */
const MIN_SPACING = 3.0;
const SEPARATION_PASSES = 2;

/**
 * The FIXED roster, seeded once. ids/types/coords are matched by the client to
 * its local meshes — DO NOT rename or reorder-sensitive fields.
 */
const ENEMY_SEED = [
  { id: "skull", type: "skull", hp: 50, x: -40.09, y: 2.83, z: -109.26 },
  { id: "skull2", type: "skull", hp: 50, x: 0.49, y: 2.04, z: -153.48 },
  { id: "skull3", type: "skull", hp: 50, x: 43.75, y: 2.82, z: -101.03 },
  { id: "skull4", type: "skull", hp: 50, x: -18.28, y: 2.33, z: -67.09 },
  { id: "skull5", type: "skull", hp: 50, x: 19.83, y: 2.9, z: -67.74 },
  { id: "Skeleton1", type: "skeleton", hp: 200, x: 25.27, y: 0, z: 83.89 },
  { id: "Skeleton2", type: "skeleton", hp: 200, x: 23.58, y: 0, z: 62.85 },
  { id: "Skeleton3", type: "skeleton", hp: 200, x: -16.74, y: 0, z: 95.53 },
  { id: "Skeleton4", type: "skeleton", hp: 200, x: -27.18, y: 0, z: 70.93 },
  { id: "Skeleton5", type: "skeleton", hp: 200, x: 8.08, y: 0, z: 102.26 },
  { id: "Zombie1", type: "zombie", hp: 150, x: -74.19, y: 0, z: -40.28 },
  { id: "Zombie2", type: "zombie", hp: 150, x: -78.46, y: 0, z: 16.32 },
  { id: "Zombie3", type: "zombie", hp: 150, x: -135.86, y: 0, z: -42.22 },
  { id: "Zombie4", type: "zombie", hp: 150, x: -136.14, y: 0, z: 7.15 },
  { id: "Zombie5", type: "zombie", hp: 150, x: -99.78, y: 0, z: -6.65 },
  { id: "DragonArmature", type: "dragon", hp: 50, x: 124.78, y: 0, z: 13.1 },
  { id: "DragonArmature2", type: "dragon", hp: 50, x: 83.23, y: 0, z: -37.8 },
  { id: "DragonArmature3", type: "dragon", hp: 50, x: 132.83, y: 0, z: -34.82 },
  { id: "DragonArmature4", type: "dragon", hp: 50, x: 91.25, y: 0, z: 5.77 },
];

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

/** True if `n` is a real, finite number (rejects NaN/Infinity/non-numbers). */
function isFiniteNumber(n) {
  return typeof n === "number" && Number.isFinite(n);
}

/** Clamp `n` into [lo, hi]. */
function clamp(n, lo, hi) {
  return n < lo ? lo : n > hi ? hi : n;
}

/** Round to 2 decimals to keep the state payload small. */
function round2(n) {
  return Math.round(n * 100) / 100;
}

/** Cheap unique id fallback when no wallet is supplied. */
let _idCounter = 0;
function genId() {
  _idCounter += 1;
  return "anon-" + Date.now().toString(36) + "-" + _idCounter.toString(36);
}

// -----------------------------------------------------------------------------
// ONE global in-memory room
// -----------------------------------------------------------------------------

/** connId -> Player { id, x, y, z, yaw, name, weapon } */
const players = new Map();

/** ws -> connId, so we can locate a player on message/close. */
const connOf = new Map();

/** enemy.id -> Enemy (authoritative shared mobs). */
const enemies = new Map();

/** Handle for the broadcast loop; null when not running. */
let loop = null;

/** Monotonic connection id source (distinct from player.id). */
let _connSeq = 0;

/** Seed the FIXED enemy roster once, at startup. */
function seedEnemies() {
  for (const s of ENEMY_SEED) {
    enemies.set(s.id, {
      id: s.id,
      type: s.type,
      hp: s.hp,
      maxHp: s.hp,
      x: s.x,
      y: s.y,
      z: s.z,
      yaw: 0,
      alive: true,
      respawnAt: 0,
      spawnX: s.x,
      spawnY: s.y,
      spawnZ: s.z,
    });
  }
}

// -----------------------------------------------------------------------------
// Handshake parsing
// -----------------------------------------------------------------------------

/**
 * Pull wallet + display name out of the connection URL query string.
 * Falls back to a generated id for the id and "anon" for the name.
 */
function parseHandshake(rawUrl) {
  let wallet = "";
  let name = "anon";
  try {
    // rawUrl is a path+query (e.g. "/parties/main/global?wallet=..&name=..").
    // Give it a base so the WHATWG URL parser accepts a relative request URL.
    const url = new URL(rawUrl, "http://localhost");
    const w = url.searchParams.get("wallet");
    const n = url.searchParams.get("name");
    if (w && w.trim()) wallet = w.trim().slice(0, 64);
    if (n && n.trim()) name = n.trim().slice(0, 32);
  } catch (e) {
    // Malformed URL -> keep fallbacks.
  }
  return { wallet, name };
}

// -----------------------------------------------------------------------------
// Send / broadcast helpers
// -----------------------------------------------------------------------------

/** Send a typed message to one socket (guarded). */
function sendTo(ws, msg) {
  try {
    if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(msg));
  } catch (e) {
    /* dead socket — ignore */
  }
}

/** Broadcast a typed message to every open socket. */
function broadcast(msg) {
  let payload;
  try {
    payload = JSON.stringify(msg);
  } catch (e) {
    return;
  }
  for (const client of wss.clients) {
    try {
      if (client.readyState === client.OPEN) client.send(payload);
    } catch (e) {
      /* ignore one bad client */
    }
  }
}

/** Broadcast to everyone except one socket. */
function broadcastExcept(exceptWs, msg) {
  let payload;
  try {
    payload = JSON.stringify(msg);
  } catch (e) {
    return;
  }
  for (const client of wss.clients) {
    if (client === exceptWs) continue;
    try {
      if (client.readyState === client.OPEN) client.send(payload);
    } catch (e) {
      /* ignore */
    }
  }
}

// -----------------------------------------------------------------------------
// Input handling
// -----------------------------------------------------------------------------

/** Validate + apply an "input" message onto the player's pose. */
function applyInput(player, msg) {
  const { x, y, z, yaw, weapon } = msg;

  // Reject anything non-finite — keeps NaN/Infinity out of the snapshot.
  if (
    !isFiniteNumber(x) ||
    !isFiniteNumber(y) ||
    !isFiniteNumber(z) ||
    !isFiniteNumber(yaw)
  ) {
    return;
  }

  // Sanity clamp on position so a glitched/hostile client can't teleport to
  // absurd coordinates. Generous bounds for an arena shooter.
  const BOUND = 10000;
  player.x = clamp(x, -BOUND, BOUND);
  player.y = clamp(y, -BOUND, BOUND);
  player.z = clamp(z, -BOUND, BOUND);
  // Wrap yaw into [-PI, PI] so a hostile client can't push a huge magnitude
  // (e.g. 1e308) into the snapshot.
  player.yaw = yaw - 2 * Math.PI * Math.round(yaw / (2 * Math.PI));

  // weapon is optional/loose — coerce to a small bounded non-negative integer.
  if (isFiniteNumber(weapon)) {
    player.weapon = clamp(Math.floor(weapon), 0, MAX_WEAPON_INDEX);
  }
}

/**
 * Validate + apply a "hit" message to a shared enemy. Server-authoritative:
 * only known, still-alive enemies take damage; everything else is ignored.
 */
function applyHit(msg) {
  const { id, dmg } = msg;
  if (typeof id !== "string") return;
  if (!isFiniteNumber(dmg)) return;

  const enemy = enemies.get(id);
  if (!enemy || !enemy.alive) return; // unknown or already dead -> ignore

  const damage = clamp(dmg, 0, MAX_HIT_DMG);
  enemy.hp -= damage;

  if (enemy.hp <= 0) {
    enemy.hp = 0;
    enemy.alive = false;
    enemy.respawnAt = Date.now() + RESPAWN_DELAY_MS;
  }
}

// -----------------------------------------------------------------------------
// Enemy simulation (respawn + AI), run once per broadcast tick (~15Hz)
// -----------------------------------------------------------------------------

/**
 * Nearest CONNECTED player to an enemy by horizontal distance, or null when the
 * room has no players.
 */
function nearestPlayer(e) {
  let best = null;
  let bestD2 = Infinity;
  for (const p of players.values()) {
    const dx = p.x - e.x;
    const dz = p.z - e.z;
    const d2 = dx * dx + dz * dz;
    if (d2 < bestD2) {
      bestD2 = d2;
      best = p;
    }
  }
  return best;
}

/** Respawn timed-out dead enemies, then step alive enemies toward players. */
function updateEnemies() {
  const now = Date.now();

  for (const e of enemies.values()) {
    // --- Respawn ---
    if (!e.alive) {
      if (e.respawnAt > 0 && e.respawnAt <= now) {
        e.alive = true;
        e.hp = e.maxHp;
        e.x = e.spawnX;
        e.y = e.spawnY;
        e.z = e.spawnZ;
        e.respawnAt = 0;
      }
      continue; // dead enemies don't run AI
    }

    // --- AI ---
    const tune = ENEMY_TUNING[e.type];
    const target = nearestPlayer(e);

    // Always face the nearest player when there is one (even idle enemies face).
    let d = Infinity;
    if (target) {
      const dx = target.x - e.x;
      const dz = target.z - e.z;
      d = Math.sqrt(dx * dx + dz * dz);
      e.yaw = Math.atan2(dx, dz);
    }

    // Chase only when this enemy moves AND the nearest player is within aggro.
    const inAggro = target !== null && d <= tune.aggro;

    // Flyers track the player's height (clamped) ONLY while in aggro; otherwise
    // they let y return toward spawnY along with the horizontal drift below.
    if (tune.flies && inAggro && target) {
      e.y = clamp(target.y, 0, 6);
    }

    if (!tune.moves) continue; // skulls never translate (either direction)

    if (inAggro && target) {
      // CHASE: move toward the player at full speed, never overshoot stopDist.
      const dx = target.x - e.x;
      const dz = target.z - e.z;
      if (d > tune.stopDist) {
        const step = Math.min(tune.speed, d - tune.stopDist);
        // d > stopDist >= 0 so d > 0 -> safe to normalize.
        const nx = dx / d;
        const nz = dz / d;
        e.x += nx * step;
        e.z += nz * step;
      }
    } else {
      // RETURN: no target (or out of aggro) -> drift back to spawn anchor at
      // HALF speed (x/y/z), snapping + idling once within ~1 unit. Keeps
      // enemies spread across their map sections instead of clustering.
      const sx = e.spawnX - e.x;
      const sy = e.spawnY - e.y;
      const sz = e.spawnZ - e.z;
      const ds = Math.sqrt(sx * sx + sy * sy + sz * sz);
      if (ds <= 1) {
        e.x = e.spawnX;
        e.y = e.spawnY;
        e.z = e.spawnZ;
      } else {
        const step = Math.min(tune.speed * 0.5, ds);
        e.x += (sx / ds) * step;
        e.y += (sy / ds) * step;
        e.z += (sz / ds) * step;
      }
    }
  }

  // --- HARD POSITIONAL SEPARATION (de-overlap) ---
  // Runs AFTER all chase/return movement, BEFORE broadcast. An iterative
  // relaxation solver: each pass walks every unordered pair of ALIVE enemies and,
  // if they are closer than MIN_SPACING, slides them directly apart on x/z. This
  // mutates POSITIONS (not velocities), so the chase cannot undo it within the
  // same tick — a tight pile resolves into a clean ring within a couple ticks.
  //
  // Movers split the overlap 50/50; a non-mover (skull, moves:false) is treated
  // as immovable, so its mover partner takes the FULL correction and slides
  // around it. Two non-movers are skipped (their spawns are far apart anyway).
  //
  // Snapshot the alive roster once; O(n^2) over <=19 enemies ~= 171 pairs/pass,
  // 2 passes at 15Hz — trivially cheap.
  const alive = [];
  for (const e of enemies.values()) {
    if (e.alive) alive.push(e);
  }

  for (let pass = 0; pass < SEPARATION_PASSES; pass++) {
    for (let i = 0; i < alive.length; i++) {
      const a = alive[i];
      const tuneA = ENEMY_TUNING[a.type];
      const aMoves = !!(tuneA && tuneA.moves);

      for (let j = i + 1; j < alive.length; j++) {
        const b = alive[j];
        const tuneB = ENEMY_TUNING[b.type];
        const bMoves = !!(tuneB && tuneB.moves);

        // Two immovable enemies (e.g. two skulls) — nothing to do.
        if (!aMoves && !bMoves) continue;

        const dx = a.x - b.x;
        const dz = a.z - b.z;
        const d = Math.sqrt(dx * dx + dz * dz);

        if (d >= MIN_SPACING) continue; // already spaced — skip

        const overlap = MIN_SPACING - d;

        // Unit direction A<-B. Coincident pair (d ~= 0) would NaN a normalize,
        // so derive a stable per-pair angle from the id hashes (deterministic,
        // no randomness, always the same split).
        let nx;
        let nz;
        if (d > 1e-4) {
          nx = dx / d;
          nz = dz / d;
        } else {
          const ang = ((hashId(a.id) - hashId(b.id)) % 360) * (Math.PI / 180);
          nx = Math.cos(ang);
          nz = Math.sin(ang);
        }

        // Distribute the correction: movers share it 50/50; if one is immovable
        // the mover absorbs the FULL overlap and slides around it.
        let aShare;
        let bShare;
        if (aMoves && bMoves) {
          aShare = overlap * 0.5;
          bShare = overlap * 0.5;
        } else if (aMoves) {
          aShare = overlap;
          bShare = 0;
        } else {
          aShare = 0;
          bShare = overlap;
        }

        // Apply on x/z only; never touch y. Guard every write against NaN/Inf.
        if (aShare !== 0) {
          const ax = a.x + nx * aShare;
          const az = a.z + nz * aShare;
          if (isFiniteNumber(ax)) a.x = ax;
          if (isFiniteNumber(az)) a.z = az;
        }
        if (bShare !== 0) {
          const bx = b.x - nx * bShare;
          const bz = b.z - nz * bShare;
          if (isFiniteNumber(bx)) b.x = bx;
          if (isFiniteNumber(bz)) b.z = bz;
        }
      }
    }
  }
}

/**
 * Tiny deterministic hash of an enemy id -> non-negative integer. Used only to
 * derive a stable split direction for exactly-coincident enemies so they never
 * NaN and always separate the same way.
 */
function hashId(id) {
  let h = 0;
  const s = String(id);
  for (let i = 0; i < s.length; i++) {
    h = (h * 31 + s.charCodeAt(i)) | 0;
  }
  return h < 0 ? -h : h;
}

// -----------------------------------------------------------------------------
// Broadcast loop
// -----------------------------------------------------------------------------

/** Start the 15Hz state loop if it isn't already running. */
function startLoop() {
  if (loop !== null) return;
  loop = setInterval(tick, STATE_INTERVAL_MS);
}

/** Stop the loop (called when the room empties). */
function stopLoop() {
  if (loop === null) return;
  clearInterval(loop);
  loop = null;
}

/** Build + broadcast a full roster snapshot. */
function tick() {
  try {
    if (players.size === 0) {
      stopLoop();
      return;
    }

    // Advance server-authoritative enemies (respawn, AI) before snapshotting.
    updateEnemies();

    const playersOut = [];
    for (const p of players.values()) {
      playersOut.push({
        id: p.id,
        x: p.x,
        y: p.y,
        z: p.z,
        yaw: round2(p.yaw),
        name: p.name,
        weapon: p.weapon,
      });
    }

    const enemiesOut = [];
    for (const e of enemies.values()) {
      enemiesOut.push({
        id: e.id,
        type: e.type,
        x: round2(e.x),
        y: round2(e.y),
        z: round2(e.z),
        yaw: round2(e.yaw),
        hp: e.hp,
        maxHp: e.maxHp,
        alive: e.alive,
      });
    }

    broadcast({
      t: "state",
      v: PROTOCOL_VERSION,
      players: playersOut,
      enemies: enemiesOut,
    });
  } catch (e) {
    // A bad tick must never crash the process.
    try {
      console.error("[revenant-ws] tick error:", e && e.message ? e.message : e);
    } catch (_) {}
  }
}

// -----------------------------------------------------------------------------
// Connection lifecycle
// -----------------------------------------------------------------------------

/** Remove a socket's player and broadcast a leave; stop loop if empty. */
function removeConnection(ws) {
  const connId = connOf.get(ws);
  if (connId === undefined) return;
  connOf.delete(ws);

  const player = players.get(connId);
  if (!player) return;

  players.delete(connId);
  broadcast({ t: "leave", v: PROTOCOL_VERSION, id: player.id });

  if (players.size === 0) {
    stopLoop();
  }
}

// -----------------------------------------------------------------------------
// HTTP + WebSocket server
// -----------------------------------------------------------------------------

seedEnemies();

const server = http.createServer((req, res) => {
  // Plain health-check / liveness endpoint for Render & uptime pingers.
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("revenant-ws ok build=bugfix1\n");
});

// Accept upgrades on ANY path (client uses /parties/main/global, but stay
// forwarder-agnostic). No `path` option -> ws accepts every path.
// maxPayload: frames here are tiny JSON; cap rejects oversized/hostile frames.
const wss = new WebSocketServer({ server, maxPayload: 16384 });

// Heartbeat: reap half-open sockets (mobile sleep / NAT timeout / lid close) so
// a dropped client doesn't hold a room-cap slot or leave a phantom player entry.
// ws has no keepalive by default; Render/proxies drop idle conns silently.
const HEARTBEAT_INTERVAL_MS = 30000;
const heartbeat = setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.isAlive === false) {
      try { ws.terminate(); } catch (e) {} // fires 'close' -> removeConnection
      continue;
    }
    ws.isAlive = false;
    try { ws.ping(); } catch (e) {}
  }
}, HEARTBEAT_INTERVAL_MS);
wss.on("close", () => { clearInterval(heartbeat); });

wss.on("connection", (ws, req) => {
  try {
    // Liveness tracking for the heartbeat reaper above.
    ws.isAlive = true;
    ws.on("pong", () => { ws.isAlive = true; });

    // Room is full -> tell the client to fall back to solo, then close.
    if (players.size >= ROOM_CAP) {
      sendTo(ws, { t: "full", v: PROTOCOL_VERSION });
      try {
        ws.close();
      } catch (e) {}
      return;
    }

    const { wallet, name } = parseHandshake(req.url || "");
    const id = wallet && wallet.length ? wallet : genId();

    // Distinct internal connection key (two sockets may share a wallet id).
    _connSeq += 1;
    const connId = "c" + _connSeq;
    connOf.set(ws, connId);

    const player = {
      id: id,
      x: 0,
      y: 2,
      z: 0,
      yaw: 0,
      name: name,
      weapon: 0,
    };
    players.set(connId, player);

    // Tell the new client its own id.
    sendTo(ws, { t: "welcome", v: PROTOCOL_VERSION, id: player.id });

    // Tell everyone else that this player joined.
    broadcastExcept(ws, {
      t: "join",
      v: PROTOCOL_VERSION,
      id: player.id,
      name: player.name,
    });

    startLoop();

    ws.on("message", (data, isBinary) => {
      try {
        const connId2 = connOf.get(ws);
        if (connId2 === undefined) return;
        const p = players.get(connId2);
        if (!p) return;

        // We only speak JSON text. Ignore binary frames silently.
        if (isBinary) return;

        let parsed;
        try {
          parsed = JSON.parse(data.toString());
        } catch (e) {
          return; // malformed JSON -> ignore
        }

        if (!parsed || typeof parsed !== "object") return;

        if (parsed.t === "input") {
          applyInput(p, parsed);
        } else if (parsed.t === "hit") {
          applyHit(parsed);
        }
        // Unknown message types are ignored (forward-compat).
      } catch (e) {
        // A bad frame must never crash the process.
      }
    });

    ws.on("close", () => {
      try {
        removeConnection(ws);
      } catch (e) {}
    });

    ws.on("error", () => {
      // Treat a socket error like a disconnect.
      try {
        removeConnection(ws);
      } catch (e) {}
    });
  } catch (e) {
    try {
      console.error(
        "[revenant-ws] connection error:",
        e && e.message ? e.message : e
      );
    } catch (_) {}
    try {
      ws.close();
    } catch (_) {}
  }
});

wss.on("error", (e) => {
  try {
    console.error(
      "[revenant-ws] server error:",
      e && e.message ? e.message : e
    );
  } catch (_) {}
});

const PORT = Number(process.env.PORT) || 1999;
const HOST = "0.0.0.0";

server.listen(PORT, HOST, () => {
  console.log(
    "[revenant-ws] listening on " +
      HOST +
      ":" +
      PORT +
      " (state every " +
      STATE_INTERVAL_MS +
      "ms, cap " +
      ROOM_CAP +
      ", " +
      enemies.size +
      " enemies)"
  );
});
