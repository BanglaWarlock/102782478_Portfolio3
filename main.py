#!/usr/bin/env python3
"""
FloodWatch Monitor — Portfolio 3 (Render Deployment)
SWE40006 Software Deployment and Evolution

A data-driven web dashboard for the FloodWatch IoT flood monitoring system.
Connects to an existing MongoDB Atlas database (external managed DB) and:
  - Reads live sensor data from existing collections (villages, river_nodes, alerts, heartbeats)
  - Allows users to create & manage named node watchlists (persistent CRUD operations)
  - Streams real-time heartbeat events via SSE, filtered to watchlist subscriptions

This satisfies Task 3.3 (High Distinction):
  - Persistent external database: MongoDB Atlas (reads AND writes)
  - Data-driven web app: dashboard reads sensor data; watchlists written/deleted
  - Full-stack deployment: FastAPI web service + Atlas managed DB on Render
  - Live app with working database functionality

REST endpoints:
  GET    /                          → Dashboard (single-page HTML app)
  GET    /api/health                → Health check
  GET    /api/villages              → List all villages with status
  GET    /api/nodes                 → List all nodes (?village_id, ?status)
  GET    /api/nodes/{node_id}       → Single node live state
  GET    /api/alerts                → Recent alerts (?village_id, ?limit)
  POST   /api/watchlists            → Create a watchlist {name, node_ids[]}
  GET    /api/watchlists            → List all saved watchlists
  DELETE /api/watchlists/{id}       → Delete a watchlist by ID
  GET    /api/stream                → SSE live heartbeat stream (?watchlist_id)
"""

import asyncio
import json
import logging
import os
from datetime import datetime, timezone, timedelta

from bson import ObjectId
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pymongo import MongoClient, DESCENDING
from pydantic import BaseModel
from sse_starlette.sse import EventSourceResponse
from starlette.requests import Request

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("floodwatch-monitor")

# ── Configuration ─────────────────────────────────────────────────────────────

MONGO_URI          = os.getenv("MONGO_URI")
MONGO_DB           = os.getenv("MONGO_DB", "flood_monitor")
POLL_INTERVAL      = float(os.getenv("POLL_INTERVAL_SECONDS", "5"))   # SSE poll cadence
MAX_FEED_EVENTS    = int(os.getenv("MAX_FEED_EVENTS", "50"))          # events kept in SSE buffer

if not MONGO_URI:
    raise RuntimeError("MONGO_URI environment variable is not set — check Render env vars")

# ── MongoDB Atlas ─────────────────────────────────────────────────────────────

mongo          = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
db             = mongo[MONGO_DB]
col_villages   = db["villages"]
col_rivers     = db["river_nodes"]
col_heartbeats = db["heartbeats"]
col_alerts     = db["alerts"]
col_watchlists = db["watchlists"]     # NEW — created by this app for portfolio read/write demo

log.info(f"Connected to MongoDB Atlas — database: {MONGO_DB}")

# ── Helpers ───────────────────────────────────────────────────────────────────

_REAL_FILTER = {"is_sample": {"$ne": True}}   # exclude demo/sample documents


def _clean(value):
    """Recursively convert MongoDB document to JSON-serialisable dict.
    Converts ObjectId → str, datetime → ISO string, strips raw _id field."""
    if isinstance(value, dict):
        out = {}
        for k, v in value.items():
            if k == "_id":
                out["id"] = str(v)    # expose _id as 'id' (not stripped)
            else:
                out[k] = _clean(v)
        return out
    if isinstance(value, list):
        return [_clean(v) for v in value]
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, ObjectId):
        return str(value)
    return value


def _water_level_label(level: int) -> str:
    return {0: "Normal", 1: "Watch", 2: "Warning", 3: "Danger"}.get(level, "Unknown")


# ── FastAPI app ───────────────────────────────────────────────────────────────

app = FastAPI(
    title="FloodWatch Monitor",
    version="1.0.0",
    description="FloodWatch IoT dashboard — Portfolio 3 (Render deployment demo)",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "DELETE"],
    allow_headers=["*"],
)

# ── Health ─────────────────────────────────────────────────────────────────────

@app.get("/api/health", tags=["health"])
def health():
    """Returns service status. Used as the Render health-check endpoint."""
    try:
        mongo.admin.command("ping")
        db_status = "connected"
    except Exception as e:
        db_status = f"error: {e}"
    return {
        "status": "ok",
        "service": "floodwatch-monitor",
        "version": "1.0.0",
        "database": db_status,
    }


# ── Villages ──────────────────────────────────────────────────────────────────

@app.get("/api/villages", tags=["data"])
def list_villages():
    """
    Return all villages with their current status and node counts.
    Excludes topology and forecast fields to keep the payload small.
    Data is read from the existing 'villages' collection in MongoDB Atlas.
    """
    docs = col_villages.find(
        _REAL_FILTER,
        {"topology": 0, "weather_forecast": 0}   # omit large nested fields
    )
    return [_clean(d) for d in docs]


# ── Nodes ─────────────────────────────────────────────────────────────────────

@app.get("/api/nodes", tags=["data"])
def list_nodes(
    village_id: str = Query(default=None, description="Filter by village ID"),
    status:     str = Query(default=None, description="Filter: online | offline"),
):
    """
    Return all river nodes with their current live state (water level, battery,
    GPS fix, online/offline). Optionally filter by village or status.
    Data is read from the 'river_nodes' collection in MongoDB Atlas.
    """
    query: dict = {**_REAL_FILTER}
    if village_id:
        query["village_id"] = village_id
    if status:
        query["status"] = status
    return [_clean(d) for d in col_rivers.find(query)]


@app.get("/api/nodes/{node_id}", tags=["data"])
def get_node(node_id: str):
    """Return a single river node by its node_id."""
    doc = col_rivers.find_one({"node_id": node_id, **_REAL_FILTER})
    if not doc:
        raise HTTPException(404, f"Node '{node_id}' not found")
    return _clean(doc)


# ── Alerts ────────────────────────────────────────────────────────────────────

@app.get("/api/alerts", tags=["data"])
def list_alerts(
    village_id: str = Query(default=None, description="Filter by village ID"),
    limit:      int = Query(default=20, ge=1, le=100, description="Max alerts to return"),
):
    """
    Return the most recent alerts, newest first.
    Optionally filter by village. Data is read from the 'alerts' collection.
    """
    query: dict = {**_REAL_FILTER}
    if village_id:
        query["village_id"] = village_id
    cursor = col_alerts.find(query).sort("timestamp", DESCENDING).limit(limit)
    return [_clean(d) for d in cursor]


# ── Watchlists (CRUD — demonstrates database read/write) ─────────────────────

class WatchlistCreate(BaseModel):
    """Request body for creating a watchlist."""
    name:     str
    node_ids: list[str]


@app.post("/api/watchlists", status_code=201, tags=["watchlists"])
def create_watchlist(body: WatchlistCreate):
    """
    CREATE a new watchlist. Writes a new document to the 'watchlists'
    collection in MongoDB Atlas. This demonstrates persistent database WRITES.

    A watchlist is a named group of node IDs the user wants to monitor.
    Once created, it can be used to filter the SSE stream to only those nodes.
    """
    name = body.name.strip()
    if not name:
        raise HTTPException(400, "Watchlist name cannot be empty")
    if not body.node_ids:
        raise HTTPException(400, "At least one node_id is required")

    # Verify all node_ids exist
    valid_nodes = {
        d["node_id"]
        for d in col_rivers.find(
            {"node_id": {"$in": body.node_ids}, **_REAL_FILTER},
            {"node_id": 1}
        )
    }
    invalid = [nid for nid in body.node_ids if nid not in valid_nodes]
    if invalid:
        raise HTTPException(400, f"Unknown node IDs: {invalid}")

    doc = {
        "name":       name,
        "node_ids":   body.node_ids,
        "created_at": datetime.now(timezone.utc),
    }
    result = col_watchlists.insert_one(doc)
    log.info(f"Watchlist created: '{name}' ({len(body.node_ids)} nodes)")
    return _clean({**doc, "_id": result.inserted_id})


@app.get("/api/watchlists", tags=["watchlists"])
def list_watchlists():
    """
    READ all saved watchlists. Reads from the 'watchlists' collection.
    This demonstrates persistent database READS.
    """
    return [_clean(d) for d in col_watchlists.find().sort("created_at", DESCENDING)]


@app.delete("/api/watchlists/{watchlist_id}", tags=["watchlists"])
def delete_watchlist(watchlist_id: str):
    """
    DELETE a watchlist by its MongoDB ObjectId.
    Demonstrates persistent database DELETES.
    """
    try:
        oid = ObjectId(watchlist_id)
    except Exception:
        raise HTTPException(400, "Invalid watchlist ID format")

    result = col_watchlists.delete_one({"_id": oid})
    if result.deleted_count == 0:
        raise HTTPException(404, "Watchlist not found")

    log.info(f"Watchlist deleted: {watchlist_id}")
    return {"deleted": watchlist_id}


# ── SSE Stream ────────────────────────────────────────────────────────────────

@app.get("/api/stream", tags=["stream"])
async def sse_stream(
    request:      Request,
    watchlist_id: str = Query(
        default=None,
        description="If given, only heartbeats from watchlist nodes are streamed"
    ),
):
    """
    Server-Sent Events stream of live heartbeat data.

    Polls the 'heartbeats' collection in MongoDB Atlas every POLL_INTERVAL seconds
    for documents newer than the last seen timestamp. No message broker (Redis)
    required — polling is sufficient for a portfolio demonstration.

    If ?watchlist_id=<id> is given, the stream is filtered to only nodes in that
    watchlist, implementing the 'subscribe to nodes' feature.

    Usage (JavaScript):
        const es = new EventSource('/api/stream?watchlist_id=<id>');
        es.addEventListener('heartbeat', e => console.log(JSON.parse(e.data)));
        es.addEventListener('alert', e => console.log(JSON.parse(e.data)));
    """
    # Resolve node filter from watchlist (if provided)
    node_filter: list[str] | None = None
    watchlist_name: str | None = None
    if watchlist_id:
        try:
            wl = col_watchlists.find_one({"_id": ObjectId(watchlist_id)})
        except Exception:
            wl = None
        if not wl:
            raise HTTPException(404, f"Watchlist '{watchlist_id}' not found")
        node_filter    = wl["node_ids"]
        watchlist_name = wl["name"]

    log.info(
        f"SSE client connected"
        + (f" — watchlist '{watchlist_name}' ({len(node_filter)} nodes)" if node_filter else " — all nodes")
    )

    async def generator():
        # Start from recent data — only stream new events from now onwards
        last_hb_ts   = datetime.now(timezone.utc) - timedelta(seconds=POLL_INTERVAL)
        last_alert_ts = last_hb_ts

        # Send an initial connected event
        yield {
            "event": "connected",
            "data": json.dumps({
                "message":   "Stream connected",
                "watchlist": watchlist_name,
                "nodes":     node_filter,
            }),
        }

        try:
            while True:
                if await request.is_disconnected():
                    log.info("SSE client disconnected")
                    break

                # ── Poll for new heartbeats ─────────────────────────────────
                hb_query: dict = {
                    "timestamp": {"$gt": last_hb_ts},
                    **_REAL_FILTER,
                }
                if node_filter:
                    hb_query["node_id"] = {"$in": node_filter}

                new_heartbeats = list(
                    col_heartbeats.find(hb_query)
                    .sort("timestamp", DESCENDING)
                    .limit(MAX_FEED_EVENTS)
                )

                for doc in reversed(new_heartbeats):   # oldest first
                    ts = doc.get("timestamp")
                    if ts and ts > last_hb_ts:
                        last_hb_ts = ts
                    clean = _clean(doc)
                    clean["water_level_label"] = _water_level_label(clean.get("water_level", 0))
                    yield {"event": "heartbeat", "data": json.dumps(clean)}

                # ── Poll for new alerts ─────────────────────────────────────
                alert_query: dict = {
                    "timestamp": {"$gt": last_alert_ts},
                    **_REAL_FILTER,
                }
                if node_filter:
                    alert_query["node_id"] = {"$in": node_filter}

                new_alerts = list(
                    col_alerts.find(alert_query)
                    .sort("timestamp", DESCENDING)
                    .limit(10)
                )

                for doc in reversed(new_alerts):
                    ts = doc.get("timestamp")
                    if ts and ts > last_alert_ts:
                        last_alert_ts = ts
                    yield {"event": "alert", "data": json.dumps(_clean(doc))}

                # ── Keepalive comment every poll cycle ──────────────────────
                yield {"comment": f"poll {datetime.now(timezone.utc).strftime('%H:%M:%S')}"}

                await asyncio.sleep(POLL_INTERVAL)

        except asyncio.CancelledError:
            log.info("SSE stream cancelled")

    return EventSourceResponse(generator())


# ── Dashboard (served HTML) ───────────────────────────────────────────────────

DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en" class="h-full">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>FloodWatch Monitor</title>
<script src="https://cdn.tailwindcss.com"></script>
<script>
  tailwind.config = {
    theme: {
      extend: {
        colors: {
          brand: { 50:'#eff6ff', 500:'#3b82f6', 600:'#2563eb', 700:'#1d4ed8', 900:'#1e3a8a' }
        }
      }
    }
  }
</script>
<style>
  :root { font-family: system-ui, -apple-system, sans-serif; }
  .tab-btn { @apply px-4 py-2 rounded-t-lg text-sm font-medium transition-colors; }
  .tab-btn.active { @apply bg-gray-800 text-white; }
  .tab-btn:not(.active) { @apply text-gray-400 hover:text-white hover:bg-gray-700; }
  .level-0 { @apply bg-blue-900 text-blue-200 border border-blue-700; }
  .level-1 { @apply bg-yellow-900 text-yellow-200 border border-yellow-700; }
  .level-2 { @apply bg-orange-900 text-orange-200 border border-orange-700; }
  .level-3 { @apply bg-red-900 text-red-200 border border-red-700; }
  .level-badge-0 { @apply bg-blue-800 text-blue-200; }
  .level-badge-1 { @apply bg-yellow-700 text-yellow-100; }
  .level-badge-2 { @apply bg-orange-700 text-orange-100; }
  .level-badge-3 { @apply bg-red-700 text-red-100 animate-pulse; }
  .alert-flood    { @apply border-l-red-500; }
  .alert-battery  { @apply border-l-yellow-500; }
  .alert-gps      { @apply border-l-purple-500; }
  .feed-event { animation: fadeIn 0.3s ease-in; }
  @keyframes fadeIn { from { opacity: 0; transform: translateY(-8px); } to { opacity: 1; transform: none; } }
  ::-webkit-scrollbar { width: 6px; }
  ::-webkit-scrollbar-track { background: #1f2937; }
  ::-webkit-scrollbar-thumb { background: #4b5563; border-radius: 3px; }
</style>
</head>
<body class="bg-gray-900 text-gray-100 min-h-screen flex flex-col">

<!-- ── Header ────────────────────────────────────────────────────────────── -->
<header class="bg-gray-800 border-b border-gray-700 px-6 py-3 flex items-center justify-between">
  <div class="flex items-center gap-3">
    <div class="w-8 h-8 rounded-lg bg-blue-600 flex items-center justify-center text-lg">🌊</div>
    <div>
      <h1 class="font-bold text-lg leading-tight">FloodWatch Monitor</h1>
      <p class="text-xs text-gray-400">Live IoT Flood Sensor Dashboard · Portfolio 3 — Render Deployment</p>
    </div>
  </div>
  <div class="flex items-center gap-4 text-sm">
    <div class="flex items-center gap-1.5">
      <span id="db-dot" class="w-2 h-2 rounded-full bg-gray-500"></span>
      <span id="db-status" class="text-gray-400 text-xs">Checking…</span>
    </div>
    <a href="/docs" target="_blank"
       class="text-blue-400 hover:text-blue-300 text-xs border border-blue-800 rounded px-2 py-1">
      API Docs ↗
    </a>
  </div>
</header>

<!-- ── Stats bar ──────────────────────────────────────────────────────────── -->
<div id="stats-bar" class="bg-gray-800 border-b border-gray-700 px-6 py-2 flex gap-6 text-sm text-gray-300">
  <span>Villages: <strong id="stat-villages" class="text-white">—</strong></span>
  <span>Nodes: <strong id="stat-nodes" class="text-white">—</strong></span>
  <span>Online: <strong id="stat-online" class="text-green-400">—</strong></span>
  <span>Offline: <strong id="stat-offline" class="text-red-400">—</strong></span>
  <span>Watchlists: <strong id="stat-watchlists" class="text-blue-400">—</strong></span>
</div>

<!-- ── Tabs ───────────────────────────────────────────────────────────────── -->
<div class="bg-gray-900 border-b border-gray-700 px-6 flex gap-1 pt-2">
  <button class="tab-btn active" onclick="showTab('dashboard')">📊 Dashboard</button>
  <button class="tab-btn" onclick="showTab('watchlists')">👁 Watchlists</button>
  <button class="tab-btn" onclick="showTab('feed')">📡 Live Feed</button>
  <button class="tab-btn" onclick="showTab('alerts')">🚨 Alerts</button>
</div>

<!-- ── Tab: Dashboard ─────────────────────────────────────────────────────── -->
<div id="tab-dashboard" class="flex-1 flex overflow-hidden p-4 gap-4">

  <!-- Village sidebar -->
  <div class="w-52 shrink-0 flex flex-col gap-2">
    <p class="text-xs text-gray-400 font-semibold uppercase tracking-wider px-1">Villages</p>
    <div id="village-list" class="flex flex-col gap-1 overflow-y-auto">
      <p class="text-gray-500 text-sm px-2">Loading…</p>
    </div>
  </div>

  <!-- Node grid -->
  <div class="flex-1 overflow-y-auto">
    <div class="flex items-center justify-between mb-3">
      <p class="text-xs text-gray-400 font-semibold uppercase tracking-wider">
        River Nodes — <span id="selected-village-name" class="text-gray-300 normal-case">All Villages</span>
      </p>
      <div class="flex gap-2">
        <button onclick="filterNodes('all')"
                class="text-xs px-2 py-1 rounded bg-gray-700 hover:bg-gray-600 text-gray-300">All</button>
        <button onclick="filterNodes('online')"
                class="text-xs px-2 py-1 rounded bg-green-900 hover:bg-green-800 text-green-300">Online</button>
        <button onclick="filterNodes('offline')"
                class="text-xs px-2 py-1 rounded bg-red-900 hover:bg-red-800 text-red-300">Offline</button>
      </div>
    </div>
    <div id="node-grid" class="grid grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-3">
      <p class="text-gray-500 text-sm col-span-4">Loading…</p>
    </div>
  </div>
</div>

<!-- ── Tab: Watchlists ────────────────────────────────────────────────────── -->
<div id="tab-watchlists" class="hidden flex-1 p-6 overflow-y-auto">
  <div class="max-w-2xl mx-auto flex flex-col gap-6">

    <!-- Create form -->
    <div class="bg-gray-800 rounded-xl p-5 border border-gray-700">
      <h2 class="font-semibold mb-3 text-blue-300">Create Watchlist</h2>
      <p class="text-xs text-gray-400 mb-4">
        A watchlist lets you subscribe to specific sensor nodes. Once saved, you can use it to filter
        the Live Feed to only show events from those nodes.
      </p>
      <div class="flex gap-3 mb-4">
        <input id="wl-name" type="text" placeholder="Watchlist name (e.g. Village A Nodes)"
               class="flex-1 bg-gray-700 border border-gray-600 rounded-lg px-3 py-2 text-sm
                      focus:outline-none focus:border-blue-500 text-white placeholder-gray-500">
        <button onclick="createWatchlist()"
                class="px-4 py-2 bg-blue-600 hover:bg-blue-500 rounded-lg text-sm font-medium transition-colors">
          Save
        </button>
      </div>
      <p class="text-xs text-gray-400 mb-2 font-medium">Select nodes to watch:</p>
      <div id="node-checkboxes" class="grid grid-cols-2 gap-1 max-h-60 overflow-y-auto text-sm">
        <p class="text-gray-500">Loading nodes…</p>
      </div>
      <p id="wl-error" class="text-red-400 text-xs mt-2 hidden"></p>
      <p id="wl-success" class="text-green-400 text-xs mt-2 hidden"></p>
    </div>

    <!-- Saved watchlists -->
    <div>
      <h2 class="font-semibold mb-3 text-blue-300">Saved Watchlists</h2>
      <div id="watchlist-cards" class="flex flex-col gap-3">
        <p class="text-gray-500 text-sm">Loading…</p>
      </div>
    </div>
  </div>
</div>

<!-- ── Tab: Live Feed ─────────────────────────────────────────────────────── -->
<div id="tab-feed" class="hidden flex-1 p-4 flex flex-col gap-3 overflow-hidden">
  <div class="flex items-center gap-3 flex-wrap">
    <select id="feed-watchlist"
            class="bg-gray-700 border border-gray-600 rounded-lg px-3 py-2 text-sm
                   focus:outline-none focus:border-blue-500 text-white">
      <option value="">— All nodes (no filter) —</option>
    </select>
    <button id="feed-btn" onclick="toggleFeed()"
            class="px-4 py-2 bg-green-700 hover:bg-green-600 rounded-lg text-sm font-medium transition-colors">
      ▶ Connect
    </button>
    <button onclick="clearFeed()"
            class="px-4 py-2 bg-gray-700 hover:bg-gray-600 rounded-lg text-sm font-medium transition-colors">
      Clear
    </button>
    <div class="flex items-center gap-1.5 ml-auto text-xs text-gray-400">
      <span id="feed-dot" class="w-2 h-2 rounded-full bg-gray-500"></span>
      <span id="feed-status">Disconnected</span>
    </div>
  </div>
  <p class="text-xs text-gray-500">
    The SSE stream polls MongoDB Atlas every 5 seconds for new heartbeat and alert events.
    Select a watchlist above to receive only events from your subscribed nodes.
  </p>
  <div id="feed-list"
       class="flex-1 overflow-y-auto flex flex-col gap-1.5 rounded-xl bg-gray-800 p-3 border border-gray-700">
    <p class="text-gray-600 text-sm text-center mt-8">Press Connect to start the live stream</p>
  </div>
  <p class="text-xs text-gray-600 text-right">Showing up to <span id="feed-count">0</span> events</p>
</div>

<!-- ── Tab: Alerts ────────────────────────────────────────────────────────── -->
<div id="tab-alerts" class="hidden flex-1 p-4 overflow-y-auto">
  <div class="flex items-center justify-between mb-3">
    <p class="text-xs text-gray-400 font-semibold uppercase tracking-wider">Recent Alerts</p>
    <button onclick="loadAlerts()"
            class="text-xs px-2 py-1 rounded bg-gray-700 hover:bg-gray-600 text-gray-300">
      Refresh
    </button>
  </div>
  <div id="alert-list" class="flex flex-col gap-2 max-w-3xl">
    <p class="text-gray-500 text-sm">Loading…</p>
  </div>
</div>

<!-- ── Scripts ────────────────────────────────────────────────────────────── -->
<script>
// ─── State ──────────────────────────────────────────────────────────────────
let allVillages   = [];
let allNodes      = [];
let allWatchlists = [];
let selectedVillage = null;
let nodeStatusFilter = 'all';
let feedSource    = null;
let feedEvents    = [];
const MAX_FEED    = 100;

// ─── Tab switching ───────────────────────────────────────────────────────────
function showTab(name) {
  ['dashboard','watchlists','feed','alerts'].forEach(t => {
    document.getElementById('tab-' + t).classList.add('hidden');
    document.getElementById('tab-' + t).classList.remove('flex', 'flex-col', 'flex-1');
  });
  const el = document.getElementById('tab-' + name);
  el.classList.remove('hidden');
  if (['feed','dashboard'].includes(name)) el.classList.add('flex', 'flex-col', 'flex-1');
  document.querySelectorAll('.tab-btn').forEach((b, i) => {
    b.classList.toggle('active', ['dashboard','watchlists','feed','alerts'][i] === name);
  });
  if (name === 'alerts')     loadAlerts();
  if (name === 'watchlists') loadWatchlistsTab();
  if (name === 'feed')       loadFeedWatchlistOptions();
}

// ─── Water level helpers ─────────────────────────────────────────────────────
const LEVEL_LABELS  = ['Normal', 'Watch', 'Warning', 'Danger'];
const LEVEL_COLOURS = ['blue', 'yellow', 'orange', 'red'];

function levelBadge(level) {
  const l = Math.min(level || 0, 3);
  const colours = [
    'bg-blue-800 text-blue-200',
    'bg-yellow-700 text-yellow-100',
    'bg-orange-700 text-orange-100',
    'bg-red-700 text-red-100',
  ];
  return `<span class="text-xs px-2 py-0.5 rounded-full font-semibold ${colours[l]}">${LEVEL_LABELS[l]}</span>`;
}

function battBadge(v) {
  if (!v) return '<span class="text-gray-500 text-xs">—</span>';
  const color = v >= 3.7 ? 'text-green-400' : v >= 3.5 ? 'text-yellow-400' : 'text-red-400';
  return `<span class="${color} text-xs font-mono">${v.toFixed(2)}V</span>`;
}

function timeAgo(iso) {
  if (!iso) return '—';
  const secs = Math.floor((Date.now() - new Date(iso)) / 1000);
  if (secs < 60)   return secs + 's ago';
  if (secs < 3600) return Math.floor(secs / 60) + 'm ago';
  return Math.floor(secs / 3600) + 'h ago';
}

// ─── API fetch helper ────────────────────────────────────────────────────────
async function api(path) {
  const r = await fetch('/api' + path);
  if (!r.ok) throw new Error(`API ${path} → ${r.status}`);
  return r.json();
}

// ─── Health check ────────────────────────────────────────────────────────────
async function checkHealth() {
  try {
    const h = await api('/health');
    const ok = h.database === 'connected';
    document.getElementById('db-dot').className =
      `w-2 h-2 rounded-full ${ok ? 'bg-green-500' : 'bg-red-500'}`;
    document.getElementById('db-status').textContent =
      ok ? 'MongoDB Atlas connected' : 'DB error: ' + h.database;
  } catch { /* silent */ }
}

// ─── Dashboard ───────────────────────────────────────────────────────────────
async function loadDashboard() {
  const [villages, nodes] = await Promise.all([api('/villages'), api('/nodes')]);
  allVillages = villages;
  allNodes    = nodes;

  // Update stats bar
  document.getElementById('stat-villages').textContent = villages.length;
  document.getElementById('stat-nodes').textContent    = nodes.length;
  document.getElementById('stat-online').textContent   = nodes.filter(n => n.status === 'online').length;
  document.getElementById('stat-offline').textContent  = nodes.filter(n => n.status === 'offline').length;

  renderVillageList();
  renderNodeGrid();
}

function renderVillageList() {
  const container = document.getElementById('village-list');
  const items = [{ village_id: null, name: 'All Villages' }, ...allVillages];
  container.innerHTML = items.map(v => {
    const online  = v.village_id ? allNodes.filter(n => n.village_id === v.village_id && n.status === 'online').length : allNodes.filter(n => n.status === 'online').length;
    const total   = v.village_id ? allNodes.filter(n => n.village_id === v.village_id).length : allNodes.length;
    const active  = selectedVillage === v.village_id;
    const statusDot = v.village_id
      ? `<span class="w-1.5 h-1.5 rounded-full ${v.status === 'online' ? 'bg-green-500' : 'bg-gray-500'} mt-0.5"></span>`
      : '';
    return `<button onclick="selectVillage(${JSON.stringify(v.village_id)})"
      class="text-left px-3 py-2 rounded-lg text-sm transition-colors
             ${active ? 'bg-blue-800 text-white' : 'hover:bg-gray-800 text-gray-300'}
             flex items-start gap-2">
      ${statusDot}
      <span class="flex-1 min-w-0">
        <span class="block truncate font-medium">${v.name || v.village_id}</span>
        <span class="text-xs text-gray-400">${online}/${total} online</span>
      </span>
    </button>`;
  }).join('');
}

function selectVillage(vid) {
  selectedVillage = vid;
  const name = vid ? (allVillages.find(v => v.village_id === vid)?.name || vid) : 'All Villages';
  document.getElementById('selected-village-name').textContent = name;
  renderVillageList();
  renderNodeGrid();
}

function filterNodes(f) {
  nodeStatusFilter = f;
  renderNodeGrid();
}

function renderNodeGrid() {
  let nodes = selectedVillage
    ? allNodes.filter(n => n.village_id === selectedVillage)
    : allNodes;
  if (nodeStatusFilter !== 'all')
    nodes = nodes.filter(n => n.status === nodeStatusFilter);

  const grid = document.getElementById('node-grid');
  if (!nodes.length) {
    grid.innerHTML = '<p class="col-span-4 text-gray-500 text-sm">No nodes match the current filter.</p>';
    return;
  }

  const levelClass = ['level-0','level-1','level-2','level-3'];
  grid.innerHTML = nodes.map(n => {
    const lvl   = Math.min(n.water_level || 0, 3);
    const depth = n.depth !== undefined ? `Depth ${n.depth}` : '';
    const gps   = n.gps_fix ? '📍' : '';
    return `<div class="rounded-xl p-3 border ${levelClass[lvl]} opacity-${n.status === 'offline' ? '50' : '100'}">
      <div class="flex items-start justify-between mb-2">
        <div class="font-mono text-xs text-gray-400 truncate max-w-[120px]">${n.node_id}</div>
        <span class="text-xs ${n.status === 'online' ? 'text-green-400' : 'text-gray-500'}">${n.status === 'online' ? '●' : '○'}</span>
      </div>
      ${levelBadge(lvl)}
      <div class="mt-2 flex flex-wrap gap-x-3 gap-y-1 text-xs text-gray-400">
        <span>${battBadge(n.battery_voltage)}</span>
        ${depth ? `<span>${depth}</span>` : ''}
        ${gps}
      </div>
      <div class="text-xs text-gray-500 mt-1">${timeAgo(n.last_seen)}</div>
    </div>`;
  }).join('');
}

// ─── Watchlists tab ──────────────────────────────────────────────────────────
async function loadWatchlistsTab() {
  const [nodes, watchlists] = await Promise.all([api('/nodes'), api('/watchlists')]);
  allNodes      = nodes;
  allWatchlists = watchlists;
  document.getElementById('stat-watchlists').textContent = watchlists.length;

  // Node checkboxes
  const cb = document.getElementById('node-checkboxes');
  cb.innerHTML = nodes.map(n => `
    <label class="flex items-center gap-2 cursor-pointer hover:text-white transition-colors py-0.5">
      <input type="checkbox" value="${n.node_id}" class="node-cb accent-blue-500">
      <span class="font-mono text-xs truncate">${n.node_id}</span>
      <span class="text-gray-500 text-xs">${n.village_id || ''}</span>
    </label>`).join('');

  // Watchlist cards
  renderWatchlistCards(watchlists);
}

function renderWatchlistCards(watchlists) {
  const c = document.getElementById('watchlist-cards');
  if (!watchlists.length) {
    c.innerHTML = '<p class="text-gray-500 text-sm">No watchlists yet — create one above.</p>';
    return;
  }
  c.innerHTML = watchlists.map(wl => `
    <div class="bg-gray-800 rounded-xl p-4 border border-gray-700 flex items-start justify-between gap-3">
      <div class="flex-1 min-w-0">
        <p class="font-medium text-white">${wl.name}</p>
        <p class="text-xs text-gray-400 mt-1">
          ${wl.node_ids.length} node${wl.node_ids.length !== 1 ? 's' : ''} ·
          Created ${timeAgo(wl.created_at)}
        </p>
        <div class="flex flex-wrap gap-1 mt-2">
          ${wl.node_ids.map(id => `<span class="text-xs bg-gray-700 rounded px-2 py-0.5 font-mono text-gray-300">${id}</span>`).join('')}
        </div>
      </div>
      <button onclick="deleteWatchlist('${wl.id}')"
              class="text-red-400 hover:text-red-300 text-xs px-2 py-1 rounded bg-gray-700
                     hover:bg-gray-600 transition-colors shrink-0">
        Delete
      </button>
    </div>`).join('');
}

async function createWatchlist() {
  const name    = document.getElementById('wl-name').value.trim();
  const nodeIds = [...document.querySelectorAll('.node-cb:checked')].map(cb => cb.value);
  const errEl   = document.getElementById('wl-error');
  const okEl    = document.getElementById('wl-success');
  errEl.classList.add('hidden');
  okEl.classList.add('hidden');

  if (!name)        { errEl.textContent = 'Please enter a watchlist name.'; errEl.classList.remove('hidden'); return; }
  if (!nodeIds.length) { errEl.textContent = 'Select at least one node.'; errEl.classList.remove('hidden'); return; }

  try {
    const r = await fetch('/api/watchlists', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name, node_ids: nodeIds }),
    });
    if (!r.ok) {
      const e = await r.json();
      throw new Error(e.detail || 'Server error');
    }
    document.getElementById('wl-name').value = '';
    document.querySelectorAll('.node-cb').forEach(cb => cb.checked = false);
    okEl.textContent = `Watchlist "${name}" saved with ${nodeIds.length} node(s)!`;
    okEl.classList.remove('hidden');
    await loadWatchlistsTab();
  } catch(e) {
    errEl.textContent = e.message;
    errEl.classList.remove('hidden');
  }
}

async function deleteWatchlist(id) {
  if (!confirm('Delete this watchlist?')) return;
  await fetch(`/api/watchlists/${id}`, { method: 'DELETE' });
  await loadWatchlistsTab();
}

// ─── Live Feed ───────────────────────────────────────────────────────────────
async function loadFeedWatchlistOptions() {
  const watchlists = await api('/watchlists');
  allWatchlists    = watchlists;
  const sel = document.getElementById('feed-watchlist');
  const cur = sel.value;
  sel.innerHTML = '<option value="">— All nodes (no filter) —</option>' +
    watchlists.map(wl => `<option value="${wl.id}">${wl.name} (${wl.node_ids.length} nodes)</option>`).join('');
  if (cur) sel.value = cur;
}

function toggleFeed() {
  if (feedSource) {
    feedSource.close();
    feedSource = null;
    document.getElementById('feed-btn').textContent    = '▶ Connect';
    document.getElementById('feed-btn').className      = 'px-4 py-2 bg-green-700 hover:bg-green-600 rounded-lg text-sm font-medium transition-colors';
    document.getElementById('feed-dot').className      = 'w-2 h-2 rounded-full bg-gray-500';
    document.getElementById('feed-status').textContent = 'Disconnected';
    return;
  }

  const wlId = document.getElementById('feed-watchlist').value;
  const url  = '/api/stream' + (wlId ? `?watchlist_id=${wlId}` : '');
  feedSource = new EventSource(url);

  feedSource.addEventListener('connected', e => {
    const d = JSON.parse(e.data);
    document.getElementById('feed-dot').className      = 'w-2 h-2 rounded-full bg-green-500';
    document.getElementById('feed-status').textContent = d.watchlist
      ? `Streaming: ${d.watchlist} (${d.nodes?.length} nodes)`
      : 'Streaming: all nodes';
  });

  feedSource.addEventListener('heartbeat', e => {
    const d = JSON.parse(e.data);
    const lvl = Math.min(d.water_level || 0, 3);
    const levelColours = ['blue-800','yellow-700','orange-700','red-700'];
    const lvlLabels = ['Normal','Watch','Warning','Danger'];
    addFeedEvent(`
      <div class="flex items-start gap-2 py-1.5 px-2 rounded-lg bg-gray-700/50 feed-event">
        <span class="text-xs bg-${levelColours[lvl]} px-1.5 py-0.5 rounded text-white shrink-0">${lvlLabels[lvl]}</span>
        <span class="font-mono text-xs text-blue-300 shrink-0">${d.node_id}</span>
        <span class="text-xs text-gray-400 flex-1">
          bat: ${d.battery_voltage?.toFixed(2) || '—'}V
          ${d.rssi ? `· rssi: ${d.rssi}` : ''}
        </span>
        <span class="text-xs text-gray-500 shrink-0">${new Date(d.timestamp).toLocaleTimeString()}</span>
      </div>
    `, 'heartbeat');
  });

  feedSource.addEventListener('alert', e => {
    const d = JSON.parse(e.data);
    addFeedEvent(`
      <div class="flex items-start gap-2 py-1.5 px-2 rounded-lg bg-red-950/50 border border-red-900/50 feed-event">
        <span class="text-xs bg-red-700 px-1.5 py-0.5 rounded text-white shrink-0">ALERT</span>
        <span class="font-mono text-xs text-red-300 shrink-0">${d.node_id}</span>
        <span class="text-xs text-gray-300 flex-1">${d.alert_type || 'unknown'}</span>
        <span class="text-xs text-gray-500 shrink-0">${new Date(d.timestamp).toLocaleTimeString()}</span>
      </div>
    `, 'alert');
  });

  feedSource.onerror = () => {
    document.getElementById('feed-dot').className      = 'w-2 h-2 rounded-full bg-yellow-500';
    document.getElementById('feed-status').textContent = 'Reconnecting…';
  };

  document.getElementById('feed-btn').textContent    = '■ Disconnect';
  document.getElementById('feed-btn').className      = 'px-4 py-2 bg-red-700 hover:bg-red-600 rounded-lg text-sm font-medium transition-colors';
}

function addFeedEvent(html, type) {
  feedEvents.unshift({ html, type });
  if (feedEvents.length > MAX_FEED) feedEvents.pop();
  renderFeed();
}

function renderFeed() {
  const list = document.getElementById('feed-list');
  list.innerHTML = feedEvents.map(e => e.html).join('') || '<p class="text-gray-600 text-sm text-center mt-8">No events yet</p>';
  document.getElementById('feed-count').textContent = feedEvents.length;
}

function clearFeed() {
  feedEvents = [];
  renderFeed();
}

// ─── Alerts tab ───────────────────────────────────────────────────────────────
async function loadAlerts() {
  const alerts = await api('/alerts?limit=50');
  const container = document.getElementById('alert-list');

  const typeConfig = {
    flood:             { icon: '🌊', colour: 'border-l-red-500',    bg: 'bg-red-950/30'    },
    battery_low:       { icon: '🔋', colour: 'border-l-yellow-500', bg: 'bg-yellow-950/30' },
    battery_critical:  { icon: '🔋', colour: 'border-l-red-500',    bg: 'bg-red-950/30'    },
    water_fall:        { icon: '📉', colour: 'border-l-blue-500',   bg: 'bg-blue-950/30'   },
    gps_moved:         { icon: '📍', colour: 'border-l-purple-500', bg: 'bg-purple-950/30' },
    gps_signal_lost:   { icon: '📡', colour: 'border-l-purple-500', bg: 'bg-purple-950/30' },
    gps_restored:      { icon: '📡', colour: 'border-l-green-500',  bg: 'bg-green-950/30'  },
  };

  container.innerHTML = alerts.map(a => {
    const cfg = typeConfig[a.alert_type] || { icon: '⚠️', colour: 'border-l-gray-500', bg: 'bg-gray-800' };
    return `
      <div class="rounded-lg border-l-4 ${cfg.colour} ${cfg.bg} px-4 py-3 flex items-start gap-3">
        <span class="text-lg shrink-0">${cfg.icon}</span>
        <div class="flex-1 min-w-0">
          <div class="flex items-center gap-2 flex-wrap">
            <span class="font-semibold text-sm">${a.alert_type}</span>
            <span class="font-mono text-xs text-gray-400">${a.node_id}</span>
            <span class="text-xs text-gray-500">${a.village_id || ''}</span>
          </div>
          ${a.level !== undefined && a.level !== null ? `<p class="text-xs text-gray-400 mt-0.5">Level: ${LEVEL_LABELS[a.level] || a.level}</p>` : ''}
          ${a.dist_m ? `<p class="text-xs text-gray-400 mt-0.5">Moved ${a.dist_m}m from install position</p>` : ''}
        </div>
        <span class="text-xs text-gray-500 shrink-0">${timeAgo(a.timestamp)}</span>
      </div>`;
  }).join('') || '<p class="text-gray-500 text-sm">No alerts found.</p>';
}

// ─── Init ─────────────────────────────────────────────────────────────────────
(async function init() {
  await checkHealth();
  await loadDashboard();
  setInterval(checkHealth, 30000);
  setInterval(async () => {
    if (document.getElementById('tab-dashboard').classList.contains('hidden')) return;
    await loadDashboard();
  }, 15000);  // refresh dashboard every 15s
})();
</script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse, tags=["dashboard"])
def dashboard():
    """Serve the FloodWatch Monitor single-page dashboard."""
    return DASHBOARD_HTML
