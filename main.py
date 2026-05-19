#!/usr/bin/env python3
"""
FloodWatch Monitor — Portfolio 3 (Render Deployment)
SWE40006 Software Deployment and Evolution

Connects to MongoDB Atlas and provides:
  - Dashboard: reads villages and river_nodes collections (DB reads)
  - Watchlists: create/delete named node groups (DB writes)

REST endpoints:
  GET    /                     → Dashboard HTML
  GET    /api/health           → Health check + DB status
  GET    /api/villages         → All villages
  GET    /api/nodes            → All river nodes (?village_id, ?status)
  POST   /api/watchlists       → Create watchlist {name, node_ids[]}
  GET    /api/watchlists       → List all watchlists
  DELETE /api/watchlists/{id}  → Delete a watchlist
"""

import logging
import os
from datetime import datetime, timezone

from bson import ObjectId
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pymongo import MongoClient, DESCENDING
from pydantic import BaseModel

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("floodwatch-monitor")

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB  = os.getenv("MONGO_DB", "flood_monitor")

if not MONGO_URI:
    raise RuntimeError("MONGO_URI environment variable is not set")

mongo          = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
db             = mongo[MONGO_DB]
col_villages   = db["villages"]
col_rivers     = db["river_nodes"]
col_watchlists = db["watchlists"]

log.info(f"Connected to MongoDB Atlas — {MONGO_DB}")

_FILTER = {}  # include all documents (real + sample)

def _clean(value):
    if isinstance(value, dict):
        return {("id" if k == "_id" else k): _clean(v) for k, v in value.items()}
    if isinstance(value, list):    return [_clean(v) for v in value]
    if isinstance(value, datetime): return value.isoformat()
    if isinstance(value, ObjectId): return str(value)
    return value

app = FastAPI(title="FloodWatch Monitor", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["GET","POST","DELETE"], allow_headers=["*"])

@app.get("/api/health", tags=["health"])
def health():
    try:
        mongo.admin.command("ping")
        db_status = "connected"
    except Exception as e:
        db_status = f"error: {e}"
    return {"status": "ok", "service": "floodwatch-monitor", "version": "1.0.0", "database": db_status}

@app.get("/api/villages", tags=["data"])
def list_villages():
    return [_clean(d) for d in col_villages.find(_FILTER, {"topology": 0, "weather_forecast": 0})]

@app.get("/api/nodes", tags=["data"])
def list_nodes(
    village_id: str = Query(default=None),
    status:     str = Query(default=None),
):
    query: dict = {**_FILTER}
    if village_id: query["village_id"] = village_id
    if status:     query["status"]     = status
    return [_clean(d) for d in col_rivers.find(query)]

class WatchlistCreate(BaseModel):
    name:     str
    node_ids: list[str]

@app.post("/api/watchlists", status_code=201, tags=["watchlists"])
def create_watchlist(body: WatchlistCreate):
    name = body.name.strip()
    if not name:           raise HTTPException(400, "Watchlist name cannot be empty")
    if not body.node_ids:  raise HTTPException(400, "At least one node_id is required")
    valid = {d["node_id"] for d in col_rivers.find({"node_id": {"$in": body.node_ids}}, {"node_id": 1})}
    invalid = [n for n in body.node_ids if n not in valid]
    if invalid: raise HTTPException(400, f"Unknown node IDs: {invalid}")
    doc = {"name": name, "node_ids": body.node_ids, "created_at": datetime.now(timezone.utc)}
    result = col_watchlists.insert_one(doc)
    log.info(f"Watchlist created: '{name}'")
    return _clean({**doc, "_id": result.inserted_id})

@app.get("/api/watchlists", tags=["watchlists"])
def list_watchlists():
    return [_clean(d) for d in col_watchlists.find().sort("created_at", DESCENDING)]

@app.delete("/api/watchlists/{watchlist_id}", tags=["watchlists"])
def delete_watchlist(watchlist_id: str):
    try:   oid = ObjectId(watchlist_id)
    except: raise HTTPException(400, "Invalid watchlist ID")
    if col_watchlists.delete_one({"_id": oid}).deleted_count == 0:
        raise HTTPException(404, "Watchlist not found")
    log.info(f"Watchlist deleted: {watchlist_id}")
    return {"deleted": watchlist_id}

DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>FloodWatch Monitor</title>
<script src="https://cdn.tailwindcss.com"></script>
<style>
  body { font-family: system-ui, -apple-system, sans-serif; }
  .tab-btn { padding: 8px 16px; border-radius: 8px 8px 0 0; font-size: 0.875rem; font-weight: 500; transition: all 0.15s; }
  .tab-btn.active { background: #1e293b; color: #fff; }
  .tab-btn:not(.active) { color: #94a3b8; }
  .tab-btn:not(.active):hover { color: #fff; background: #1e293b99; }
  .level-0 { background:#1e3a8a22; border-color:#1e3a8a; }
  .level-1 { background:#78350f22; border-color:#d97706; }
  .level-2 { background:#7c2d1222; border-color:#ea580c; }
  .level-3 { background:#7f1d1d22; border-color:#dc2626; }
  ::-webkit-scrollbar { width: 5px; }
  ::-webkit-scrollbar-track { background: #1e293b; }
  ::-webkit-scrollbar-thumb { background: #475569; border-radius: 3px; }
</style>
</head>
<body class="bg-slate-900 text-slate-100 min-h-screen flex flex-col">

<header class="bg-slate-800 border-b border-slate-700 px-6 py-3 flex items-center justify-between shrink-0">
  <div class="flex items-center gap-3">
    <div class="w-8 h-8 rounded-lg bg-blue-600 flex items-center justify-center text-lg">🌊</div>
    <div>
      <h1 class="font-bold text-base">FloodWatch Monitor</h1>
      <p class="text-xs text-slate-400">IoT Flood Sensor Dashboard · Portfolio 3 — Render Deployment · SWE40006</p>
    </div>
  </div>
  <div class="flex items-center gap-3">
    <div class="flex items-center gap-1.5 text-xs">
      <span id="db-dot" class="w-2 h-2 rounded-full bg-slate-500 shrink-0"></span>
      <span id="db-status" class="text-slate-400">Checking…</span>
    </div>
    <a href="/docs" target="_blank" class="text-xs text-blue-400 hover:text-blue-300 border border-blue-900 rounded px-2 py-1">API Docs ↗</a>
  </div>
</header>

<div class="bg-slate-800 border-b border-slate-700 px-6 py-2 flex gap-6 text-sm shrink-0">
  <span class="text-slate-400">Villages: <strong id="s-villages" class="text-white">—</strong></span>
  <span class="text-slate-400">Nodes: <strong id="s-nodes" class="text-white">—</strong></span>
  <span class="text-slate-400">Online: <strong id="s-online" class="text-green-400">—</strong></span>
  <span class="text-slate-400">Offline: <strong id="s-offline" class="text-red-400">—</strong></span>
  <span class="text-slate-400">Watchlists: <strong id="s-watchlists" class="text-blue-400">—</strong></span>
</div>

<div class="bg-slate-900 border-b border-slate-700 px-6 flex gap-1 pt-2 shrink-0">
  <button class="tab-btn active" onclick="showTab('dashboard')">📊 Dashboard</button>
  <button class="tab-btn"        onclick="showTab('watchlists')">👁 Watchlists</button>
</div>

<!-- Dashboard Tab -->
<div id="tab-dashboard" class="flex-1 flex overflow-hidden p-4 gap-4">
  <div class="w-48 shrink-0 flex flex-col gap-1">
    <p class="text-xs text-slate-500 font-semibold uppercase tracking-wider px-1 mb-1">Villages</p>
    <div id="village-list" class="flex flex-col gap-1 overflow-y-auto">
      <p class="text-slate-500 text-sm px-2">Loading…</p>
    </div>
  </div>
  <div class="flex-1 flex flex-col overflow-hidden">
    <div class="flex items-center justify-between mb-3 shrink-0">
      <p class="text-xs text-slate-500 font-semibold uppercase tracking-wider">
        Nodes — <span id="village-label" class="text-slate-300 normal-case font-normal">All Villages</span>
      </p>
      <div class="flex gap-1.5">
        <button onclick="setFilter('all')"     class="text-xs px-2 py-1 rounded bg-slate-700 hover:bg-slate-600 text-slate-300">All</button>
        <button onclick="setFilter('online')"  class="text-xs px-2 py-1 rounded bg-green-900 hover:bg-green-800 text-green-300">Online</button>
        <button onclick="setFilter('offline')" class="text-xs px-2 py-1 rounded bg-red-900 hover:bg-red-800 text-red-300">Offline</button>
      </div>
    </div>
    <div id="node-grid" class="flex-1 overflow-y-auto grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-3 content-start">
      <p class="col-span-4 text-slate-500 text-sm">Loading…</p>
    </div>
  </div>
</div>

<!-- Watchlists Tab -->
<div id="tab-watchlists" class="hidden flex-1 overflow-y-auto p-6">
  <div class="max-w-2xl mx-auto flex flex-col gap-6">

    <div class="bg-slate-800 rounded-xl p-5 border border-slate-700">
      <h2 class="font-semibold text-blue-300 mb-1">Create a Watchlist</h2>
      <p class="text-xs text-slate-400 mb-4">
        Group sensor nodes by name. Each watchlist is saved as a document in the
        <code class="bg-slate-700 px-1 rounded text-slate-300">watchlists</code> collection
        in MongoDB Atlas, demonstrating persistent database writes.
      </p>
      <div class="flex gap-2 mb-4">
        <input id="wl-name" type="text" placeholder="Watchlist name…"
               class="flex-1 bg-slate-700 border border-slate-600 rounded-lg px-3 py-2 text-sm
                      text-white placeholder-slate-500 focus:outline-none focus:border-blue-500">
        <button onclick="createWatchlist()"
                class="px-4 py-2 bg-blue-600 hover:bg-blue-500 rounded-lg text-sm font-medium transition-colors">
          Save
        </button>
      </div>
      <p class="text-xs text-slate-400 font-medium mb-2">Select nodes to include:</p>
      <div id="node-checkboxes"
           class="grid grid-cols-2 gap-1 max-h-56 overflow-y-auto border border-slate-700 rounded-lg p-2 bg-slate-900">
        <p class="text-slate-500 text-xs">Loading nodes…</p>
      </div>
      <p id="wl-error"   class="hidden text-red-400 text-xs mt-2"></p>
      <p id="wl-success" class="hidden text-green-400 text-xs mt-2"></p>
    </div>

    <div>
      <div class="flex items-center justify-between mb-3">
        <h2 class="font-semibold text-blue-300">Saved Watchlists</h2>
        <button onclick="loadWatchlistsTab()"
                class="text-xs text-slate-400 hover:text-white border border-slate-700 rounded px-2 py-1">
          Refresh
        </button>
      </div>
      <div id="watchlist-cards" class="flex flex-col gap-3">
        <p class="text-slate-500 text-sm">Loading…</p>
      </div>
    </div>

  </div>
</div>

<script>
let allNodes = [], selectedVillage = null, statusFilter = 'all';

const LEVEL_LABEL  = ['Normal','Watch','Warning','Danger'];
const LEVEL_COLOUR = ['bg-blue-800 text-blue-200','bg-yellow-700 text-yellow-100','bg-orange-700 text-orange-100','bg-red-700 text-red-100'];

function showTab(name) {
  ['dashboard','watchlists'].forEach(t => {
    document.getElementById('tab-'+t).classList.add('hidden');
    document.getElementById('tab-'+t).classList.remove('flex');
  });
  document.getElementById('tab-'+name).classList.remove('hidden');
  document.getElementById('tab-'+name).classList.add('flex');
  document.querySelectorAll('.tab-btn').forEach((b,i) =>
    b.classList.toggle('active', ['dashboard','watchlists'][i] === name));
  if (name === 'watchlists') loadWatchlistsTab();
}

function timeAgo(iso) {
  if (!iso) return '—';
  const s = Math.floor((Date.now() - new Date(iso)) / 1000);
  if (s < 60) return s+'s ago';
  if (s < 3600) return Math.floor(s/60)+'m ago';
  return Math.floor(s/3600)+'h ago';
}

async function api(path) {
  const r = await fetch('/api'+path);
  if (!r.ok) throw new Error('API error '+r.status);
  return r.json();
}

async function checkHealth() {
  try {
    const h = await api('/health');
    const ok = h.database === 'connected';
    document.getElementById('db-dot').className = `w-2 h-2 rounded-full shrink-0 ${ok?'bg-green-500':'bg-red-500'}`;
    document.getElementById('db-status').textContent = ok ? 'MongoDB Atlas connected' : 'DB error';
  } catch {}
}

async function loadDashboard() {
  const [villages, nodes, watchlists] = await Promise.all([api('/villages'), api('/nodes'), api('/watchlists')]);
  allNodes = nodes;
  document.getElementById('s-villages').textContent  = villages.length;
  document.getElementById('s-nodes').textContent     = nodes.length;
  document.getElementById('s-online').textContent    = nodes.filter(n=>n.status==='online').length;
  document.getElementById('s-offline').textContent   = nodes.filter(n=>n.status==='offline').length;
  document.getElementById('s-watchlists').textContent = watchlists.length;

  const all = [{village_id:null, name:'All Villages'}, ...villages];
  document.getElementById('village-list').innerHTML = all.map(v => {
    const total  = v.village_id ? nodes.filter(n=>n.village_id===v.village_id).length : nodes.length;
    const online = v.village_id ? nodes.filter(n=>n.village_id===v.village_id&&n.status==='online').length : nodes.filter(n=>n.status==='online').length;
    const active = selectedVillage === v.village_id;
    const dot    = v.village_id ? `<span class="w-1.5 h-1.5 rounded-full mt-1 shrink-0 ${v.status==='online'?'bg-green-500':'bg-slate-500'}"></span>` : '<span class="w-1.5 h-1.5 mt-1 shrink-0"></span>';
    return `<button onclick="selectVillage(${JSON.stringify(v.village_id)})"
      class="text-left px-2 py-2 rounded-lg text-sm flex items-start gap-2 transition-colors ${active?'bg-blue-800 text-white':'text-slate-300 hover:bg-slate-800'}">
      ${dot}<span><span class="block font-medium leading-tight truncate">${v.name||v.village_id}</span><span class="text-xs text-slate-400">${online}/${total} online</span></span>
    </button>`;
  }).join('');
  renderNodeGrid();
}

function selectVillage(vid) {
  selectedVillage = vid;
  document.getElementById('village-label').textContent = vid || 'All Villages';
  renderNodeGrid();
}

function setFilter(f) { statusFilter = f; renderNodeGrid(); }

function renderNodeGrid() {
  let nodes = selectedVillage ? allNodes.filter(n=>n.village_id===selectedVillage) : allNodes;
  if (statusFilter !== 'all') nodes = nodes.filter(n=>n.status===statusFilter);
  const grid = document.getElementById('node-grid');
  if (!nodes.length) { grid.innerHTML = '<p class="col-span-4 text-slate-500 text-sm">No nodes match this filter.</p>'; return; }
  grid.innerHTML = nodes.map(n => {
    const lvl = Math.min(n.water_level||0, 3);
    return `<div class="rounded-xl p-3 border level-${lvl} ${n.status==='offline'?'opacity-50':''} flex flex-col gap-1.5">
      <div class="flex items-start justify-between gap-1">
        <span class="font-mono text-xs text-slate-400 break-all leading-tight">${n.node_id}</span>
        <span class="text-xs shrink-0 ${n.status==='online'?'text-green-400':'text-slate-600'}">${n.status==='online'?'●':'○'}</span>
      </div>
      <span class="text-xs px-2 py-0.5 rounded-full font-semibold w-fit ${LEVEL_COLOUR[lvl]}">${LEVEL_LABEL[lvl]}</span>
      <div class="text-xs text-slate-400 flex flex-col gap-0.5">
        <span>🔋 ${n.battery_voltage?n.battery_voltage.toFixed(2)+'V':'—'}</span>
        ${n.depth!==undefined?`<span>Depth: ${n.depth}</span>`:''}
        ${n.gps_fix?'<span>📍 GPS fix</span>':''}
        <span class="text-slate-600">${timeAgo(n.last_seen)}</span>
      </div>
    </div>`;
  }).join('');
}

async function loadWatchlistsTab() {
  const [nodes, watchlists] = await Promise.all([api('/nodes'), api('/watchlists')]);
  allNodes = nodes;
  document.getElementById('s-watchlists').textContent = watchlists.length;
  document.getElementById('node-checkboxes').innerHTML = nodes.map(n =>
    `<label class="flex items-center gap-2 cursor-pointer hover:text-white py-0.5 px-1 rounded hover:bg-slate-800">
      <input type="checkbox" value="${n.node_id}" class="node-cb accent-blue-500 shrink-0">
      <span class="font-mono text-xs truncate">${n.node_id}</span>
      <span class="text-slate-500 text-xs shrink-0">${n.village_id||''}</span>
    </label>`).join('');
  const c = document.getElementById('watchlist-cards');
  if (!watchlists.length) { c.innerHTML = '<p class="text-slate-500 text-sm">No watchlists yet — create one above.</p>'; return; }
  c.innerHTML = watchlists.map(wl =>
    `<div class="bg-slate-800 rounded-xl p-4 border border-slate-700 flex items-start justify-between gap-3">
      <div class="flex-1 min-w-0">
        <p class="font-medium">${wl.name}</p>
        <p class="text-xs text-slate-400 mt-0.5">${wl.node_ids.length} node${wl.node_ids.length!==1?'s':''} · saved ${timeAgo(wl.created_at)}</p>
        <div class="flex flex-wrap gap-1 mt-2">
          ${wl.node_ids.map(id=>`<span class="text-xs bg-slate-700 rounded px-1.5 py-0.5 font-mono text-slate-300">${id}</span>`).join('')}
        </div>
      </div>
      <button onclick="deleteWatchlist('${wl.id}')"
              class="text-red-400 hover:text-red-300 text-xs px-2 py-1 rounded bg-slate-700 hover:bg-slate-600 transition-colors shrink-0">
        Delete
      </button>
    </div>`).join('');
}

async function createWatchlist() {
  const name    = document.getElementById('wl-name').value.trim();
  const nodeIds = [...document.querySelectorAll('.node-cb:checked')].map(cb=>cb.value);
  const errEl   = document.getElementById('wl-error');
  const okEl    = document.getElementById('wl-success');
  errEl.classList.add('hidden'); okEl.classList.add('hidden');
  if (!name)           { errEl.textContent='Please enter a watchlist name.'; errEl.classList.remove('hidden'); return; }
  if (!nodeIds.length) { errEl.textContent='Select at least one node.';      errEl.classList.remove('hidden'); return; }
  try {
    const r = await fetch('/api/watchlists', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({name, node_ids:nodeIds})});
    if (!r.ok) { const e=await r.json(); throw new Error(e.detail||'Server error'); }
    document.getElementById('wl-name').value='';
    document.querySelectorAll('.node-cb').forEach(cb=>cb.checked=false);
    okEl.textContent=`✓ Watchlist "${name}" saved with ${nodeIds.length} node(s).`;
    okEl.classList.remove('hidden');
    await loadWatchlistsTab();
  } catch(e) { errEl.textContent=e.message; errEl.classList.remove('hidden'); }
}

async function deleteWatchlist(id) {
  if (!confirm('Delete this watchlist?')) return;
  await fetch(`/api/watchlists/${id}`, {method:'DELETE'});
  await loadWatchlistsTab();
}

(async function init() {
  await checkHealth();
  await loadDashboard();
  setInterval(checkHealth,   30000);
  setInterval(loadDashboard, 15000);
})();
</script>
</body>
</html>
"""

@app.get("/", response_class=HTMLResponse, tags=["dashboard"])
def dashboard():
    return DASHBOARD_HTML