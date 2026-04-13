#!/usr/bin/env node
/**
 * GeoFleet VPS Worker — Live Vehicle Sync
 * 
 * Haalt elke 30 seconden live posities op van GeoFleet API
 * en pusht ze naar de Supabase database (geofleet_cache + geofleet_history).
 * 
 * SETUP:
 *   1. npm init -y && npm install node-fetch@3
 *   2. Kopieer dit bestand naar je VPS
 *   3. Maak een .env bestand (zie onder)
 *   4. node geofleet-worker.js
 * 
 * .env bestand:
 *   GEOFLEET_API_KEY=979B5133-E4DD-4A3D-8813-71798E27F364
 *   SUPABASE_URL=https://prialehwcyzjlrnxancs.supabase.co
 *   SUPABASE_SERVICE_KEY=<jouw service role key>
 *   SYNC_INTERVAL_MS=30000
 */

// ─── Config ───
const GEOFLEET_BASE = 'https://secure.geofleet.eu/geoapi/v2.0';
const GEOFLEET_API_KEY = process.env.GEOFLEET_API_KEY;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;
const SYNC_INTERVAL = parseInt(process.env.SYNC_INTERVAL_MS || '30000');

if (!GEOFLEET_API_KEY || !SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
  console.error('❌ Ontbrekende environment variabelen. Stel in:');
  console.error('   GEOFLEET_API_KEY, SUPABASE_URL, SUPABASE_SERVICE_KEY');
  process.exit(1);
}

const log = (msg) => console.log(`[${new Date().toISOString()}] ${msg}`);
const logErr = (msg) => console.error(`[${new Date().toISOString()}] ❌ ${msg}`);

// ─── GeoFleet API ───
async function fetchGeoFleetObjects() {
  const res = await fetch(`${GEOFLEET_BASE}/read`, {
    headers: { 'x-api-key': GEOFLEET_API_KEY, 'Accept': 'application/json' },
  });
  if (!res.ok) throw new Error(`GeoFleet API ${res.status}: ${await res.text()}`);
  const data = await res.json();
  return data.response?.object || data.objects || [];
}

async function fetchTripReport(idcode, date) {
  const fromDate = date.replace(/-/g, '/');
  const url = `${GEOFLEET_BASE}/reports for objects/trip?id=${idcode}&from=${encodeURIComponent(fromDate)}&fromtime=00%3A00%3A00&to=${encodeURIComponent(fromDate)}&totime=23%3A59%3A59&stationary=true&sensors=true`;
  const res = await fetch(url, {
    headers: { 'x-api-key': GEOFLEET_API_KEY, 'Accept': 'application/json' },
  });
  if (!res.ok) throw new Error(`Trip API ${res.status}`);
  return res.json();
}

// ─── Supabase ───
async function supabasePost(table, data, upsert = false) {
  const headers = {
    'apikey': SUPABASE_SERVICE_KEY,
    'Authorization': `Bearer ${SUPABASE_SERVICE_KEY}`,
    'Content-Type': 'application/json',
  };
  if (upsert) headers['Prefer'] = 'resolution=merge-duplicates';
  const res = await fetch(`${SUPABASE_URL}/rest/v1/${table}`, {
    method: 'POST', headers, body: JSON.stringify(data),
  });
  if (!res.ok) {
    const err = await res.text();
    throw new Error(`Supabase ${table} error ${res.status}: ${err}`);
  }
  return res;
}

// ─── Transform GeoFleet object to DB row ───
function transformObject(o) {
  const info = o.information || {};
  const pos = o.position || {};
  const io = o.io?.digInput || {};
  
  let positieTijd = new Date().toISOString();
  if (pos.dateTime) {
    const dt = pos.dateTime;
    const y = dt.slice(0,4), mo = dt.slice(4,6), d = dt.slice(6,8);
    const h = dt.slice(9,11), mi = dt.slice(11,13), s = dt.slice(13,15);
    positieTijd = `${y}-${mo}-${d}T${h}:${mi}:${s}Z`;
  }

  return {
    idcode: o.idcode,
    naam: o.name || null,
    nummerplaat: info.numberPlate || null,
    merk: info.brand || null,
    type: info.serie || null,
    latitude: parseFloat(pos.lat) || 0,
    longitude: parseFloat(pos.lng) || 0,
    snelheid: pos.speed || 0,
    richting: pos.heading || 0,
    contact_aan: io.ignition ?? false,
    adres: pos.address || pos.place || null,
    positie_tijd: positieTijd,
    updated_at: new Date().toISOString(),
  };
}

// ─── Main sync loop ───
let syncCount = 0;
let errorCount = 0;

async function syncPositions() {
  try {
    const objects = await fetchGeoFleetObjects();
    if (!Array.isArray(objects) || objects.length === 0) {
      log('⚠️  Geen objecten ontvangen van GeoFleet');
      return;
    }

    const rows = objects.map(transformObject);

    // Upsert cache
    await supabasePost('geofleet_cache', rows, true);

    // Insert history (skip updated_at field)
    const historyRows = rows.map(({ updated_at, ...rest }) => rest);
    await supabasePost('geofleet_history', historyRows);

    syncCount++;
    const rijdend = rows.filter(r => r.snelheid > 0).length;
    const contactAan = rows.filter(r => r.contact_aan).length;
    log(`✅ Sync #${syncCount} — ${rows.length} voertuigen (${rijdend} rijdend, ${contactAan} contact aan)`);
    errorCount = 0; // Reset error counter on success
  } catch (err) {
    errorCount++;
    logErr(`Sync mislukt (${errorCount}x): ${err.message}`);
    if (errorCount >= 10) {
      logErr('⛔ 10 opeenvolgende fouten — wacht 5 minuten');
      await new Promise(r => setTimeout(r, 300000));
      errorCount = 0;
    }
  }
}

// ─── Trip report endpoint (optional HTTP server) ───
async function handleTripRequest(idcode, date) {
  try {
    const data = await fetchTripReport(idcode, date);
    return { success: true, data };
  } catch (err) {
    return { success: false, error: err.message };
  }
}

// ─── Start ───
log('🚀 GeoFleet VPS Worker gestart');
log(`   API Key: ${GEOFLEET_API_KEY.slice(0, 8)}...`);
log(`   Supabase: ${SUPABASE_URL}`);
log(`   Interval: ${SYNC_INTERVAL / 1000}s`);
log('');

// Initial sync
syncPositions();

// Periodic sync
setInterval(syncPositions, SYNC_INTERVAL);

// Optional: HTTP API for on-demand trip reports (port 3847)
const PORT = parseInt(process.env.PORT || '3847');
const http = await import('node:http');
const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://localhost:${PORT}`);
  
  res.setHeader('Content-Type', 'application/json');
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Headers', 'content-type');
  
  if (req.method === 'OPTIONS') { res.writeHead(200); res.end(); return; }
  
  if (url.pathname === '/status') {
    res.end(JSON.stringify({ running: true, syncCount, errorCount, uptime: process.uptime() }));
    return;
  }
  
  if (url.pathname === '/trip') {
    const idcode = url.searchParams.get('idcode');
    const date = url.searchParams.get('datum');
    if (!idcode || !date) {
      res.writeHead(400);
      res.end(JSON.stringify({ error: 'idcode en datum parameters vereist' }));
      return;
    }
    const result = await handleTripRequest(idcode, date);
    res.end(JSON.stringify(result));
    return;
  }
  
  if (url.pathname === '/sync') {
    await syncPositions();
    res.end(JSON.stringify({ synced: true, syncCount }));
    return;
  }
  
  res.writeHead(404);
  res.end(JSON.stringify({ error: 'Niet gevonden. Gebruik /status, /sync, of /trip?idcode=X&datum=YYYY-MM-DD' }));
});

server.listen(PORT, () => {
  log(`📡 HTTP API beschikbaar op http://localhost:${PORT}`);
  log(`   GET /status — Worker status`);
  log(`   GET /sync   — Forceer sync`);
  log(`   GET /trip?idcode=X&datum=YYYY-MM-DD — Haal ritten op`);
});

// Graceful shutdown
process.on('SIGINT', () => { log('👋 Worker gestopt'); process.exit(0); });
process.on('SIGTERM', () => { log('👋 Worker gestopt'); process.exit(0); });
