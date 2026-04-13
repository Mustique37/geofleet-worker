#!/usr/bin/env node
/**
 * GeoFleet VPS Worker — Live Vehicle Sync v2
 * Uses raw HTTPS (HTTP/1.1) to work with GeoFleet's ASP.NET server
 */

import https from 'node:https';
import http from 'node:http';

const GEOFLEET_HOST = 'secure.geofleet.eu';
const GEOFLEET_API_KEY = process.env.GEOFLEET_API_KEY;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;
const SYNC_INTERVAL = parseInt(process.env.SYNC_INTERVAL_MS || '30000');

if (!GEOFLEET_API_KEY || !SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
  console.error('❌ Missing env vars: GEOFLEET_API_KEY, SUPABASE_URL, SUPABASE_SERVICE_KEY');
  process.exit(1);
}

const log = (msg) => console.log(`[${new Date().toISOString()}] ${msg}`);
const logErr = (msg) => console.error(`[${new Date().toISOString()}] ❌ ${msg}`);

let lastError = null;
let lastSyncTime = null;
let syncCount = 0;
let errorCount = 0;

// ─── GeoFleet via raw HTTPS (HTTP/1.1) ───
function geofleetRequest(path) {
  return new Promise((resolve, reject) => {
    const options = {
      hostname: GEOFLEET_HOST,
      port: 443,
      path: path,
      method: 'GET',
      headers: {
        'x-api-key': GEOFLEET_API_KEY,
        'Accept': 'application/json',
        'Connection': 'close',
      },
      // Force HTTP/1.1
      agent: new https.Agent({ maxVersion: 'TLSv1.3', minVersion: 'TLSv1.2' }),
    };

    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', (chunk) => data += chunk);
      res.on('end', () => {
        if (res.statusCode !== 200) {
          reject(new Error(`GeoFleet ${res.statusCode}: ${data.substring(0, 200)}`));
          return;
        }
        try {
          resolve(JSON.parse(data));
        } catch (e) {
          reject(new Error(`GeoFleet parse error: ${data.substring(0, 200)}`));
        }
      });
    });

    req.on('error', (err) => reject(new Error(`GeoFleet connection: ${err.message}`)));
    req.setTimeout(15000, () => { req.destroy(); reject(new Error('GeoFleet timeout')); });
    req.end();
  });
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
    throw new Error(`Supabase ${table} ${res.status}: ${err.substring(0, 300)}`);
  }
  return res;
}

// ─── Transform ───
function transformObject(o) {
  const info = o.information || {};
  const pos = o.position || {};
  const io = o.io?.digInput || {};

  let positieTijd = new Date().toISOString();
  if (pos.dateTime) {
    const dt = pos.dateTime;
    positieTijd = `${dt.slice(0,4)}-${dt.slice(4,6)}-${dt.slice(6,8)}T${dt.slice(9,11)}:${dt.slice(11,13)}:${dt.slice(13,15)}Z`;
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

// ─── Sync ───
async function syncPositions() {
  try {
    log('🔄 Sync starten...');
    const data = await geofleetRequest('/geoapi/v2.0/read');
    const objects = data.response?.object || data.objects || [];
    log(`📡 GeoFleet: ${objects.length} objecten`);

    if (!Array.isArray(objects) || objects.length === 0) {
      log('⚠️  Geen objecten ontvangen');
      return;
    }

    const rows = objects.map(transformObject);
    await supabasePost('geofleet_cache', rows, true);

    const historyRows = rows.map(({ updated_at, ...rest }) => rest);
    await supabasePost('geofleet_history', historyRows);

    syncCount++;
    lastSyncTime = new Date().toISOString();
    lastError = null;
    errorCount = 0;
    const rijdend = rows.filter(r => r.snelheid > 0).length;
    log(`✅ Sync #${syncCount} — ${rows.length} voertuigen (${rijdend} rijdend)`);
  } catch (err) {
    errorCount++;
    lastError = { message: err.message, time: new Date().toISOString(), count: errorCount };
    logErr(`Sync mislukt (${errorCount}x): ${err.message}`);
    if (errorCount >= 10) {
      logErr('⛔ 10 errors — wacht 5 min');
      await new Promise(r => setTimeout(r, 300000));
      errorCount = 0;
    }
  }
}

// ─── Start ───
log('🚀 GeoFleet Worker v2 gestart');
log(`   Supabase: ${SUPABASE_URL}`);
log(`   Interval: ${SYNC_INTERVAL / 1000}s`);

syncPositions();
setInterval(syncPositions, SYNC_INTERVAL);

// ─── HTTP API ───
const HTTP_PORT = parseInt(process.env.PORT || '3847');
const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://localhost:${HTTP_PORT}`);
  res.setHeader('Content-Type', 'application/json');
  res.setHeader('Access-Control-Allow-Origin', '*');

  if (url.pathname === '/status') {
    res.end(JSON.stringify({ running: true, syncCount, errorCount, lastError, lastSyncTime, uptime: process.uptime() }));
    return;
  }
  if (url.pathname === '/debug') {
    const results = {};
    try {
      const geoData = await geofleetRequest('/geoapi/v2.0/read');
      const objs = geoData.response?.object || geoData.objects || [];
      results.geofleet = { ok: true, objects: objs.length };
    } catch (e) { results.geofleet = { ok: false, error: e.message }; }
    try {
      const sbRes = await fetch(`${SUPABASE_URL}/rest/v1/geofleet_cache?select=idcode&limit=1`, {
        headers: { 'apikey': SUPABASE_SERVICE_KEY, 'Authorization': `Bearer ${SUPABASE_SERVICE_KEY}` },
      });
      results.supabase = { ok: sbRes.ok, status: sbRes.status, body: (await sbRes.text()).substring(0, 200) };
    } catch (e) { results.supabase = { ok: false, error: e.message }; }
    res.end(JSON.stringify(results, null, 2));
    return;
  }
  if (url.pathname === '/sync') {
    await syncPositions();
    res.end(JSON.stringify({ synced: true, syncCount, errorCount, lastError }));
    return;
  }
  if (url.pathname === '/trip') {
    const idcode = url.searchParams.get('idcode');
    const date = url.searchParams.get('datum');
    if (!idcode || !date) { res.writeHead(400); res.end(JSON.stringify({ error: 'idcode + datum vereist' })); return; }
    try {
      const data = await geofleetRequest(`/geoapi/v2.0/reports%20for%20objects/trip?id=${idcode}&from=${encodeURIComponent(date.replace(/-/g, '/'))}&fromtime=00%3A00%3A00&to=${encodeURIComponent(date.replace(/-/g, '/'))}&totime=23%3A59%3A59&stationary=true&sensors=true`);
      res.end(JSON.stringify({ success: true, data }));
    } catch (e) { res.end(JSON.stringify({ success: false, error: e.message })); }
    return;
  }
  res.writeHead(404);
  res.end(JSON.stringify({ routes: ['/status', '/debug', '/sync', '/trip'] }));
});

server.listen(HTTP_PORT, () => log(`📡 HTTP op poort ${HTTP_PORT}`));
process.on('SIGINT', () => process.exit(0));
process.on('SIGTERM', () => process.exit(0));
