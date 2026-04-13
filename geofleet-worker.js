#!/usr/bin/env node
/**
 * GeoFleet VPS Worker v3
 * Uses raw TLS socket (HTTP/1.1 forced) — same approach as edge function
 */

import * as tls from 'node:tls';
import * as net from 'node:net';
import http from 'node:http';

const GEOFLEET_HOST = 'dev.geofleet.eu';
const GEOFLEET_API_KEY = process.env.GEOFLEET_API_KEY;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;
const SYNC_INTERVAL = parseInt(process.env.SYNC_INTERVAL_MS || '30000');

if (!GEOFLEET_API_KEY || !SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
  console.error('❌ Missing: GEOFLEET_API_KEY, SUPABASE_URL, SUPABASE_SERVICE_KEY');
  process.exit(1);
}

const log = (msg) => console.log(`[${new Date().toISOString()}] ${msg}`);
const logErr = (msg) => console.error(`[${new Date().toISOString()}] ❌ ${msg}`);

let lastError = null;
let lastSyncTime = null;
let syncCount = 0;
let errorCount = 0;

// ─── Raw TLS HTTP/1.1 request (forces HTTP/1.1, no ALPN h2) ───
function geofleetRawRequest(path) {
  return new Promise((resolve, reject) => {
    const socket = net.connect({ host: GEOFLEET_HOST, port: 443 }, () => {
      const tlsSocket = tls.connect({
        socket: socket,
        servername: GEOFLEET_HOST,
        ALPNProtocols: ['http/1.1'], // Force HTTP/1.1
      }, () => {
        const request = [
          `GET ${path} HTTP/1.1`,
          `Host: ${GEOFLEET_HOST}`,
          `x-api-key: ${GEOFLEET_API_KEY}`,
          `Accept: application/json`,
          `User-Agent: GeoFleetSync/1.0`,
          `Connection: close`,
          '', ''
        ].join('\r\n');

        tlsSocket.write(request);

        let data = '';
        tlsSocket.on('data', (chunk) => data += chunk.toString());
        tlsSocket.on('end', () => {
          tlsSocket.destroy();
          socket.destroy();

          const headerEnd = data.indexOf('\r\n\r\n');
          if (headerEnd === -1) { reject(new Error('No HTTP headers')); return; }

          const statusLine = data.substring(0, data.indexOf('\r\n'));
          const statusCode = parseInt(statusLine.split(' ')[1]) || 500;
          const headerSection = data.substring(0, headerEnd).toLowerCase();
          let body = data.substring(headerEnd + 4);

          // Handle chunked transfer encoding
          if (headerSection.includes('transfer-encoding: chunked')) {
            let decoded = '', remaining = body;
            while (remaining.length > 0) {
              const le = remaining.indexOf('\r\n');
              if (le === -1) break;
              const cs = parseInt(remaining.substring(0, le), 16);
              if (cs === 0 || isNaN(cs)) break;
              decoded += remaining.substring(le + 2, le + 2 + cs);
              remaining = remaining.substring(le + 2 + cs + 2);
            }
            body = decoded;
          }

          if (statusCode !== 200) {
            reject(new Error(`GeoFleet ${statusCode}: ${body.substring(0, 200)}`));
            return;
          }

          try { resolve(JSON.parse(body)); }
          catch (e) { reject(new Error(`Parse error: ${body.substring(0, 100)}`)); }
        });
        tlsSocket.on('error', (err) => { socket.destroy(); reject(err); });
      });
      tlsSocket.on('error', (err) => { socket.destroy(); reject(err); });
    });
    socket.on('error', (err) => reject(err));
    socket.setTimeout(15000, () => { socket.destroy(); reject(new Error('Timeout')); });
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
    idcode: o.idcode, naam: o.name || null, nummerplaat: info.numberPlate || null,
    merk: info.brand || null, type: info.serie || null,
    latitude: parseFloat(pos.lat) || 0, longitude: parseFloat(pos.lng) || 0,
    snelheid: pos.speed || 0, richting: pos.heading || 0,
    contact_aan: io.ignition ?? false, adres: pos.address || pos.place || null,
    positie_tijd: positieTijd, updated_at: new Date().toISOString(),
  };
}

// ─── Sync ───
async function syncPositions() {
  try {
    log('🔄 Sync...');
    const data = await geofleetRawRequest('/geoapi/v2.0/read');
    const objects = data.response?.object || data.objects || [];
    if (!Array.isArray(objects) || objects.length === 0) { log('⚠️ Geen objecten'); return; }

    const rows = objects.map(transformObject);
    await supabasePost('geofleet_cache', rows, true);
    const historyRows = rows.map(({ updated_at, ...rest }) => rest);
    await supabasePost('geofleet_history', historyRows);

    syncCount++;
    lastSyncTime = new Date().toISOString();
    lastError = null;
    errorCount = 0;
    log(`✅ Sync #${syncCount} — ${rows.length} voertuigen (${rows.filter(r => r.snelheid > 0).length} rijdend)`);
  } catch (err) {
    errorCount++;
    lastError = { message: err.message, time: new Date().toISOString(), count: errorCount };
    logErr(`(${errorCount}x): ${err.message}`);
    if (errorCount >= 10) { await new Promise(r => setTimeout(r, 300000)); errorCount = 0; }
  }
}

// ─── Start ───
log('🚀 GeoFleet Worker v3 (raw TLS)');
syncPositions();
setInterval(syncPositions, SYNC_INTERVAL);

// ─── HTTP API ───
const PORT = parseInt(process.env.PORT || '3847');
http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://localhost:${PORT}`);
  res.setHeader('Content-Type', 'application/json');
  res.setHeader('Access-Control-Allow-Origin', '*');

  if (url.pathname === '/status') {
    res.end(JSON.stringify({ running: true, syncCount, errorCount, lastError, lastSyncTime, uptime: process.uptime() }));
  } else if (url.pathname === '/debug') {
    const r = {};
    try { 
      const d = await geofleetRawRequest('/geoapi/v2.0/read'); 
      r.geofleet = { ok: true, objects: (d.response?.object || d.objects || []).length, keys: Object.keys(d) }; 
    }
    catch (e) { 
      r.geofleet = { ok: false, error: e.message, keyPresent: !!GEOFLEET_API_KEY, keyLength: GEOFLEET_API_KEY?.length }; 
    }
    try { const s = await fetch(`${SUPABASE_URL}/rest/v1/geofleet_cache?select=idcode&limit=1`, { headers: { 'apikey': SUPABASE_SERVICE_KEY, 'Authorization': `Bearer ${SUPABASE_SERVICE_KEY}` } }); r.supabase = { ok: s.ok, status: s.status }; }
    catch (e) { r.supabase = { ok: false, error: e.message }; }
    res.end(JSON.stringify(r, null, 2));
  } else if (url.pathname === '/sync') {
    await syncPositions();
    res.end(JSON.stringify({ synced: true, syncCount, errorCount, lastError }));
  } else if (url.pathname === '/trip') {
    const idcode = url.searchParams.get('idcode'), date = url.searchParams.get('datum');
    if (!idcode || !date) { res.writeHead(400); res.end(JSON.stringify({ error: 'idcode + datum vereist' })); return; }
    try { const d = await geofleetRawRequest(`/geoapi/v2.0/reports%20for%20objects/trip?id=${idcode}&from=${encodeURIComponent(date.replace(/-/g, '/'))}&fromtime=00%3A00%3A00&to=${encodeURIComponent(date.replace(/-/g, '/'))}&totime=23%3A59%3A59&stationary=true&sensors=true`); res.end(JSON.stringify({ success: true, data: d })); }
    catch (e) { res.end(JSON.stringify({ success: false, error: e.message })); }
  } else { res.writeHead(404); res.end(JSON.stringify({ routes: ['/status', '/debug', '/sync', '/trip'] })); }
}).listen(PORT, () => log(`📡 HTTP :${PORT}`));

process.on('SIGINT', () => process.exit(0));
process.on('SIGTERM', () => process.exit(0));
