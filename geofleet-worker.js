/**
 * GeoFleet Worker — Railway deployment
 * Syncs vehicle positions from GeoFleet API to Supabase every 30 seconds.
 * 
 * Environment variables needed:
 *   GEOFLEET_API_KEY       — GeoFleet x-api-key
 *   SUPABASE_URL           — e.g. https://prialehwcyzjlrnxancs.supabase.co
 *   SUPABASE_SERVICE_KEY   — Supabase service_role key
 *   PORT                   — (optional, default 3000)
 */

const http = require('http');
const https = require('https');

const GEOFLEET_HOST = 'secure.geofleet.eu';
const GEOFLEET_PATH = '/geoapi/v2.0/account/objects';
const SYNC_INTERVAL = 30000;

const state = {
  lastSync: null,
  lastError: null,
  vehicleCount: 0,
  syncCount: 0,
  startedAt: new Date().toISOString(),
};

function httpsRequest(url, options) {
  return new Promise((resolve, reject) => {
    const req = https.request(url, options, (res) => {
      let data = '';
      res.on('data', (chunk) => { data += chunk; });
      res.on('end', () => resolve({ status: res.statusCode, body: data }));
    });
    req.on('error', reject);
    req.setTimeout(15000, () => { req.destroy(new Error('Request timeout')); });
    if (options.body) req.write(options.body);
    req.end();
  });
}

async function syncPositions() {
  const apiKey = process.env.GEOFLEET_API_KEY;
  const supabaseUrl = process.env.SUPABASE_URL;
  const serviceKey = process.env.SUPABASE_SERVICE_KEY;

  if (!apiKey || !supabaseUrl || !serviceKey) {
    state.lastError = 'Missing environment variables';
    console.error('[sync] Missing env vars');
    return;
  }

  try {
    console.log(`[sync] Fetching https://${GEOFLEET_HOST}${GEOFLEET_PATH}`);

    const res = await httpsRequest(`https://${GEOFLEET_HOST}${GEOFLEET_PATH}`, {
      method: 'GET',
      headers: {
        'x-api-key': apiKey,
        'Accept': 'application/json',
        'User-Agent': 'GeoFleetSync/2.0',
      },
    });

    console.log(`[sync] Response: ${res.status}, length: ${res.body.length}`);

    if (res.status === 401) {
      state.lastError = `Auth failed (401): ${res.body.substring(0, 200)}`;
      console.error('[sync]', state.lastError);
      return;
    }

    if (res.status >= 400) {
      state.lastError = `HTTP ${res.status}: ${res.body.substring(0, 200)}`;
      console.error('[sync]', state.lastError);
      return;
    }

    const geoData = JSON.parse(res.body);
    const objects = geoData.response?.object || geoData.objects || geoData.keys || [];

    if (!Array.isArray(objects) || objects.length === 0) {
      state.lastError = `No objects. Keys: ${Object.keys(geoData).join(', ')}. Body: ${res.body.substring(0, 300)}`;
      console.warn('[sync]', state.lastError);
      return;
    }

    console.log(`[sync] Got ${objects.length} vehicles`);

    const now = new Date().toISOString();
    const rows = objects.map((o) => {
      const info = o.information || {};
      const pos = o.position || {};
      const io = o.io?.digInput || {};
      let positieTijd = now;
      if (pos.dateTime) {
        const dt = pos.dateTime;
        positieTijd = `${dt.slice(0,4)}-${dt.slice(4,6)}-${dt.slice(6,8)}T${dt.slice(9,11)}:${dt.slice(11,13)}:${dt.slice(13,15)}Z`;
      }
      return {
        idcode: o.idcode, naam: o.name || null,
        nummerplaat: info.numberPlate || null, merk: info.brand || null, type: info.serie || null,
        latitude: parseFloat(pos.lat) || 0, longitude: parseFloat(pos.lng) || 0,
        snelheid: pos.speed || 0, richting: pos.heading || 0,
        contact_aan: io.ignition ?? false,
        adres: pos.address || pos.place || null,
        positie_tijd: positieTijd, updated_at: now,
      };
    });

    // Upsert cache
    const cacheBody = JSON.stringify(rows);
    const cacheRes = await httpsRequest(`${supabaseUrl}/rest/v1/geofleet_cache`, {
      method: 'POST',
      headers: {
        'apikey': serviceKey, 'Authorization': `Bearer ${serviceKey}`,
        'Content-Type': 'application/json', 'Prefer': 'resolution=merge-duplicates',
      },
      body: cacheBody,
    });
    if (cacheRes.status >= 400) console.error('[sync] Cache error:', cacheRes.body.substring(0, 200));

    // Insert history
    const historyRows = rows.map((r) => ({
      idcode: r.idcode, nummerplaat: r.nummerplaat, naam: r.naam,
      merk: r.merk, type: r.type, latitude: r.latitude, longitude: r.longitude,
      snelheid: r.snelheid, richting: r.richting, contact_aan: r.contact_aan,
      adres: r.adres, positie_tijd: r.positie_tijd,
    }));
    const histBody = JSON.stringify(historyRows);
    const histRes = await httpsRequest(`${supabaseUrl}/rest/v1/geofleet_history`, {
      method: 'POST',
      headers: {
        'apikey': serviceKey, 'Authorization': `Bearer ${serviceKey}`,
        'Content-Type': 'application/json',
      },
      body: histBody,
    });
    if (histRes.status >= 400) console.error('[sync] History error:', histRes.body.substring(0, 200));

    state.lastSync = now;
    state.vehicleCount = rows.length;
    state.syncCount++;
    state.lastError = null;
    console.log(`[sync] ✅ ${rows.length} vehicles synced (#${state.syncCount})`);
  } catch (err) {
    state.lastError = err.message || String(err);
    console.error('[sync] Error:', state.lastError);
  }
}

// ─── HTTP health server (starts FIRST for Railway healthcheck) ───
const PORT = process.env.PORT || 3000;

const server = http.createServer((req, res) => {
  res.setHeader('Content-Type', 'application/json');

  if (req.url === '/debug') {
    const apiKey = process.env.GEOFLEET_API_KEY || '';
    res.end(JSON.stringify({
      ...state, uptime: process.uptime(),
      keyPresent: apiKey.length > 0, keyLength: apiKey.length,
      host: GEOFLEET_HOST, path: GEOFLEET_PATH,
      supabaseConfigured: !!(process.env.SUPABASE_URL && process.env.SUPABASE_SERVICE_KEY),
    }));
    return;
  }

  if (req.url === '/sync') {
    syncPositions().then(() => res.end(JSON.stringify({ triggered: true, ...state })));
    return;
  }

  res.end(JSON.stringify({ status: 'running', ...state }));
});

server.listen(PORT, () => {
  console.log(`[worker] ✅ Listening on port ${PORT}`);
  console.log(`[worker] Host: ${GEOFLEET_HOST}, Path: ${GEOFLEET_PATH}`);

  // Start sync AFTER server is listening (so healthcheck passes)
  setTimeout(() => {
    syncPositions();
    setInterval(syncPositions, SYNC_INTERVAL);
  }, 2000);
});
