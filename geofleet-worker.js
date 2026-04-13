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

const GEOFLEET_HOST = 'secure.geofleet.eu';
const GEOFLEET_PATH = '/geoapi/v2.0/account/objects';
const SYNC_INTERVAL = 30_000;

const state = {
  lastSync: null,
  lastError: null,
  vehicleCount: 0,
  syncCount: 0,
  startedAt: new Date().toISOString(),
};

async function syncPositions() {
  const apiKey = process.env.GEOFLEET_API_KEY;
  const supabaseUrl = process.env.SUPABASE_URL;
  const serviceKey = process.env.SUPABASE_SERVICE_KEY;

  if (!apiKey || !supabaseUrl || !serviceKey) {
    console.error('[sync] Missing env vars');
    state.lastError = 'Missing environment variables';
    return;
  }

  try {
    const url = `https://${GEOFLEET_HOST}${GEOFLEET_PATH}`;
    console.log(`[sync] Fetching ${url}`);

    const res = await fetch(url, {
      method: 'GET',
      headers: {
        'x-api-key': apiKey,
        'Accept': 'application/json',
        'User-Agent': 'GeoFleetSync/2.0',
      },
    });

    const body = await res.text();
    console.log(`[sync] Response: ${res.status}, length: ${body.length}`);

    if (res.status === 401) {
      state.lastError = `Auth failed (401): ${body.substring(0, 200)}`;
      console.error('[sync]', state.lastError);
      return;
    }

    if (!res.ok) {
      state.lastError = `HTTP ${res.status}: ${body.substring(0, 200)}`;
      console.error('[sync]', state.lastError);
      return;
    }

    const geoData = JSON.parse(body);
    // GeoFleet v2 returns objects in response.object or objects array
    const objects = geoData.response?.object || geoData.objects || geoData.keys || [];

    if (!Array.isArray(objects) || objects.length === 0) {
      state.lastError = `No objects returned. Keys: ${Object.keys(geoData).join(', ')}`;
      console.warn('[sync]', state.lastError);
      // Log first 500 chars for debugging
      console.log('[sync] Response body:', body.substring(0, 500));
      return;
    }

    console.log(`[sync] Got ${objects.length} vehicles`);

    const rows = objects.map((o) => {
      const info = o.information || {};
      const pos = o.position || {};
      const io = o.io?.digInput || {};

      let positieTijd = new Date().toISOString();
      if (pos.dateTime) {
        const dt = pos.dateTime;
        positieTijd = `${dt.slice(0, 4)}-${dt.slice(4, 6)}-${dt.slice(6, 8)}T${dt.slice(9, 11)}:${dt.slice(11, 13)}:${dt.slice(13, 15)}Z`;
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
    });

    // Upsert to geofleet_cache
    const cacheRes = await fetch(`${supabaseUrl}/rest/v1/geofleet_cache`, {
      method: 'POST',
      headers: {
        apikey: serviceKey,
        Authorization: `Bearer ${serviceKey}`,
        'Content-Type': 'application/json',
        Prefer: 'resolution=merge-duplicates',
      },
      body: JSON.stringify(rows),
    });

    if (!cacheRes.ok) {
      const errText = await cacheRes.text();
      console.error('[sync] Cache upsert error:', errText);
    }

    // Insert to geofleet_history
    const historyRows = rows.map((r) => ({
      idcode: r.idcode,
      nummerplaat: r.nummerplaat,
      naam: r.naam,
      merk: r.merk,
      type: r.type,
      latitude: r.latitude,
      longitude: r.longitude,
      snelheid: r.snelheid,
      richting: r.richting,
      contact_aan: r.contact_aan,
      adres: r.adres,
      positie_tijd: r.positie_tijd,
    }));

    const histRes = await fetch(`${supabaseUrl}/rest/v1/geofleet_history`, {
      method: 'POST',
      headers: {
        apikey: serviceKey,
        Authorization: `Bearer ${serviceKey}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(historyRows),
    });

    if (!histRes.ok) {
      const errText = await histRes.text();
      console.error('[sync] History insert error:', errText);
    }

    state.lastSync = new Date().toISOString();
    state.vehicleCount = rows.length;
    state.syncCount++;
    state.lastError = null;

    console.log(`[sync] ✅ ${rows.length} vehicles synced (total: ${state.syncCount})`);
  } catch (err) {
    state.lastError = err.message || String(err);
    console.error('[sync] Error:', state.lastError);
  }
}

// ─── HTTP health server ───
const http = require('http');
const PORT = process.env.PORT || 3000;

const server = http.createServer((req, res) => {
  res.setHeader('Content-Type', 'application/json');

  if (req.url === '/debug') {
    const apiKey = process.env.GEOFLEET_API_KEY || '';
    res.end(JSON.stringify({
      ...state,
      uptime: process.uptime(),
      keyPresent: apiKey.length > 0,
      keyLength: apiKey.length,
      host: GEOFLEET_HOST,
      path: GEOFLEET_PATH,
      supabaseConfigured: !!(process.env.SUPABASE_URL && process.env.SUPABASE_SERVICE_KEY),
    }));
    return;
  }

  if (req.url === '/sync') {
    syncPositions().then(() => {
      res.end(JSON.stringify({ triggered: true, ...state }));
    });
    return;
  }

  res.end(JSON.stringify({ status: 'running', ...state }));
});

server.listen(PORT, () => {
  console.log(`[worker] Listening on port ${PORT}`);
  console.log(`[worker] GeoFleet host: ${GEOFLEET_HOST}`);
  console.log(`[worker] GeoFleet path: ${GEOFLEET_PATH}`);
  console.log(`[worker] API key present: ${!!(process.env.GEOFLEET_API_KEY)}`);

  // Initial sync
  syncPositions();

  // Periodic sync
  setInterval(syncPositions, SYNC_INTERVAL);
});
