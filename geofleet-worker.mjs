import http from 'node:http';

import https from 'node:https';

const GEOFLEET_HOST = 'secure.geofleet.eu';
const GEOFLEET_PATH = '/geoapi/v2.0/account/objects';
const SYNC_INTERVAL = 30_000;
const PORT = Number(process.env.PORT || 3000);

const state = {
  startedAt: new Date().toISOString(),
  lastSync: null,
  lastError: null,
  vehicleCount: 0,
  syncCount: 0,
};

function httpsRequest(url, options = {}, body) {
  return new Promise((resolve, reject) => {
    const req = https.request(url, options, (res) => {
      let data = '';
      res.on('data', (chunk) => {
        data += chunk;
      });
      res.on('end', () => {
        resolve({ status: res.statusCode || 500, body: data, headers: res.headers });
      });
    });

    req.on('error', reject);
    req.setTimeout(15000, () => req.destroy(new Error('Request timeout')));

    if (body) req.write(body);
    req.end();
  });
}

function parseGeoFleetObjects(payload) {
  if (Array.isArray(payload?.response?.object)) return payload.response.object;
  if (Array.isArray(payload?.objects)) return payload.objects;
  if (Array.isArray(payload?.response)) return payload.response;
  return [];
}

function mapObjectToRow(object) {
  const info = object?.information ?? {};
  const pos = object?.position ?? {};
  const io = object?.io?.digInput ?? {};

  let positieTijd = new Date().toISOString();
  if (typeof pos.dateTime === 'string' && pos.dateTime.length >= 15) {
    const dt = pos.dateTime;
    positieTijd = `${dt.slice(0, 4)}-${dt.slice(4, 6)}-${dt.slice(6, 8)}T${dt.slice(9, 11)}:${dt.slice(11, 13)}:${dt.slice(13, 15)}Z`;
  }

  return {
    idcode: object?.idcode ?? null,
    naam: object?.name ?? null,
    nummerplaat: info.numberPlate ?? null,
    merk: info.brand ?? null,
    type: info.serie ?? info.type ?? null,
    latitude: Number.parseFloat(pos.lat) || 0,
    longitude: Number.parseFloat(pos.lng) || 0,
    snelheid: Number(pos.speed) || 0,
    richting: Number(pos.heading) || 0,
    contact_aan: Boolean(io.ignition ?? false),
    adres: pos.address ?? pos.place ?? null,
    positie_tijd: positieTijd,
    updated_at: new Date().toISOString(),
  };
}

async function writeToBackend(rows, serviceKey, supabaseUrl) {
  const cacheResponse = await httpsRequest(
    `${supabaseUrl}/rest/v1/geofleet_cache`,
    {
      method: 'POST',
      headers: {
        apikey: serviceKey,
        Authorization: `Bearer ${serviceKey}`,
        'Content-Type': 'application/json',
        Prefer: 'resolution=merge-duplicates',
      },
    },
    JSON.stringify(rows),
  );

  if (cacheResponse.status >= 400) {
    throw new Error(`Cache upsert failed: ${cacheResponse.status} ${cacheResponse.body.slice(0, 200)}`);
  }

  const historyRows = rows.map((row) => ({
    idcode: row.idcode,
    nummerplaat: row.nummerplaat,
    naam: row.naam,
    merk: row.merk,
    type: row.type,
    latitude: row.latitude,
    longitude: row.longitude,
    snelheid: row.snelheid,
    richting: row.richting,
    contact_aan: row.contact_aan,
    adres: row.adres,
    positie_tijd: row.positie_tijd,
  }));

  const historyResponse = await httpsRequest(
    `${supabaseUrl}/rest/v1/geofleet_history`,
    {
      method: 'POST',
      headers: {
        apikey: serviceKey,
        Authorization: `Bearer ${serviceKey}`,
        'Content-Type': 'application/json',
      },
    },
    JSON.stringify(historyRows),
  );

  if (historyResponse.status >= 400) {
    throw new Error(`History insert failed: ${historyResponse.status} ${historyResponse.body.slice(0, 200)}`);
  }
}

async function syncPositions() {
  const apiKey = process.env.GEOFLEET_API_KEY;
  const supabaseUrl = process.env.SUPABASE_URL;
  const serviceKey = process.env.SUPABASE_SERVICE_KEY;

  if (!apiKey || !supabaseUrl || !serviceKey) {
    state.lastError = 'Missing environment variables';
    console.error('[worker] Missing GEOFLEET_API_KEY, SUPABASE_URL, or SUPABASE_SERVICE_KEY');
    return;
  }

  try {
    const geoResponse = await httpsRequest(`https://${GEOFLEET_HOST}${GEOFLEET_PATH}`, {
      method: 'GET',
      headers: {
        'x-api-key': apiKey,
        Accept: 'application/json',
        'User-Agent': 'GeoFleetSync/3.0',
      },
    });

    if (geoResponse.status === 401) {
      throw new Error(`GeoFleet unauthorized (401): ${geoResponse.body.slice(0, 200)}`);
    }

    if (geoResponse.status >= 400) {
      throw new Error(`GeoFleet error ${geoResponse.status}: ${geoResponse.body.slice(0, 200)}`);
    }

    const payload = JSON.parse(geoResponse.body || '{}');
    const objects = parseGeoFleetObjects(payload);

    if (!Array.isArray(objects) || objects.length === 0) {
      throw new Error(`No objects returned. Top-level keys: ${Object.keys(payload).join(', ')}`);
    }

    const rows = objects.map(mapObjectToRow).filter((row) => row.idcode);
    await writeToBackend(rows, serviceKey, supabaseUrl);

    state.lastSync = new Date().toISOString();
    state.lastError = null;
    state.vehicleCount = rows.length;
    state.syncCount += 1;
    console.log(`[worker] Synced ${rows.length} vehicles`);
  } catch (error) {
    state.lastError = error instanceof Error ? error.message : String(error);
    console.error('[worker] Sync failed:', state.lastError);
  }
}

const server = http.createServer(async (req, res) => {
  res.setHeader('Content-Type', 'application/json');

  if (req.url === '/status' || req.url === '/health') {
    res.writeHead(200);
    res.end(JSON.stringify({ ok: true, ...state }));
    return;
  }

  if (req.url === '/debug') {
    res.writeHead(200);
    res.end(JSON.stringify({
      ok: true,
      ...state,
      uptime: process.uptime(),
      host: GEOFLEET_HOST,
      path: GEOFLEET_PATH,
      keyPresent: Boolean(process.env.GEOFLEET_API_KEY),
      supabaseConfigured: Boolean(process.env.SUPABASE_URL && process.env.SUPABASE_SERVICE_KEY),
    }));
    return;
  }

  if (req.url === '/sync') {
    await syncPositions();
    res.writeHead(200);
    res.end(JSON.stringify({ triggered: true, ...state }));
    return;
  }

  res.writeHead(200);
  res.end(JSON.stringify({ ok: true, route: req.url, ...state }));
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`[worker] Listening on ${PORT}`);
  setTimeout(() => {
    syncPositions();
    setInterval(syncPositions, SYNC_INTERVAL);
  }, 1000);
});
