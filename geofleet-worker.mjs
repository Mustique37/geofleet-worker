/**
 * GeoFleet Railway Worker v3.0
 * - Syncs live positions to geofleet_cache + geofleet_history (every 30s)
 * - Fetches trip reports from GeoFleet Trip API and saves to geofleet_trips (every 5 min)
 */
import http from 'node:http';
import https from 'node:https';
import tls from 'node:tls';

const PORT = process.env.PORT || 3000;
const GEOFLEET_API_KEY = process.env.GEOFLEET_API_KEY || '';
const SUPABASE_URL = process.env.SUPABASE_URL || '';
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY || '';
const SYNC_INTERVAL = 30_000;        // 30 seconds for positions
const TRIP_INTERVAL = 5 * 60_000;    // 5 minutes for trips

let lastSync = null;
let lastTripSync = null;
let syncCount = 0;
let tripSyncCount = 0;
let lastError = null;
let vehicleCount = 0;
let tripCount = 0;

// ─── HTTP/1.1 forced request (GeoFleet needs this) ───
function httpsRequest(url, options = {}, body = null) {
  return new Promise((resolve, reject) => {
    const u = new URL(url);
    const socket = tls.connect({
      host: u.hostname,
      port: 443,
      ALPNProtocols: ['http/1.1'],
      servername: u.hostname,
    }, () => {
      const method = options.method || 'GET';
      const path = u.pathname + u.search;
      const headers = { ...options.headers, Host: u.hostname, Connection: 'close' };
      if (body) {
        headers['Content-Length'] = Buffer.byteLength(body);
      }
      let req = `${method} ${path} HTTP/1.1\r\n`;
      for (const [k, v] of Object.entries(headers)) req += `${k}: ${v}\r\n`;
      req += '\r\n';
      socket.write(req);
      if (body) socket.write(body);

      let raw = '';
      socket.on('data', chunk => raw += chunk.toString());
      socket.on('end', () => {
        const idx = raw.indexOf('\r\n\r\n');
        const statusLine = raw.split('\r\n')[0];
        const statusCode = parseInt(statusLine.split(' ')[1], 10);
        let responseBody = raw.substring(idx + 4);
        // Handle chunked transfer encoding
        if (raw.toLowerCase().includes('transfer-encoding: chunked')) {
          let decoded = '';
          let remaining = responseBody;
          while (remaining.length > 0) {
            const lineEnd = remaining.indexOf('\r\n');
            if (lineEnd === -1) break;
            const chunkSize = parseInt(remaining.substring(0, lineEnd), 16);
            if (chunkSize === 0) break;
            decoded += remaining.substring(lineEnd + 2, lineEnd + 2 + chunkSize);
            remaining = remaining.substring(lineEnd + 2 + chunkSize + 2);
          }
          responseBody = decoded;
        }
        resolve({ statusCode, body: responseBody });
      });
      socket.on('error', reject);
    });
    socket.on('error', reject);
  });
}

// ─── Position Sync ───
async function syncPositions() {
  try {
    const res = await httpsRequest(
      `https://secure.geofleet.eu/geoapi/v2.0/account/objects?apikey=${GEOFLEET_API_KEY}`,
      { headers: { 'Content-Type': 'application/json', 'User-Agent': 'GeoFleetWorker/3.0' } }
    );
    if (res.statusCode !== 200) throw new Error(`GeoFleet API ${res.statusCode}`);
    const data = JSON.parse(res.body);
    const objects = data.objects || [];
    vehicleCount = objects.length;

    const rows = objects.map(o => ({
      idcode: o.idcode,
      naam: o.name || null,
      merk: o.information?.brand || null,
      type: o.information?.type || null,
      nummerplaat: o.information?.numberPlate || null,
      latitude: o.position?.lat || null,
      longitude: o.position?.lng || null,
      snelheid: o.position?.speed || 0,
      richting: o.position?.heading || 0,
      contact_aan: o.io?.digInput?.ignition || false,
      adres: o.position?.address || null,
      positie_tijd: o.position?.dateTime || new Date().toISOString(),
      updated_at: new Date().toISOString(),
    }));

    // Upsert cache
    const cacheRes = await httpsRequest(
      `${SUPABASE_URL}/rest/v1/geofleet_cache?on_conflict=idcode`,
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          apikey: SUPABASE_SERVICE_KEY,
          Authorization: `Bearer ${SUPABASE_SERVICE_KEY}`,
          Prefer: 'resolution=merge-duplicates',
        },
      },
      JSON.stringify(rows)
    );

    // Insert history
    const historyRows = rows.map(({ updated_at, ...rest }) => rest);
    await httpsRequest(
      `${SUPABASE_URL}/rest/v1/geofleet_history`,
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          apikey: SUPABASE_SERVICE_KEY,
          Authorization: `Bearer ${SUPABASE_SERVICE_KEY}`,
          Prefer: 'return=minimal',
        },
      },
      JSON.stringify(historyRows)
    );

    syncCount++;
    lastSync = new Date().toISOString();
    lastError = null;
  } catch (err) {
    lastError = err.message;
    console.error('[SYNC] Error:', err.message);
  }
}

// ─── Trip Sync (fetches trips for today for all vehicles) ───
async function syncTrips() {
  try {
    // First get all known idcodes from cache
    const cacheRes = await httpsRequest(
      `${SUPABASE_URL}/rest/v1/geofleet_cache?select=idcode,nummerplaat,naam`,
      {
        headers: {
          'Content-Type': 'application/json',
          apikey: SUPABASE_SERVICE_KEY,
          Authorization: `Bearer ${SUPABASE_SERVICE_KEY}`,
        },
      }
    );
    const vehicles = JSON.parse(cacheRes.body);
    if (!Array.isArray(vehicles) || vehicles.length === 0) return;

    const today = new Date().toISOString().split('T')[0];
    const yesterday = new Date(Date.now() - 86400000).toISOString().split('T')[0];
    let totalTrips = 0;

    // Fetch trips for yesterday and today (batch of 5 at a time to avoid overload)
    for (const datum of [yesterday, today]) {
      for (let i = 0; i < vehicles.length; i += 5) {
        const batch = vehicles.slice(i, i + 5);
        const promises = batch.map(async (v) => {
          try {
            const tripRes = await httpsRequest(
              `https://secure.geofleet.eu/geoapi/v2.0/reports/trips?apikey=${GEOFLEET_API_KEY}&id=${v.idcode}&from=${datum}&fromtime=00:00&to=${datum}&totime=23:59`,
              { headers: { 'Content-Type': 'application/json', 'User-Agent': 'GeoFleetWorker/3.0' } }
            );
            if (tripRes.statusCode !== 200) return [];
            const tripData = JSON.parse(tripRes.body);
            const trips = tripData.tripResults || tripData.trips || [];
            return trips.map(t => ({
              idcode: v.idcode,
              nummerplaat: v.nummerplaat || t.vehicleName || null,
              naam: v.naam || t.vehicleName || null,
              start_time: `${t.startDate || datum}T${t.startTime || '00:00:00'}`,
              start_place: t.startPlace || null,
              start_lat: parseFloat(t.startLat) || null,
              start_lng: parseFloat(t.startLng) || null,
              stop_time: `${t.stopDate || datum}T${t.stopTime || '23:59:59'}`,
              stop_place: t.stopPlace || null,
              stop_lat: parseFloat(t.stopLat) || null,
              stop_lng: parseFloat(t.stopLng) || null,
              distance: t.distance || 0,
              drive_time: t.driveTime || null,
              odometer: t.odometer || 0,
              is_private: t.isPrivatTrip || false,
              drivers: t.drivers || null,
            }));
          } catch (e) {
            console.warn(`[TRIPS] Failed for ${v.idcode}:`, e.message);
            return [];
          }
        });

        const batchResults = await Promise.all(promises);
        const tripRows = batchResults.flat();

        if (tripRows.length > 0) {
          // Delete existing trips for these vehicles on this date to avoid duplicates
          for (const v of batch) {
            await httpsRequest(
              `${SUPABASE_URL}/rest/v1/geofleet_trips?idcode=eq.${v.idcode}&start_time=gte.${datum}T00:00:00Z&start_time=lte.${datum}T23:59:59Z`,
              {
                method: 'DELETE',
                headers: {
                  'Content-Type': 'application/json',
                  apikey: SUPABASE_SERVICE_KEY,
                  Authorization: `Bearer ${SUPABASE_SERVICE_KEY}`,
                },
              }
            );
          }

          // Insert new trips
          await httpsRequest(
            `${SUPABASE_URL}/rest/v1/geofleet_trips`,
            {
              method: 'POST',
              headers: {
                'Content-Type': 'application/json',
                apikey: SUPABASE_SERVICE_KEY,
                Authorization: `Bearer ${SUPABASE_SERVICE_KEY}`,
                Prefer: 'return=minimal',
              },
            },
            JSON.stringify(tripRows)
          );
          totalTrips += tripRows.length;
        }

        // Small delay between batches
        await new Promise(r => setTimeout(r, 500));
      }
    }

    tripCount = totalTrips;
    tripSyncCount++;
    lastTripSync = new Date().toISOString();
    console.log(`[TRIPS] Synced ${totalTrips} trips for ${yesterday} & ${today}`);
  } catch (err) {
    console.error('[TRIPS] Error:', err.message);
  }
}

// ─── HTTP Server ───
const server = http.createServer((req, res) => {
  res.setHeader('Content-Type', 'application/json');
  res.setHeader('Access-Control-Allow-Origin', '*');

  if (req.url === '/status') {
    res.end(JSON.stringify({
      status: 'running',
      version: '3.0',
      uptime: process.uptime(),
      lastSync,
      lastTripSync,
      syncCount,
      tripSyncCount,
      vehicleCount,
      tripCount,
      lastError,
    }));
  } else if (req.url === '/trips/sync') {
    syncTrips().then(() => {
      res.end(JSON.stringify({ ok: true, tripCount, lastTripSync }));
    });
  } else {
    res.end(JSON.stringify({ ok: true, message: 'GeoFleet Worker v3.0 — /status /trips/sync' }));
  }
});

server.listen(PORT, () => {
  console.log(`GeoFleet Worker v3.0 listening on :${PORT}`);
  syncPositions();
  setInterval(syncPositions, SYNC_INTERVAL);
  // Initial trip sync after 10s, then every 5 min
  setTimeout(() => {
    syncTrips();
    setInterval(syncTrips, TRIP_INTERVAL);
  }, 10_000);
});
