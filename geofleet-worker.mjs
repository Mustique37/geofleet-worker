/**
 * GeoFleet Railway Worker v3.2
 * - Syncs live positions to geofleet_cache + geofleet_history (every 30s)
 * - Fetches trip reports incrementally (3 vehicles per cycle, every 60s)
 * - FIXED: correct Trip API URL and date format
 */
import http from 'node:http';
import https from 'node:https';
import tls from 'node:tls';

const PORT = process.env.PORT || 3000;
const GEOFLEET_API_KEY = process.env.GEOFLEET_API_KEY || '';
const SUPABASE_URL = process.env.SUPABASE_URL || '';
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY || '';
const SYNC_INTERVAL = 30_000;
const TRIP_INTERVAL = 60_000;

let lastSync = null;
let lastTripSync = null;
let syncCount = 0;
let tripSyncCount = 0;
let lastError = null;
let lastTripError = null;
let vehicleCount = 0;
let tripCount = 0;
let tripOffset = 0;
let allVehicles = [];

function httpsRequest(url, options = {}, body = null) {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => reject(new Error('Request timeout')), 15000);
    const u = new URL(url);
    const socket = tls.connect({
      host: u.hostname, port: 443,
      ALPNProtocols: ['http/1.1'], servername: u.hostname,
    }, () => {
      const method = options.method || 'GET';
      const path = u.pathname + u.search;
      const headers = { ...options.headers, Host: u.hostname, Connection: 'close' };
      if (body) headers['Content-Length'] = Buffer.byteLength(body);
      let req = `${method} ${path} HTTP/1.1\r\n`;
      for (const [k, v] of Object.entries(headers)) req += `${k}: ${v}\r\n`;
      req += '\r\n';
      socket.write(req);
      if (body) socket.write(body);

      let raw = '';
      socket.on('data', chunk => raw += chunk.toString());
      socket.on('end', () => {
        clearTimeout(timeout);
        const idx = raw.indexOf('\r\n\r\n');
        const statusLine = raw.split('\r\n')[0];
        const statusCode = parseInt(statusLine.split(' ')[1], 10);
        let responseBody = raw.substring(idx + 4);
        if (raw.toLowerCase().includes('transfer-encoding: chunked')) {
          let decoded = '', remaining = responseBody;
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
      socket.on('error', e => { clearTimeout(timeout); reject(e); });
    });
    socket.on('error', e => { clearTimeout(timeout); reject(e); });
  });
}

// ─── Position Sync ───
async function syncPositions() {
  try {
    const res = await httpsRequest(
      `https://secure.geofleet.eu/geoapi/v2.0/account/objects?apikey=${GEOFLEET_API_KEY}`,
      { headers: { 'Content-Type': 'application/json', 'User-Agent': 'GeoFleetWorker/3.3' } }
    );
    if (res.statusCode !== 200) throw new Error(`GeoFleet API ${res.statusCode}`);
    const data = JSON.parse(res.body);
    const objects = data.objects || [];
    vehicleCount = objects.length;

    allVehicles = objects.map(o => ({
      idcode: o.idcode,
      nummerplaat: o.information?.numberPlate || null,
      naam: o.name || null,
    }));

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

    await httpsRequest(
      `${SUPABASE_URL}/rest/v1/geofleet_cache?on_conflict=idcode`,
      { method: 'POST', headers: { 'Content-Type': 'application/json', apikey: SUPABASE_SERVICE_KEY, Authorization: `Bearer ${SUPABASE_SERVICE_KEY}`, Prefer: 'resolution=merge-duplicates' } },
      JSON.stringify(rows)
    );

    const historyRows = rows.map(({ updated_at, ...rest }) => rest);
    await httpsRequest(
      `${SUPABASE_URL}/rest/v1/geofleet_history`,
      { method: 'POST', headers: { 'Content-Type': 'application/json', apikey: SUPABASE_SERVICE_KEY, Authorization: `Bearer ${SUPABASE_SERVICE_KEY}`, Prefer: 'return=minimal' } },
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

// ─── Helper: format date as YYYY/MM/DD (GeoFleet format) ───
function geoDate(d) {
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, '0');
  const dd = String(d.getDate()).padStart(2, '0');
  return `${yyyy}/${mm}/${dd}`;
}

// ─── Helper: format date as YYYY-MM-DD (ISO for Supabase) ───
function isoDate(d) {
  return d.toISOString().split('T')[0];
}

// ─── Incremental Trip Sync (3 vehicles per cycle) ───
async function syncTripsIncremental() {
  if (allVehicles.length === 0) return;
  try {
    const BATCH = 3;
    if (tripOffset >= allVehicles.length) tripOffset = 0;
    const batch = allVehicles.slice(tripOffset, tripOffset + BATCH);
    tripOffset += BATCH;

    const today = new Date();
    const yesterday = new Date(Date.now() - 86400000);
    let batchTrips = 0;

    for (const v of batch) {
      for (const dateObj of [yesterday, today]) {
        const datum = geoDate(dateObj);       // YYYY/MM/DD for GeoFleet API
        const isoDatum = isoDate(dateObj);    // YYYY-MM-DD for Supabase
        try {
          // CORRECT URL: /reports for objects/trip (with spaces encoded as %20)
          const tripUrl = `https://secure.geofleet.eu/geoapi/v2.0/report/trip?apikey=${GEOFLEET_API_KEY}&id=${v.idcode}&from=${datum}&fromtime=00:00:00&to=${datum}&totime=23:59:59`;
          console.log(`[TRIPS] Fetching: ${v.idcode} ${datum}`);
          
          const tripRes = await httpsRequest(tripUrl, {
            headers: { 'Content-Type': 'application/json', 'User-Agent': 'GeoFleetWorker/3.3' }
          });
          
          console.log(`[TRIPS] ${v.idcode} ${datum}: status=${tripRes.statusCode}`);
          
          if (tripRes.statusCode !== 200) {
            lastTripError = `${v.idcode}: HTTP ${tripRes.statusCode}`;
            continue;
          }
          
          const tripData = JSON.parse(tripRes.body);
          const trips = tripData.results || tripData.tripResults || tripData.trips || [];
          
          console.log(`[TRIPS] ${v.idcode} ${datum}: ${trips.length} trips found`);
          
          if (trips.length === 0) continue;

          const tripRows = trips.map(t => ({
            idcode: v.idcode,
            nummerplaat: v.nummerplaat || t.vehicleName || null,
            naam: v.naam || t.vehicleName || null,
            start_time: `${(t.startDate || datum).replace(/\//g, '-')}T${t.startTime || '00:00:00'}`,
            start_place: t.startPlace || null,
            start_lat: parseFloat(t.startLat) || null,
            start_lng: parseFloat(t.startLng) || null,
            stop_time: `${(t.stopDate || datum).replace(/\//g, '-')}T${t.stopTime || '23:59:59'}`,
            stop_place: t.stopPlace || null,
            stop_lat: parseFloat(t.stopLat) || null,
            stop_lng: parseFloat(t.stopLng) || null,
            distance: t.distance || 0,
            drive_time: t.driveTime || null,
            odometer: t.odometer || 0,
            is_private: t.isPrivatTrip || false,
            drivers: t.drivers || null,
          }));

          // Delete old trips for this vehicle+date
          await httpsRequest(
            `${SUPABASE_URL}/rest/v1/geofleet_trips?idcode=eq.${v.idcode}&start_time=gte.${isoDatum}T00:00:00Z&start_time=lte.${isoDatum}T23:59:59Z`,
            { method: 'DELETE', headers: { 'Content-Type': 'application/json', apikey: SUPABASE_SERVICE_KEY, Authorization: `Bearer ${SUPABASE_SERVICE_KEY}` } }
          );

          await httpsRequest(
            `${SUPABASE_URL}/rest/v1/geofleet_trips`,
            { method: 'POST', headers: { 'Content-Type': 'application/json', apikey: SUPABASE_SERVICE_KEY, Authorization: `Bearer ${SUPABASE_SERVICE_KEY}`, Prefer: 'return=minimal' } },
            JSON.stringify(tripRows)
          );
          batchTrips += tripRows.length;
          lastTripError = null;
        } catch (e) {
          lastTripError = `${v.idcode}: ${e.message}`;
          console.warn(`[TRIPS] ${v.idcode} ${datum}:`, e.message);
        }
      }
      await new Promise(r => setTimeout(r, 300));
    }

    tripCount += batchTrips;
    tripSyncCount++;
    lastTripSync = new Date().toISOString();
    console.log(`[TRIPS] Batch done (offset ${tripOffset}/${allVehicles.length}): ${batchTrips} trips`);
  } catch (err) {
    lastTripError = err.message;
    console.error('[TRIPS] Error:', err.message);
  }
}

// ─── HTTP Server ───
const server = http.createServer((req, res) => {
  res.setHeader('Content-Type', 'application/json');
  res.setHeader('Access-Control-Allow-Origin', '*');

  if (req.url === '/status') {
    res.end(JSON.stringify({
      status: 'running', version: '3.3', uptime: process.uptime(),
      lastSync, lastTripSync, syncCount, tripSyncCount,
      vehicleCount, tripCount, tripOffset,
      totalVehicles: allVehicles.length, lastError, lastTripError,
    }));
  } else if (req.url === '/trips/sync') {
    syncTripsIncremental().then(() => {
      res.end(JSON.stringify({ ok: true, tripCount, lastTripSync, tripOffset, lastTripError }));
    }).catch(e => {
      res.end(JSON.stringify({ ok: false, error: e.message }));
    });
  } else {
    res.end(JSON.stringify({ ok: true, message: 'GeoFleet Worker v3.3 — /status /trips/sync' }));
  }
});

server.listen(PORT, () => {
  console.log(`GeoFleet Worker v3.3 listening on :${PORT}`);
  syncPositions();
  setInterval(syncPositions, SYNC_INTERVAL);
  setTimeout(() => {
    syncTripsIncremental();
    setInterval(syncTripsIncremental, TRIP_INTERVAL);
  }, 15_000);
});
