/**
 * GeoFleet Worker v3.5 — Railway deployment
 * - Syncs vehicle positions every 30s
 * - Syncs trips every 60s (3 vehicles at a time, incremental)
 * - NEW: /trips/sync-range?from=YYYY-MM-DD&to=YYYY-MM-DD to sync historical trips for ALL vehicles
 * - NEW: /trips/sync-vehicle?idcode=XXX&from=YYYY-MM-DD&to=YYYY-MM-DD to sync specific vehicle
 * - Uses correct /report/trips endpoint (plural)
 * - Forces HTTP/1.1 via raw TLS for GeoFleet compatibility
 */
import https from 'https';
import http from 'http';
import tls from 'tls';

const PORT = process.env.PORT || 3000;
const GEOFLEET_API_KEY = process.env.GEOFLEET_API_KEY;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!GEOFLEET_API_KEY || !SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
  console.error('Missing env vars'); process.exit(1);
}

// ─── HTTP/1.1 forced fetch for GeoFleet ───
function geoFetch(url) {
  return new Promise((resolve, reject) => {
    const u = new URL(url);
    const socket = tls.connect({ host: u.hostname, port: 443, servername: u.hostname, ALPNProtocols: ['http/1.1'] }, () => {
      const path = u.pathname + u.search;
      const req = `GET ${path} HTTP/1.1\r\nHost: ${u.hostname}\r\nAccept: application/json\r\nUser-Agent: GeoFleetSync/3.5\r\nConnection: close\r\n\r\n`;
      socket.write(req);
      let data = '';
      socket.on('data', chunk => data += chunk.toString());
      socket.on('end', () => {
        const idx = data.indexOf('\r\n\r\n');
        const body = idx >= 0 ? data.slice(idx + 4) : data;
        // Handle chunked transfer encoding
        const headerPart = idx >= 0 ? data.slice(0, idx).toLowerCase() : '';
        if (headerPart.includes('transfer-encoding: chunked')) {
          let decoded = '', rest = body;
          while (rest.length > 0) {
            const nl = rest.indexOf('\r\n');
            if (nl < 0) break;
            const size = parseInt(rest.slice(0, nl), 16);
            if (size === 0) break;
            decoded += rest.slice(nl + 2, nl + 2 + size);
            rest = rest.slice(nl + 2 + size + 2);
          }
          resolve(decoded);
        } else {
          resolve(body);
        }
      });
      socket.on('error', reject);
    });
    socket.on('error', reject);
    setTimeout(() => { socket.destroy(); reject(new Error('Timeout')); }, 30000);
  });
}

// ─── Supabase helpers ───
async function sbPost(table, rows, upsert = false) {
  const res = await fetch(`${SUPABASE_URL}/rest/v1/${table}`, {
    method: 'POST',
    headers: {
      'apikey': SUPABASE_SERVICE_KEY, 'Authorization': `Bearer ${SUPABASE_SERVICE_KEY}`,
      'Content-Type': 'application/json',
      ...(upsert ? { 'Prefer': 'resolution=merge-duplicates' } : {}),
    },
    body: JSON.stringify(rows),
  });
  return res;
}
async function sbGet(path) {
  const res = await fetch(`${SUPABASE_URL}/rest/v1/${path}`, {
    headers: { 'apikey': SUPABASE_SERVICE_KEY, 'Authorization': `Bearer ${SUPABASE_SERVICE_KEY}` },
  });
  return res.json();
}

// ─── Position sync ───
let positionStats = { lastSync: null, count: 0, errors: 0 };
async function syncPositions() {
  try {
    const raw = await geoFetch(`https://secure.geofleet.eu/geoapi/v2.0/account/objects?apikey=${GEOFLEET_API_KEY}`);
    const data = JSON.parse(raw);
    const objects = data.response?.object || data.objects || [];
    if (!Array.isArray(objects) || objects.length === 0) return;
    const rows = objects.map(o => {
      const info = o.information || {}, pos = o.position || {}, io = o.io?.digInput || {};
      let positieTijd = new Date().toISOString();
      if (pos.dateTime) { const dt = pos.dateTime; positieTijd = `${dt.slice(0,4)}-${dt.slice(4,6)}-${dt.slice(6,8)}T${dt.slice(9,11)}:${dt.slice(11,13)}:${dt.slice(13,15)}Z`; }
      return { idcode: o.idcode, naam: o.name || null, nummerplaat: info.numberPlate || null, merk: info.brand || null, type: info.serie || null, latitude: parseFloat(pos.lat) || 0, longitude: parseFloat(pos.lng) || 0, snelheid: pos.speed || 0, richting: pos.heading || 0, contact_aan: io.ignition ?? false, adres: pos.address || pos.place || null, positie_tijd: positieTijd, updated_at: new Date().toISOString() };
    });
    await sbPost('geofleet_cache', rows, true);
    const histRows = rows.map(r => ({ idcode: r.idcode, nummerplaat: r.nummerplaat, naam: r.naam, merk: r.merk, type: r.type, latitude: r.latitude, longitude: r.longitude, snelheid: r.snelheid, richting: r.richting, contact_aan: r.contact_aan, adres: r.adres, positie_tijd: r.positie_tijd }));
    await sbPost('geofleet_history', histRows);
    positionStats = { lastSync: new Date().toISOString(), count: rows.length, errors: 0 };
  } catch (e) { positionStats.errors++; console.error('Position sync error:', e.message); }
}

// ─── Trip sync (incremental) ───
let tripOffset = 0;
let tripStats = { lastSync: null, totalTrips: 0, vehiclesDone: 0, errors: 0, batchSize: 3 };
async function syncTrips() {
  try {
    const vehicles = await sbGet('geofleet_cache?select=idcode,nummerplaat,naam&order=idcode.asc');
    if (!Array.isArray(vehicles) || vehicles.length === 0) return;
    const batch = vehicles.slice(tripOffset, tripOffset + 3);
    tripOffset = (tripOffset + 3) % vehicles.length;
    const today = new Date().toISOString().split('T')[0].replace(/-/g, '/');
    const yesterday = new Date(Date.now() - 86400000).toISOString().split('T')[0].replace(/-/g, '/');
    let inserted = 0;
    for (const v of batch) {
      for (const datum of [yesterday, today]) {
        try {
          const trips = await fetchTripsForVehicle(v.idcode, datum, datum);
          if (trips.length > 0) {
            const rows = trips.map(t => formatTripRow(t, v));
            await sbPost('geofleet_trips', rows, true);
            inserted += rows.length;
          }
        } catch (e) { console.warn(`Trip sync failed for ${v.nummerplaat || v.idcode} on ${datum}:`, e.message); }
      }
    }
    tripStats = { lastSync: new Date().toISOString(), totalTrips: tripStats.totalTrips + inserted, vehiclesDone: tripOffset, errors: 0, batchSize: 3 };
  } catch (e) { tripStats.errors++; console.error('Trip sync error:', e.message); }
}

async function fetchTripsForVehicle(idcode, fromDate, toDate) {
  const url = `https://secure.geofleet.eu/geoapi/v2.0/report/trips?apikey=${GEOFLEET_API_KEY}&id=${idcode}&from=${fromDate}&fromtime=00:00:00&to=${toDate}&totime=23:59:59`;
  const raw = await geoFetch(url);
  const data = JSON.parse(raw);
  return data.results || [];
}

function formatTripRow(t, v) {
  const startDate = t.startDate?.replace(/\//g, '-') || '';
  const stopDate = t.stopDate?.replace(/\//g, '-') || '';
  return {
    idcode: t.idcode || v.idcode,
    naam: t.vehicleName || v.naam || null,
    nummerplaat: v.nummerplaat || null,
    start_time: `${startDate}T${t.startTime || '00:00:00'}`,
    start_place: t.startPlace || null,
    start_lat: parseFloat(t.startLat) || null,
    start_lng: parseFloat(t.startLng) || null,
    stop_time: `${stopDate}T${t.stopTime || '00:00:00'}`,
    stop_place: t.stopPlace || null,
    stop_lat: parseFloat(t.stopLat) || null,
    stop_lng: parseFloat(t.stopLng) || null,
    distance: t.distance || 0,
    drive_time: t.driveTime || null,
    odometer: t.odometer || null,
    drivers: t.drivers || null,
    is_private: t.isPrivatTrip || false,
  };
}

// ─── Sync range (historical) ───
async function syncTripsRange(fromDate, toDate, specificIdcode = null) {
  const vehicles = specificIdcode 
    ? [{ idcode: specificIdcode, nummerplaat: null, naam: null }] 
    : await sbGet('geofleet_cache?select=idcode,nummerplaat,naam&order=idcode.asc');
  if (!Array.isArray(vehicles)) return { error: 'No vehicles found' };
  
  const dates = [];
  const start = new Date(fromDate);
  const end = new Date(toDate);
  for (let d = new Date(start); d <= end; d.setDate(d.getDate() + 1)) {
    dates.push(d.toISOString().split('T')[0].replace(/-/g, '/'));
  }
  
  let totalInserted = 0, errors = 0;
  for (const v of vehicles) {
    for (const datum of dates) {
      try {
        const trips = await fetchTripsForVehicle(v.idcode, datum, datum);
        if (trips.length > 0) {
          const rows = trips.map(t => formatTripRow(t, v));
          await sbPost('geofleet_trips', rows, true);
          totalInserted += rows.length;
        }
      } catch (e) { errors++; console.warn(`Range sync error ${v.idcode} ${datum}:`, e.message); }
      // Small delay to avoid overwhelming the API
      await new Promise(r => setTimeout(r, 500));
    }
  }
  return { totalInserted, errors, vehicles: vehicles.length, dates: dates.length };
}

// ─── HTTP Server ───
const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://localhost:${PORT}`);
  res.setHeader('Content-Type', 'application/json');
  
  if (url.pathname === '/status') {
    res.end(JSON.stringify({ status: 'running', version: '3.5', positions: positionStats, trips: tripStats }));
  } else if (url.pathname === '/trips/sync') {
    syncTrips().then(() => res.end(JSON.stringify({ ok: true, stats: tripStats })));
  } else if (url.pathname === '/trips/sync-range') {
    const from = url.searchParams.get('from');
    const to = url.searchParams.get('to');
    if (!from || !to) { res.statusCode = 400; res.end(JSON.stringify({ error: 'from and to required (YYYY-MM-DD)' })); return; }
    console.log(`Starting range sync: ${from} to ${to}`);
    syncTripsRange(from, to).then(result => {
      console.log('Range sync complete:', result);
      res.end(JSON.stringify({ ok: true, ...result }));
    }).catch(e => { res.statusCode = 500; res.end(JSON.stringify({ error: e.message })); });
  } else if (url.pathname === '/trips/sync-vehicle') {
    const idcode = url.searchParams.get('idcode');
    const from = url.searchParams.get('from');
    const to = url.searchParams.get('to');
    if (!idcode || !from || !to) { res.statusCode = 400; res.end(JSON.stringify({ error: 'idcode, from, to required' })); return; }
    console.log(`Starting vehicle sync: ${idcode} from ${from} to ${to}`);
    syncTripsRange(from, to, idcode).then(result => {
      console.log('Vehicle sync complete:', result);
      res.end(JSON.stringify({ ok: true, ...result }));
    }).catch(e => { res.statusCode = 500; res.end(JSON.stringify({ error: e.message })); });
  } else {
    res.statusCode = 404;
    res.end(JSON.stringify({ error: 'Not found', endpoints: ['/status', '/trips/sync', '/trips/sync-range?from=YYYY-MM-DD&to=YYYY-MM-DD', '/trips/sync-vehicle?idcode=XXX&from=YYYY-MM-DD&to=YYYY-MM-DD'] }));
  }
});

server.listen(PORT, () => {
  console.log(`GeoFleet Worker v3.5 running on port ${PORT}`);
  syncPositions();
  setInterval(syncPositions, 30000);
  setTimeout(() => { syncTrips(); setInterval(syncTrips, 60000); }, 15000);
});
