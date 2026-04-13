// geofleet-worker.mjs v3.5.3 — Railway Node.js worker
// Based on v3.4 (working) + added: /trips/sync-vehicle, /trips/sync-range, /debug-trip
// FIX: Don't delete existing trips when API returns empty results
import { createClient } from '@supabase/supabase-js';
import http from 'node:http';
import tls from 'node:tls';

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const GEOFLEET_API_KEY = process.env.GEOFLEET_API_KEY;
const PORT = parseInt(process.env.PORT || '3000', 10);

if (!SUPABASE_URL || !SUPABASE_KEY || !GEOFLEET_API_KEY) {
  console.error('[FATAL] Missing env vars');
  const s = http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'error', message: 'Missing env vars', hasUrl: !!SUPABASE_URL, hasKey: !!SUPABASE_KEY, hasGeo: !!GEOFLEET_API_KEY }));
  });
  s.listen(PORT, '0.0.0.0', () => console.log(`[ERROR] Listening on ${PORT}`));
} else {
  const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);
  const GEOFLEET_HOST = 'secure.geofleet.eu';
  const VERSION = '3.5.3';

  let state = {
    version: VERSION, started: new Date().toISOString(),
    positionSyncs: 0, tripSyncs: 0, tripCount: 0,
    lastPositionSync: null, lastTripSync: null,
    lastError: null, lastTripError: null,
    vehicleCount: 0, tripOffset: 0,
  };

  function geofleetRequest(path) {
    return new Promise((resolve, reject) => {
      const socket = tls.connect(443, GEOFLEET_HOST, { servername: GEOFLEET_HOST, ALPNProtocols: ['http/1.1'] }, () => {
        socket.write(`GET ${path} HTTP/1.1\r\nHost: ${GEOFLEET_HOST}\r\nUser-Agent: GeoFleetSync/3.5\r\nAccept: application/json\r\nConnection: close\r\n\r\n`);
      });
      let data = '';
      socket.on('data', chunk => data += chunk.toString());
      socket.on('end', () => {
        try {
          const bodyStart = data.indexOf('\r\n\r\n');
          if (bodyStart === -1) return reject(new Error('No HTTP body'));
          let body = data.slice(bodyStart + 4);
          if (data.toLowerCase().includes('transfer-encoding: chunked')) {
            let decoded = '', rest = body;
            while (rest.length > 0) {
              const nl = rest.indexOf('\r\n');
              if (nl === -1) break;
              const size = parseInt(rest.slice(0, nl), 16);
              if (size === 0) break;
              decoded += rest.slice(nl + 2, nl + 2 + size);
              rest = rest.slice(nl + 2 + size + 2);
            }
            body = decoded;
          }
          resolve(JSON.parse(body));
        } catch (e) { reject(new Error(`Parse error: ${e.message}`)); }
      });
      socket.on('error', reject);
      socket.setTimeout(30000, () => { socket.destroy(); reject(new Error('Timeout')); });
    });
  }

  function geofleetRequestRaw(path) {
    return new Promise((resolve, reject) => {
      const socket = tls.connect(443, GEOFLEET_HOST, { servername: GEOFLEET_HOST, ALPNProtocols: ['http/1.1'] }, () => {
        socket.write(`GET ${path} HTTP/1.1\r\nHost: ${GEOFLEET_HOST}\r\nUser-Agent: GeoFleetSync/3.5\r\nAccept: application/json\r\nConnection: close\r\n\r\n`);
      });
      let data = '';
      socket.on('data', chunk => data += chunk.toString());
      socket.on('end', () => {
        const bodyStart = data.indexOf('\r\n\r\n');
        let body = bodyStart > -1 ? data.slice(bodyStart + 4) : data;
        if (data.toLowerCase().includes('transfer-encoding: chunked')) {
          let decoded = '', rest = body;
          while (rest.length > 0) {
            const nl = rest.indexOf('\r\n');
            if (nl === -1) break;
            const size = parseInt(rest.slice(0, nl), 16);
            if (size === 0) break;
            decoded += rest.slice(nl + 2, nl + 2 + size);
            rest = rest.slice(nl + 2 + size + 2);
          }
          body = decoded;
        }
        resolve(body);
      });
      socket.on('error', reject);
      socket.setTimeout(30000, () => { socket.destroy(); reject(new Error('Timeout')); });
    });
  }

  // ─── Position Sync ───
  async function syncPositions() {
    try {
      const geoData = await geofleetRequest(`/geoapi/v2.0/account/objects?apikey=${GEOFLEET_API_KEY}`);
      const objects = geoData.response?.object || geoData.objects || [];
      if (!Array.isArray(objects) || objects.length === 0) { state.lastError = 'No objects'; return; }
      state.vehicleCount = objects.length;

      const rows = objects.map(obj => {
        const pos = obj.position || obj;
        let positieTijd = new Date().toISOString();
        if (pos.dateTime) {
          const dt = pos.dateTime;
          positieTijd = `${dt.slice(0,4)}-${dt.slice(4,6)}-${dt.slice(6,8)}T${dt.slice(9,11)}:${dt.slice(11,13)}:${dt.slice(13,15)}Z`;
        }
        return {
          idcode: String(obj.idcode || obj.id),
          naam: obj.naam || obj.name || null,
          nummerplaat: obj.nummerplaat || obj.licensePlate || null,
          merk: obj.merk || obj.brand || null,
          type: obj.type || null,
          latitude: pos.latitude || pos.lat || null,
          longitude: pos.longitude || pos.lng || null,
          snelheid: pos.snelheid || pos.speed || 0,
          richting: pos.richting || pos.direction || 0,
          adres: pos.adres || pos.address || null,
          contact_aan: pos.contactAan ?? pos.ignition ?? false,
          positie_tijd: positieTijd,
          updated_at: new Date().toISOString(),
        };
      });

      const { error } = await supabase.from('geofleet_cache').upsert(rows, { onConflict: 'idcode' });
      if (error) state.lastError = error.message;
      else { state.positionSyncs++; state.lastPositionSync = new Date().toISOString(); state.lastError = null; }
    } catch (e) { state.lastError = e.message; }
  }

  // ─── Build trip row from API response ───
  function buildTripRow(idcode, t) {
    // API returns: startDate "2022/03/04", startTime "08:48:44" → combine to ISO
    const startDateTime = combineDateTime(t.startDate, t.startTime) || t.startTime || t.start_time || null;
    const stopDateTime = combineDateTime(t.stopDate, t.stopTime) || t.stopTime || t.stop_time || null;
    return {
      idcode,
      naam: t.vehicleName || t.naam || t.name || null,
      nummerplaat: t.nummerplaat || t.licensePlate || null,
      start_time: startDateTime,
      stop_time: stopDateTime,
      start_place: t.startPlace || t.start_place || null,
      stop_place: t.stopPlace || t.stop_place || null,
      start_lat: parseFloat(t.startLat) || null,
      start_lng: parseFloat(t.startLng) || null,
      stop_lat: parseFloat(t.stopLat) || null,
      stop_lng: parseFloat(t.stopLng) || null,
      distance: parseFloat(t.distance) || 0,
      drive_time: t.driveTime || t.drive_time || null,
      odometer: parseFloat(t.odometer) || null,
      drivers: t.drivers || t.bestuurder || null,
      is_private: t.isPrivatTrip || t.isPrivate || false,
    };
  }

  function combineDateTime(dateStr, timeStr) {
    if (!dateStr || !timeStr) return null;
    // dateStr: "2022/03/04" or "2022-03-04"
    const d = dateStr.replace(/\//g, '-');
    return `${d}T${timeStr}Z`;
  }

  // ─── Trip Sync for one vehicle + one day ───
  // CRITICAL FIX: Only delete existing trips if API returns new data
  async function syncTripsForVehicle(idcode, datum) {
    const data = await geofleetRequest(`/geoapi/v2.0/report/trips?apikey=${GEOFLEET_API_KEY}&id=${idcode}&from=${datum}&fromtime=00:00:00&to=${datum}&totime=23:59:59`);
    const trips = data.results || data.result || [];
    if (!Array.isArray(trips) || trips.length === 0) return 0; // DON'T delete existing data!

    // Only delete+insert if we got actual trip data
    await supabase.from('geofleet_trips').delete()
      .eq('idcode', idcode)
      .gte('start_time', `${datum}T00:00:00Z`)
      .lte('start_time', `${datum}T23:59:59Z`);

    const rows = trips.map(t => buildTripRow(idcode, t));
    const { error } = await supabase.from('geofleet_trips').insert(rows);
    if (error) console.error(`[TRIP] Insert ${idcode}/${datum}:`, error.message);
    return rows.length;
  }

  // ─── Incremental trip sync ───
  async function syncTripsIncremental() {
    try {
      const { data: vehicles } = await supabase.from('geofleet_cache').select('idcode').order('idcode');
      if (!vehicles || vehicles.length === 0) return;
      const batch = vehicles.slice(state.tripOffset, state.tripOffset + 3);
      if (batch.length === 0) { state.tripOffset = 0; return; }

      const today = new Date().toISOString().split('T')[0];
      const yesterday = new Date(Date.now() - 86400000).toISOString().split('T')[0];
      let count = 0;
      for (const v of batch) {
        try { count += await syncTripsForVehicle(v.idcode, today); } catch (e) { console.error(`[TRIP] ${v.idcode} today:`, e.message); }
        try { count += await syncTripsForVehicle(v.idcode, yesterday); } catch (e) { console.error(`[TRIP] ${v.idcode} yesterday:`, e.message); }
      }
      state.tripOffset += 3;
      if (state.tripOffset >= vehicles.length) state.tripOffset = 0;
      state.tripCount += count;
      state.tripSyncs++;
      state.lastTripSync = new Date().toISOString();
      state.lastTripError = null;
    } catch (e) { state.lastTripError = e.message; }
  }

  // ─── Manual sync for date range ───
  async function syncVehicleRange(idcode, fromDate, toDate) {
    let total = 0;
    const from = new Date(fromDate), to = new Date(toDate);
    for (let d = new Date(from); d <= to; d.setDate(d.getDate() + 1)) {
      try { total += await syncTripsForVehicle(idcode, d.toISOString().split('T')[0]); } catch (e) { console.error(`[SYNC] ${idcode} ${d.toISOString().split('T')[0]}: ${e.message}`); }
      await new Promise(r => setTimeout(r, 500));
    }
    return total;
  }

  async function syncAllVehiclesRange(fromDate, toDate) {
    const { data: vehicles } = await supabase.from('geofleet_cache').select('idcode');
    if (!vehicles) return 0;
    let total = 0;
    for (const v of vehicles) total += await syncVehicleRange(v.idcode, fromDate, toDate);
    return total;
  }

  // ─── HTTP Server ───
  const server = http.createServer(async (req, res) => {
    const url = new URL(req.url, `http://localhost:${PORT}`);
    const p = url.pathname;
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
    if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }

    const json = (data, status = 200) => { res.writeHead(status, { 'Content-Type': 'application/json' }); res.end(JSON.stringify(data)); };

    try {
      if (p === '/' || p === '/health' || p === '/healthz' || p === '/status') {
        json({ status: 'ok', ...state });
      } else if (p === '/sync') {
        await syncPositions();
        json({ ok: true, vehicleCount: state.vehicleCount });
      } else if (p === '/trips/sync') {
        await syncTripsIncremental();
        json({ ok: true, tripCount: state.tripCount, offset: state.tripOffset });
      } else if (p === '/trips/sync-vehicle') {
        const idcode = url.searchParams.get('idcode');
        const from = url.searchParams.get('from');
        const to = url.searchParams.get('to');
        if (!idcode || !from || !to) return json({ error: 'Need: idcode, from, to' }, 400);
        const count = await syncVehicleRange(idcode, from, to);
        json({ ok: true, idcode, from, to, tripsFound: count });
      } else if (p === '/trips/sync-range') {
        const from = url.searchParams.get('from');
        const to = url.searchParams.get('to');
        if (!from || !to) return json({ error: 'Need: from, to' }, 400);
        const count = await syncAllVehiclesRange(from, to);
        json({ ok: true, from, to, tripsFound: count });
      } else if (p === '/debug-trip') {
        const idcode = url.searchParams.get('idcode') || '621946';
        const datum = url.searchParams.get('date') || new Date().toISOString().split('T')[0];
        const raw = await geofleetRequestRaw(`/geoapi/v2.0/report/trips?apikey=${GEOFLEET_API_KEY}&id=${idcode}&from=${datum}&fromtime=00:00:00&to=${datum}&totime=23:59:59`);
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(raw);
      } else {
        json({ error: 'Not found' }, 404);
      }
    } catch (e) { json({ error: e.message }, 500); }
  });

  server.listen(PORT, '0.0.0.0', () => {
    console.log(`[v${VERSION}] GeoFleet Worker listening on port ${PORT}`);
  });

  setInterval(syncPositions, 30000);
  setInterval(syncTripsIncremental, 60000);
  setTimeout(syncPositions, 3000);
  setTimeout(syncTripsIncremental, 10000);
}
