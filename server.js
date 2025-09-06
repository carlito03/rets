// server.js — CommonJS, Node 18+ (global fetch), Express only
const express = require('express');
const app = express();

const PORT = process.env.PORT || 8080;
const TRESTLE_HOST = process.env.TRESTLE_HOST || 'https://api-prod.corelogic.com';
const TOKEN_URL = `${TRESTLE_HOST}/trestle/oidc/connect/token`;
const ODATA_BASE = `${TRESTLE_HOST}/trestle/odata/Property`;

// ---- Token cache (scope=api) ----
let tokenCache = { token: null, exp: 0 }; // epoch seconds

async function getApiToken() {
  const now = Math.floor(Date.now() / 1000);
  if (tokenCache.token && tokenCache.exp - 60 > now) return tokenCache.token;

  const cid = process.env.TRESTLE_CLIENT_ID;
  const csec = process.env.TRESTLE_CLIENT_SECRET;

  // Don't crash on startup if env vars are missing; just fail when an endpoint uses the token
  if (!cid || !csec) {
    throw new Error('Missing TRESTLE_CLIENT_ID / TRESTLE_CLIENT_SECRET environment variables');
  }

  const basic = Buffer.from(`${cid}:${csec}`).toString('base64');
  const body = new URLSearchParams({ grant_type: 'client_credentials', scope: 'api' });

  const resp = await fetch(TOKEN_URL, {
    method: 'POST',
    headers: {
      Authorization: `Basic ${basic}`,
      'Content-Type': 'application/x-www-form-urlencoded',
      Accept: 'application/json'
    },
    body
  });

  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`Token failed: ${resp.status} ${resp.statusText} - ${text}`);
  }

  const data = await resp.json();
  tokenCache.token = data.access_token;
  tokenCache.exp = now + (typeof data.expires_in === 'number' ? data.expires_in : 3600);
  return tokenCache.token;
}

// ---- Helpers ----
function escapeOdataString(s) { return String(s).replace(/'/g, "''"); }

function buildBaseFilter({ city, days, minPrice, maxPrice }) {
  const filters = [];
  if (city) filters.push(`City eq '${escapeOdataString(city)}'`);
  const cutoffISO = new Date(Date.now() - (Number(days) || 90) * 24 * 3600 * 1000).toISOString();
  filters.push(`ModificationTimestamp ge ${cutoffISO}`);
  if (Number.isFinite(minPrice)) filters.push(`ListPrice ge ${minPrice}`);
  if (Number.isFinite(maxPrice)) filters.push(`ListPrice le ${maxPrice}`);
  return filters;
}

function buildParams({ select, top = 50, filter, orderby = 'ModificationTimestamp desc' }) {
  const p = new URLSearchParams();
  p.set('$count', 'true');
  p.set('$top', String(Math.min(Number(top) || 50, 1000)));
  p.set('$orderby', orderby);
  if (select) p.set('$select', select);
  if (filter) p.set('$filter', filter);
  return p;
}

async function fetchOData(url) {
  const token = await getApiToken();
  const r = await fetch(url, { headers: { Authorization: `Bearer ${token}`, Accept: 'application/json' } });
  const txt = await r.text();
  if (!r.ok) throw new Error(`OData fetch failed ${r.status} ${r.statusText} - ${txt}`);
  try { return JSON.parse(txt); } catch { throw new Error(`OData returned non-JSON body: ${txt.slice(0, 300)}...`); }
}

async function fetchCityListings({ city, top, days, minPrice, maxPrice, status = 'Active' }) {
  const baseFilters = buildBaseFilter({ city, days, minPrice, maxPrice });
  const SELECT_FIELDS = [
    'ListingKey','ListingId','City','PostalCode','ListPrice','ModificationTimestamp'
  ].join(',');

  const attempts = [
    { field: 'StandardStatus', clause: status ? ` and StandardStatus eq '${escapeOdataString(status)}'` : '' },
    { field: 'MlsStatus',      clause: status ? ` and MlsStatus eq '${escapeOdataString(status)}'` : '' },
    { field: '(none)',         clause: '' }
  ];

  const tried = [];
  for (const a of attempts) {
    const filter = baseFilters.join(' and ') + a.clause;
    const params = buildParams({ select: SELECT_FIELDS, top, filter, orderby: 'ModificationTimestamp desc' });
    const url = `${ODATA_BASE}?${params.toString()}`;
    try {
      const data = await fetchOData(url);
      return { ok: true, statusFieldUsed: a.field, query: Object.fromEntries(params.entries()), data };
    } catch (err) {
      tried.push({ statusFieldTried: a.field, error: err.message });
    }
  }
  return { ok: false, tried };
}

// ---- Routes ----
app.get('/', (_req, res) => res.send('CRMLS OData proxy is running on Cloud Run ✅'));
app.get('/healthz', (_req, res) => res.json({ ok: true }));
app.get('/__routes', (_req, res) => {
  res.json({
    ok: true,
    routes: ['/', '/healthz', '/__routes', '/auth/token', '/listings'],
    note: 'If you see this JSON, you are hitting the Express app.'
  });
});

// Dev-only: masked token preview
app.get('/auth/token', async (_req, res) => {
  try {
    const tok = await getApiToken();
    res.json({ scope: 'api', access_token_preview: tok ? tok.slice(0, 16) + '…' : null, note: 'masked' });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

/**
 * GET /listings?city=Long%20Beach&top=25&days=90&minPrice=700000&status=Active
 */
app.get('/listings', async (req, res) => {
  try {
    const city = (req.query.city || 'Long Beach').trim();
    const top = Math.min(parseInt(req.query.top || '50', 10) || 50, 1000);
    const days = parseInt(req.query.days || '90', 10) || 90;
    const minPrice = req.query.minPrice ? parseInt(req.query.minPrice, 10) : null;
    const maxPrice = req.query.maxPrice ? parseInt(req.query.maxPrice, 10) : null;
    const status = (req.query.status || 'Active').trim();

    const result = await fetchCityListings({ city, top, days, minPrice, maxPrice, status });
    if (!result.ok) return res.status(502).json({ error: 'All status variants failed', attempts: result.tried });

    const { data } = result;
    res.json({
      statusFieldUsed: result.statusFieldUsed,
      query: result.query,
      count: Array.isArray(data.value) ? data.value.length : 0,
      odataCount: data['@odata.count'],
      value: data.value || []
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Catch-all -> JSON 404 (so you never see Google’s HTML if it’s your service)
app.use((req, res) => {
  res.status(404).json({ error: 'Route not found', path: req.path, try: ['/', '/healthz', '/__routes', '/listings'] });
});

// ---- Start ----
app.listen(PORT, '0.0.0.0', () => {
  const cidSet = Boolean(process.env.TRESTLE_CLIENT_ID);
  const csSet = Boolean(process.env.TRESTLE_CLIENT_SECRET);
  console.log(`✅ Listening on ${PORT}`);
  console.log(`ENV: TRESTLE_CLIENT_ID set? ${cidSet} | TRESTLE_CLIENT_SECRET set? ${csSet}`);
});
