// server.js — CommonJS, Node 18+ (uses global fetch), Express only
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
function buildParams({ select, top = 50, filter, orderby = 'ModificationTimestamp desc' }) {
  const p = new URLSearchParams();
  p.set('$count', 'true');
  p.set('$top', String(Math.min(Number(top) || 50, 1000))); // hard cap at 1000
  p.set('$orderby', orderby);
  if (select) p.set('$select', select);
  if (filter) p.set('$filter', filter);
  return p;
}

async function fetchOData(url) {
  const token = await getApiToken();
  const r = await fetch(url, {
    headers: {
      Authorization: `Bearer ${token}`,
      Accept: 'application/json'
    }
  });
  if (!r.ok) {
    const t = await r.text();
    throw new Error(`OData fetch failed ${r.status} ${r.statusText} - ${t}`);
  }
  return r.json();
}

// ---- Routes ----
app.get('/', (_req, res) => {
  res.send('CRMLS OData proxy is running on Cloud Run ✅');
});

app.get('/healthz', (_req, res) => res.json({ ok: true }));

// Dev-only: inspect token (mask in logs; safe JSON response)
app.get('/auth/token', async (_req, res) => {
  try {
    const tok = await getApiToken();
    res.json({ scope: 'api', access_token: tok ? tok.slice(0, 16) + '…' : null, note: 'masked' });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

/**
 * GET /listings
 * Query city-scoped, ad-hoc results to stay well under the 5k limit.
 * Query params:
 *   city        (default: Long Beach)
 *   top         (default: 50, max: 1000)
 *   minPrice    (optional)
 *   maxPrice    (optional)
 *   status      (default: Active) - often MlsStatus or StandardStatus depending on feed
 *
 * Example:
 *   /listings?city=Long%20Beach&minPrice=700000&status=Active&top=25
 */
app.get('/listings', async (req, res) => {
  try {
    const city = (req.query.city || 'Long Beach').trim();
    const top = Math.min(parseInt(req.query.top || '50', 10) || 50, 1000);
    const minPrice = req.query.minPrice ? parseInt(req.query.minPrice, 10) : null;
    const maxPrice = req.query.maxPrice ? parseInt(req.query.maxPrice, 10) : null;
    const status = (req.query.status || 'Active').trim();

    // Build a safe FILTER without OriginatingSystemName (per CRMLS docs/limits)
    const filters = [`City eq '${city.replace(/'/g, "''")}'`]; // escape single quotes

    // Many CRMLS feeds expose MlsStatus; if yours uses StandardStatus, swap as needed:
    if (status) filters.push(`MlsStatus eq '${status.replace(/'/g, "''")}'`);

    if (Number.isFinite(minPrice)) filters.push(`ListPrice ge ${minPrice}`);
    if (Number.isFinite(maxPrice)) filters.push(`ListPrice le ${maxPrice}`);

    // Example: also allow a recency cut to keep results small (optional)
    // const isoCut = new Date(Date.now() - 1000 * 60 * 60 * 24 * 60).toISOString(); // last 60 days
    // filters.push(`ModificationTimestamp ge ${isoCut}`);

    const FILTER = filters.join(' and ');

    // Keep SELECT lean and universal
    const SELECT_FIELDS = [
      'ListingKeyNumeric',
      'ListingId',
      'City',
      'PostalCode',
      'ListPrice',
      'PropertyType',
      'MlsStatus',
      'ModificationTimestamp'
    ].join(',');

    const params = buildParams({
      select: SELECT_FIELDS,
      top,
      filter: FILTER,
      orderby: 'ModificationTimestamp desc'
    });

    const url = `${ODATA_BASE}?${params.toString()}`;
    const data = await fetchOData(url);

    res.json({
      query: Object.fromEntries(params.entries()),
      count: Array.isArray(data.value) ? data.value.length : 0,
      odataCount: data['@odata.count'],
      value: data.value || []
    });
  } catch (e) {
    // Always return JSON so PowerShell/curl|jq don't choke
    res.status(500).json({ error: e.message });
  }
});

// ---- Start ----
app.listen(PORT, '0.0.0.0', () => {
  const cidSet = Boolean(process.env.TRESTLE_CLIENT_ID);
  const csSet = Boolean(process.env.TRESTLE_CLIENT_SECRET);
  console.log(`✅ Listening on ${PORT}`);
  console.log(`ENV: TRESTLE_CLIENT_ID set? ${cidSet} | TRESTLE_CLIENT_SECRET set? ${csSet}`);
});
