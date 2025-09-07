// server.js — Minimal RESO auth + metadata + by-city
// Node 18+ (global fetch). Only dependency: express.
const express = require('express');
const app = express();

const PORT = process.env.PORT || 8080;

// --- Config ---
const TRESTLE_HOST  = process.env.TRESTLE_HOST  || 'https://api-trestle.corelogic.com';
const TOKEN_URL     = `${TRESTLE_HOST}/trestle/oidc/connect/token`;
const TRESTLE_SCOPE = process.env.TRESTLE_SCOPE || 'api';
const DEFAULT_OSN   = process.env.MLS_OSN || ''; // e.g., CRMLS

// --- Token cache ---
let tokenCache = { access_token: null, expires_at: 0 };

async function fetchAccessToken() {
  const client_id = process.env.TRESTLE_CLIENT_ID;
  const client_secret = process.env.TRESTLE_CLIENT_SECRET;
  if (!client_id || !client_secret) throw new Error('Missing TRESTLE_CLIENT_ID or TRESTLE_CLIENT_SECRET');

  // reuse if > 60s remaining
  const now = Date.now();
  if (tokenCache.access_token && tokenCache.expires_at - 60_000 > now) {
    return tokenCache.access_token;
  }

  const form = new URLSearchParams();
  form.set('client_id', client_id);
  form.set('client_secret', client_secret);
  form.set('grant_type', 'client_credentials');
  form.set('scope', TRESTLE_SCOPE);

  const resp = await fetch(TOKEN_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'Accept': 'application/json' },
    body: form
  });

  const text = await resp.text();
  if (!resp.ok) throw new Error(`Token request failed: ${resp.status} ${resp.statusText} - ${text}`);

  const json = JSON.parse(text);
  const expiresIn = typeof json.expires_in === 'number' ? json.expires_in : 3600;
  tokenCache = { access_token: json.access_token, expires_at: Date.now() + expiresIn * 1000 };
  return json.access_token;
}

// --- Small helpers ---
const odString = (s) => String(s).replace(/'/g, "''"); // escape single quotes for OData

function toAbsoluteNextLink(link) {
  if (!link) return null;
  if (/^https?:\/\//i.test(link)) return link;
  if (link.startsWith('/')) return `${TRESTLE_HOST}${link}`;
  return `${TRESTLE_HOST}/${link}`;
}

async function fetchODataUrl(url, token) {
  const r = await fetch(url, {
    headers: {
      Authorization: `Bearer ${token}`,
      Accept: 'application/json',
      'User-Agent': 'IdeasPlusActions/1.0'
    }
  });
  const text = await r.text();
  if (!r.ok) throw new Error(`OData request failed: ${r.status} ${text}`);
  return JSON.parse(text);
}

// Loops @odata.nextLink to aggregate all pages
async function fetchODataPaged(resource, params) {
  const token = await fetchAccessToken();
  const baseUrl = `${TRESTLE_HOST}/trestle/odata/${resource}?${params.toString()}`;
  const total = [];
  let url = baseUrl;
  let count = null;

  while (url) {
    const page = await fetchODataUrl(url, token);
    if (count == null && typeof page['@odata.count'] === 'number') count = page['@odata.count'];
    if (Array.isArray(page.value)) total.push(...page.value);
    url = toAbsoluteNextLink(page['@odata.nextLink']);
    if (total.length > 1_000_000) throw new Error('Aborting: >1,000,000 rows accumulated.');
  }

  return { count: count ?? total.length, results: total };
}

// --- Routes ---
app.get('/', (_req, res) => res.send('RESO Web API: /auth/token, /webapi/metadata, /webapi/property/by-city'));

app.get('/healthz', (_req, res) => res.json({ ok: true }));

// Token (debug)
app.get('/auth/token', async (_req, res) => {
  try {
    const token = await fetchAccessToken();
    const ttl = Math.max(0, Math.floor((tokenCache.expires_at - Date.now()) / 1000));
    res.json({ token_type: 'Bearer', scope: TRESTLE_SCOPE, expires_in: ttl, access_token: token });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: String(err.message || err) });
  }
});

// Metadata (XML)
app.get('/webapi/metadata', async (_req, res) => {
  try {
    const token = await fetchAccessToken();
    const url = `${TRESTLE_HOST}/trestle/odata/$metadata`;
    const r = await fetch(url, { headers: { Authorization: `Bearer ${token}`, Accept: 'application/xml' } });
    const xml = await r.text();
    if (!r.ok) return res.status(r.status).send(xml);
    res.type('application/xml').send(xml);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: String(err.message || err) });
  }
});

// --- Simple by-city ---
// GET /webapi/property/by-city?city=San%20Dimas&top=1000
// Optional: &osn=CRMLS (overrides MLS_OSN env var)
app.get('/webapi/property/by-city', async (req, res) => {
  try {
    const city = req.query.city;
    if (!city) return res.status(400).json({ error: 'city query param is required' });

    const osn = (req.query.osn || DEFAULT_OSN || '').trim(); // strongly recommended by Trestle
    const top = Math.min(parseInt(req.query.top || '1000', 10), 1000);

    const params = new URLSearchParams();
    params.set('$count', 'true');
    params.set('$top', String(top));
    // keep a minimal, safe select based on your metadata
    params.set('$select', 'ListingKey,City,StateOrProvince,PostalCode');

    // Build filter
    let filter = `City eq '${odString(city)}'`;
    if (osn) filter = `(OriginatingSystemName eq '${odString(osn)}') and ${filter}`;
    params.set('$filter', filter);

    const { count, results } = await fetchODataPaged('Property', params);
    res.json({ query: { city, osn: osn || null, top }, count, listings: results });
  } catch (err) {
    // Return upstream OData error text to make debugging easier
    res.status(500).json({ error: String(err.message || err) });
  }
});

app.listen(PORT, () => console.log(`Listening on ${PORT}`));
