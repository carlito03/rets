// server.js  — Node 18+ (uses global fetch)
const express = require('express');
const app = express();

const PORT = process.env.PORT || 8080;
const TRESTLE_HOST = process.env.TRESTLE_HOST || 'https://api-prod.corelogic.com';
const TOKEN_URL = `${TRESTLE_HOST}/trestle/oidc/connect/token`;

// =============== OAuth token (scope=api) =================
let cached = { token: null, exp: 0 }; // epoch seconds

async function getApiToken() {
  const now = Math.floor(Date.now() / 1000);
  if (cached.token && cached.exp - 60 > now) return cached.token;

  const cid = process.env.TRESTLE_CLIENT_ID;
  const csec = process.env.TRESTLE_CLIENT_SECRET;
  if (!cid || !csec) throw new Error('Missing TRESTLE_CLIENT_ID / TRESTLE_CLIENT_SECRET');

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
  cached.token = data.access_token;
  cached.exp = now + (typeof data.expires_in === 'number' ? data.expires_in : 3600);
  return cached.token;
}

// =============== Helpers =================
const ODATA_BASE = `${TRESTLE_HOST}/trestle/odata/Property`;

// Your C#-style pieces (translated to params):
const SELECT_FIELDS =
  'ListingKey,ListingContractDate,OriginatingSystemName,StreetName,PhotosCount,ModificationTimestamp,PhotosChangeTimestamp,Media';
const EXPAND = 'Media';
const TOP = 1000;
// Filter exactly like your screenshot (year >= 2020, month >= 04, system = 'CCAR')
const FILTER =
  "year(ListingContractDate) ge 2025 and month(ListingContractDate) ge 4 and OriginatingSystemName eq 'CRMLS'";

// Build querystring safely
function buildParams({ select, expand, top, filter, orderby = 'ListingKey' }) {
  const p = new URLSearchParams();
  p.set('$count', 'true');
  p.set('$orderby', orderby);       // OData uses $orderby (not "orderBy")
  p.set('$top', String(Math.min(Number(top) || TOP, 1000))); // enforce 1000 cap
  if (select) p.set('$select', select);
  if (expand) p.set('$expand', expand);
  if (filter) p.set('$filter', filter);
  return p;
}

async function fetchODataPage(url) {
  const token = await getApiToken();
  const r = await fetch(url, { headers: { Authorization: `Bearer ${token}`, Accept: 'application/json' } });
  if (!r.ok) {
    const t = await r.text();
    throw new Error(`OData fetch failed ${r.status} ${r.statusText} - ${t}`);
  }
  return r.json();
}

async function fetchAllPages(url, maxPages = 100) {
  const totalListings = [];
  let next = url;
  let pages = 0;
  let firstCount = null;

  while (next && pages < maxPages) {
    const page = await fetchODataPage(next);
    if (firstCount == null && typeof page['@odata.count'] === 'number') firstCount = page['@odata.count'];
    if (Array.isArray(page.value)) totalListings.push(...page.value);
    next = page['@odata.nextLink'] || null;
    pages++;
  }
  return { odataCount: firstCount, listings: totalListings, pages };
}

// =============== Routes =================
app.get('/', (_req, res) => res.send('Hello from Cloud Run + Node.js! Web API (OData) proxy running.'));
app.get('/healthz', (_req, res) => res.json({ ok: true }));

// Dev-only: inspect a token
app.get('/auth/token', async (_req, res) => {
  try {
    const token = await getApiToken();
    res.json({ scope: 'api', access_token: token });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// The exact example query (select + expand=Media + top=1000 + your filter)
// Aggregates all pages into one array (like the C# totalListings).
app.get('/odata/example', async (_req, res) => {
  try {
    const params = buildParams({
      select: SELECT_FIELDS,
      expand: EXPAND,
      top: TOP,
      filter: FILTER
    });
    const url = `${ODATA_BASE}?${params.toString()}`;
    const { odataCount, listings, pages } = await fetchAllPages(url);
    res.json({ query: Object.fromEntries(params.entries()), pages, odataCount, count: listings.length, listings });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Tiny debug route: first page only (no paging), useful to verify shape fast
app.get('/odata/example/one', async (_req, res) => {
  try {
    const params = buildParams({
      select: SELECT_FIELDS,
      expand: EXPAND,
      top: 1,
      filter: FILTER
    });
    const url = `${ODATA_BASE}?${params.toString()}`;
    const page = await fetchODataPage(url);
    res.json(page);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.listen(PORT, () => console.log(`Listening on ${PORT}`));
