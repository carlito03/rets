// server.js  — Node 18+ (uses global fetch)
const express = require('express');
const app = express();

const PORT = process.env.PORT || 8080;
const TRESTLE_HOST = process.env.TRESTLE_HOST || 'https://api-prod.corelogic.com';
const TOKEN_URL = `${TRESTLE_HOST}/trestle/oidc/connect/token`;

// =================== TOKEN MANAGEMENT (rets + api) ===================
const tokenCache = {
  api: { token: null, exp: 0 },
  rets: { token: null, exp: 0 }
};

async function getAccessToken(scope = 'api') {
  const now = Math.floor(Date.now() / 1000);
  const cache = tokenCache[scope] || tokenCache.api;
  if (cache.token && cache.exp - 60 > now) return cache.token;

  const cid = process.env.TRESTLE_CLIENT_ID;
  const csec = process.env.TRESTLE_CLIENT_SECRET;
  if (!cid || !csec) throw new Error('Missing TRESTLE_CLIENT_ID / TRESTLE_CLIENT_SECRET');

  const basic = Buffer.from(`${cid}:${csec}`).toString('base64');
  const body = new URLSearchParams({ grant_type: 'client_credentials', scope });

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
  cache.token = data.access_token;
  cache.exp = now + (typeof data.expires_in === 'number' ? data.expires_in : 3600);
  return cache.token;
}

// =================== COMMON ===================
app.get('/', (_req, res) => res.send('Hello from Cloud Run + Node.js! Trestle proxy is running.'));
app.get('/healthz', (_req, res) => res.json({ ok: true }));

// Dev-only: view token (scope param: api|rets)
app.get('/auth/token', async (req, res) => {
  try {
    const scope = req.query.scope === 'rets' ? 'rets' : 'api';
    const token = await getAccessToken(scope);
    res.json({ scope, access_token: token });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// =================== ODATA (WEB API) ===================
// Helpers to build & fetch OData and page @odata.nextLink
function odataBaseUrl() {
  return `${TRESTLE_HOST}/trestle/odata/Property`;
}

function buildParams({ select, expand, top = 1000, filter, orderby = 'ListingKey' }) {
  const p = new URLSearchParams();
  p.set('$count', 'true');
  p.set('$orderby', orderby);
  p.set('$top', String(Math.min(Number(top) || 1000, 1000)));
  if (select) p.set('$select', select);
  if (expand) p.set('$expand', expand);
  if (filter) p.set('$filter', filter);
  return p;
}

async function fetchODataPage(url) {
  const token = await getAccessToken('api');
  const r = await fetch(url, {
    headers: { Authorization: `Bearer ${token}`, Accept: 'application/json' }
  });
  if (!r.ok) {
    const t = await r.text();
    throw new Error(`OData fetch failed ${r.status} ${r.statusText} - ${t}`);
  }
  return r.json();
}

async function fetchAllPages(url, maxPages = 100) {
  const all = [];
  let next = url;
  let pages = 0;
  while (next && pages < maxPages) {
    const page = await fetchODataPage(next);
    const items = Array.isArray(page.value) ? page.value : [];
    all.push(...items);
    next = page['@odata.nextLink'] || null;
    pages++;
  }
  return all;
}

// Your EXACT filter (Apr-2025 onward, inclusive)
const FILTER_FROM_APR_2025_CCARD =
  "(year(ListingContractDate) gt 2025 or (year(ListingContractDate) eq 2025 and month(ListingContractDate) ge 4)) and OriginatingSystemName eq 'CRMLS'";

// Example route using your filter + Media expansion
app.get('/odata/crmls-from-apr-2025', async (req, res) => {
  try {
    const top = req.query.top || 1000;
    // You can trim Media payload by using nested $select: Media($select=MediaURL,Order)
    const params = buildParams({
      top,
      select:
        'ListingKey,ListingContractDate,OriginatingSystemName,StreetName,PhotosCount,ModificationTimestamp,PhotosChangeTimestamp',
      expand: 'Media',
      filter: FILTER_FROM_APR_2025_CCARD
    });

    const url = `${odataBaseUrl()}?${params.toString()}`;
    const listings = await fetchAllPages(url);
    res.json({ count: listings.length, listings });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Generic OData search if you want to pass filter yourself
app.get('/odata/search', async (req, res) => {
  try {
    const { filter, select, expand, top, orderby } = req.query;
    const params = buildParams({ filter, select, expand, top, orderby });
    const url = `${odataBaseUrl()}?${params.toString()}`;
    const listings = await fetchAllPages(url);
    res.json({ count: listings.length, listings });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// =================== (Optional) RETS bits can stay here if you were using them ===================
// ... (omit for brevity; your earlier /rets routes can remain unchanged)

// =================== START ===================
app.listen(PORT, () => console.log(`Listening on ${PORT}`));

