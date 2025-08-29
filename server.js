// server.js
// Node 18+ (uses global fetch). Only dependency: express.
const express = require('express');
const app = express();

const PORT = process.env.PORT || 8080;

// ----- Trestle config -----
const TRESTLE_HOST = process.env.TRESTLE_HOST || 'https://api-prod.corelogic.com';
const TOKEN_URL = `${TRESTLE_HOST}/trestle/oidc/connect/token`;
const SCOPE = process.env.TRESTLE_SCOPE || 'rets'; // << ensure 'rets' for RETS calls

// ----- Token cache -----
let cachedToken = null;
let tokenExpiresAt = 0; // epoch seconds

async function getAccessToken() {
  const now = Math.floor(Date.now() / 1000);
  if (cachedToken && tokenExpiresAt - 60 > now) return cachedToken;

  const clientId = process.env.TRESTLE_CLIENT_ID;
  const clientSecret = process.env.TRESTLE_CLIENT_SECRET;
  if (!clientId || !clientSecret) {
    throw new Error('Missing TRESTLE_CLIENT_ID/TRESTLE_CLIENT_SECRET env vars');
  }

  const basic = Buffer.from(`${clientId}:${clientSecret}`).toString('base64');
  const body = new URLSearchParams();
  body.set('grant_type', 'client_credentials');
  body.set('scope', SCOPE);

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
    throw new Error(`Token request failed: ${resp.status} ${resp.statusText} - ${text}`);
  }

  const data = await resp.json();
  cachedToken = data.access_token;
  const expiresIn = typeof data.expires_in === 'number' ? data.expires_in : 3600;
  tokenExpiresAt = now + expiresIn;
  return cachedToken;
}

// ----- Helpers -----

// Parse RETS COMPACT-DECODED into array of objects
function parseCompactDecoded(text) {
  const colMatch = text.match(/<COLUMNS>([^<]*)<\/COLUMNS>/);
  if (!colMatch) return [];
  const cols = colMatch[1].split('\t').map(s => s.trim()).filter(Boolean);

  const rows = [];
  const dataRe = /<DATA>([^<]*)<\/DATA>/g;
  let m;
  while ((m = dataRe.exec(text)) !== null) {
    const vals = m[1].split('\t');
    const obj = {};
    for (let i = 0; i < cols.length; i++) obj[cols[i]] = vals[i] ?? null;
    rows.push(obj);
  }
  return rows;
}

// Build a DMQL2 query string from typical filters
function buildDmql2({ status, city, cities, minPrice, maxPrice }) {
  const parts = [];
  if (status) parts.push(`(StandardStatus=|${status})`);

  // City: pass either a single city (city) or CSV (cities)
  if (cities) {
    const list = cities.split(',').map(s => s.trim()).filter(Boolean);
    if (list.length) parts.push(`(City=|${list.join(',')})`);
  } else if (city) {
    parts.push(`(City=${city})`);
  }

  if (minPrice || maxPrice) {
    const lo = minPrice || 0;
    const hi = maxPrice || '';
    parts.push(`(ListPrice=${lo}-${hi})`);
  }

  // Join with comma = AND
  return parts.length ? parts.join(',') : '(StandardStatus=|Active)';
}

// Shared RETS search fetcher
async function retsSearch({ select, limit = 10, dmqlQuery }) {
  const token = await getAccessToken();
  const params = new URLSearchParams({
    SearchType: 'Property',
    Class: 'Property',
    QueryType: 'DMQL2',
    Query: dmqlQuery,
    Format: 'COMPACT-DECODED',
    Limit: String(Math.min(Number(limit) || 10, 1000)),
    Select: select || 'ListingKey,ListingId,ListPrice,StandardStatus,City,StateOrProvince,PostalCode'
  });

  const url = `${TRESTLE_HOST}/trestle/rets/search?${params.toString()}`;
  const resp = await fetch(url, {
    headers: {
      Authorization: `Bearer ${token}`,
      'RETS-Version': 'RETS/1.8',
      Accept: 'text/plain'
    }
  });
  const body = await resp.text();
  if (!resp.ok) {
    const err = new Error(`RETS search failed: ${resp.status}`);
    err.body = body;
    throw err;
  }
  return parseCompactDecoded(body);
}

// ----- Routes -----
app.get('/', (_req, res) =>
  res.send('Hello from Cloud Run + Node.js! Trestle RETS proxy.')
);

app.get('/healthz', (_req, res) => res.json({ ok: true }));

// ⚠️ Dev-only: shows token info. Disable or protect in prod.
app.get('/auth/token', async (_req, res) => {
  try {
    const token = await getAccessToken();
    res.json({ access_token: token, token_type: 'Bearer', expires_at: tokenExpiresAt });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Generic RETS search (accepts query params)
app.get('/rets/search', async (req, res) => {
  try {
    const dmql = buildDmql2({
      status: req.query.status || 'Active',
      city: req.query.city,
      cities: req.query.cities,
      minPrice: req.query.minPrice,
      maxPrice: req.query.maxPrice
    });
    const limit = req.query.limit || 10;
    const select = req.query.select;
    const results = await retsSearch({ select, limit, dmqlQuery: dmql });
    res.json({ query: dmql, count: results.length, results });
  } catch (e) {
    res
      .status(500)
      .json({ error: e.message, body: e.body });
  }
});

// Example 1: Active in San Dimas
app.get('/rets/examples/active-sandimas', async (_req, res) => {
  try {
    const dmql = buildDmql2({ status: 'Active', city: 'San Dimas' });
    const results = await retsSearch({ limit: 10, dmqlQuery: dmql });
    res.json({ query: dmql, count: results.length, results });
  } catch (e) {
    res.status(500).json({ error: e.message, body: e.body });
  }
});

// Example 2: Price range (e.g., 300k–800k), Active anywhere
app.get('/rets/examples/price-range', async (req, res) => {
  try {
    const min = req.query.min || '300000';
    const max = req.query.max || '800000';
    const dmql = buildDmql2({ status: 'Active', minPrice: min, maxPrice: max });
    const results = await retsSearch({ limit: 10, dmqlQuery: dmql });
    res.json({ query: dmql, count: results.length, results });
  } catch (e) {
    res.status(500).json({ error: e.message, body: e.body });
  }
});

// Example 3: Active in Covina OR San Dimas
app.get('/rets/examples/active-covina-sandimas', async (_req, res) => {
  try {
    const dmql = buildDmql2({ status: 'Active', cities: 'Covina,San Dimas' });
    const results = await retsSearch({ limit: 10, dmqlQuery: dmql });
    res.json({ query: dmql, count: results.length, results });
  } catch (e) {
    res.status(500).json({ error: e.message, body: e.body });
  }
});

// Debug: raw RETS response (handy if you want to see <COLUMNS>/<DATA>)
app.get('/rets/search/raw', async (_req, res) => {
  try {
    const token = await getAccessToken();
    const params = new URLSearchParams({
      SearchType: 'Property',
      Class: 'Property',
      QueryType: 'DMQL2',
      Query: '(StandardStatus=|Active)',
      Format: 'COMPACT-DECODED',
      Limit: '5',
      Select: 'ListingKey,ListingId,ListPrice,StandardStatus'
    });
    const url = `${TRESTLE_HOST}/trestle/rets/search?${params.toString()}`;
    const r = await fetch(url, {
      headers: {
        Authorization: `Bearer ${token}`,
        'RETS-Version': 'RETS/1.8',
        Accept: 'text/plain'
      }
    });
    const txt = await r.text();
    res.type('text/plain').send(txt);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ----- Photos -----
// GET /rets/photo/:listingKey?n=1  -> streams primary photo by default
app.get('/rets/photo/:listingKey', async (req, res) => {
  try {
    const token = await getAccessToken();
    const listingKey = req.params.listingKey;
    const n = String(req.query.n || '1'); // 1 = first photo; some boards use 0 for primary

    // Step 1: ask for Location=1 so the server returns a URL to the photo
    const params = new URLSearchParams({
      Resource: 'Property',
      Type: 'Photo',
      ID: `${listingKey}:${n}`,
      Location: '1'
    });
    const url = `${TRESTLE_HOST}/trestle/rets/getobject?${params.toString()}`;
    const resp = await fetch(url, {
      headers: {
        Authorization: `Bearer ${token}`,
        'RETS-Version': 'RETS/1.8',
        Accept: '*/*'
      }
    });

    if (!resp.ok) {
      const text = await resp.text();
      return res.status(resp.status).send(text);
    }

    // Try to extract "Location:" header from the (likely) multipart response
    const ct = resp.headers.get('content-type') || '';
    const buf = Buffer.from(await resp.arrayBuffer());

    let photoUrl = null;
    if (/^multipart\//i.test(ct)) {
      // Parse headers of the first part; Location header contains the URL
      const boundary = (ct.match(/boundary="?([^=";]+)"?/) || [])[1];
      if (!boundary) throw new Error('No boundary in multipart response');
      const boundaryMarker = `--${boundary}`;
      const raw = buf.toString('utf8'); // safe because body is tiny when Location=1
      const firstPartStart = raw.indexOf(boundaryMarker);
      const headerStart = raw.indexOf('\r\n', firstPartStart) + 2;
      const headerEnd = raw.indexOf('\r\n\r\n', headerStart);
      const headerText = raw.slice(headerStart, headerEnd);
      const locMatch = headerText.match(/Location:\s*(.+)\r?\n/i);
      if (locMatch) photoUrl = locMatch[1].trim();
    } else {
      // Some servers might return the actual image directly
      if ((ct || '').startsWith('image/')) {
        res.setHeader('Content-Type', ct);
        return res.send(buf);
      }
    }

    if (!photoUrl) {
      // Fallback: ask for binary (Location=0) and stream directly
      const p2 = new URLSearchParams({
        Resource: 'Property',
        Type: 'Photo',
        ID: `${listingKey}:${n}`,
        Location: '0'
      });
      const url2 = `${TRESTLE_HOST}/trestle/rets/getobject?${p2.toString()}`;
      const r2 = await fetch(url2, {
        headers: {
          Authorization: `Bearer ${token}`,
          'RETS-Version': 'RETS/1.8',
          Accept: 'image/*'
        }
      });
      const ct2 = r2.headers.get('content-type') || 'image/jpeg';
      const b2 = Buffer.from(await r2.arrayBuffer());
      res.setHeader('Content-Type', ct2);
      return res.send(b2);
    }

    // Step 2: fetch actual image URL and stream to client
    const img = await fetch(photoUrl);
    if (!img.ok) {
      const txt = await img.text();
      return res.status(img.status).send(txt);
    }
    res.setHeader('Content-Type', img.headers.get('content-type') || 'image/jpeg');
    const arr = Buffer.from(await img.arrayBuffer());
    return res.send(arr);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ----- Start -----
app.listen(PORT, () => {
  console.log(`Listening on ${PORT}`);
});
