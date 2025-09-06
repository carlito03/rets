// server.js — RESO Web API (OData) helpers + routes
// Node 18+ (uses global fetch). Only dependency: express.
const express = require('express');
const app = express();

const PORT = process.env.PORT || 8080;

// ======== Trestle config ========
const TRESTLE_HOST = process.env.TRESTLE_HOST || 'https://api-trestle.corelogic.com';
const TOKEN_URL = `${TRESTLE_HOST}/trestle/oidc/connect/token`;

// This file focuses on RESO Web API (scope=api)
const DEFAULT_SCOPE = process.env.TRESTLE_SCOPE || 'api';

// ======== Token cache ========
let cached = { token: null, exp: 0, scope: null };

async function getAccessToken(scope = DEFAULT_SCOPE) {
  const now = Math.floor(Date.now() / 1000);
  if (cached.token && cached.scope === scope && cached.exp - 60 > now) return cached.token;

  const id = process.env.TRESTLE_CLIENT_ID;
  const secret = process.env.TRESTLE_CLIENT_SECRET;
  if (!id || !secret) throw new Error('Missing TRESTLE_CLIENT_ID / TRESTLE_CLIENT_SECRET');

  const basic = Buffer.from(`${id}:${secret}`).toString('base64');
  const body = new URLSearchParams();
  body.set('grant_type', 'client_credentials');
  body.set('scope', scope);

  const resp = await fetch(TOKEN_URL, {
    method: 'POST',
    headers: {
      Authorization: `Basic ${basic}`,
      'Content-Type': 'application/x-www-form-urlencoded',
      Accept: 'application/json'
    },
    body
  });
  if (!resp.ok) throw new Error(`Token request failed: ${resp.status} ${await resp.text()}`);

  const data = await resp.json();
  cached = {
    token: data.access_token,
    exp: Math.floor(Date.now() / 1000) + (data.expires_in || 3600),
    scope
  };
  return cached.token;
}

// ======== Small utils ========

// double single quotes per OData string rules
const odString = s => String(s).replace(/'/g, "''");

// nextLink may be absolute or relative
function toAbsoluteNextLink(link) {
  if (!link) return null;
  if (/^https?:\/\//i.test(link)) return link;
  // ensure exactly one slash when joining
  if (link.startsWith('/')) return `${TRESTLE_HOST}${link}`;
  return `${TRESTLE_HOST}/${link}`;
}

// Fetch one OData URL and parse JSON
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

// Fetch /trestle/odata/<resource>?<params>, loop @odata.nextLink
async function fetchODataPaged(resource, params) {
  const token = await getAccessToken('api');
  const baseUrl = `${TRESTLE_HOST}/trestle/odata/${resource}?${params.toString()}`;
  const total = [];
  let url = baseUrl;
  let count = null;

  while (url) {
    const page = await fetchODataUrl(url, token);
    if (count == null && typeof page['@odata.count'] === 'number') count = page['@odata.count'];

    if (Array.isArray(page.value)) total.push(...page.value);

    const next = toAbsoluteNextLink(page['@odata.nextLink']);
    url = next || null;

    // guard rail so we don't loop forever if something goes weird
    if (total.length > 1_000_000) throw new Error('Aborting: >1,000,000 rows accumulated.');
  }

  return { count: count ?? total.length, results: total };
}

// ======== Basic routes ========
app.get('/', (_req, res) => res.send('Hello from Cloud Run + Node.js (RESO Web API)!'));
app.get('/healthz', (_req, res) => res.json({ ok: true }));

// Dev helper: mint a token (scope defaults to api). Disable/protect in prod.
app.get('/auth/token', async (req, res) => {
  try {
    const scope = req.query.scope || 'api';
    const token = await getAccessToken(scope);
    res.json({ scope, access_token: token });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ======== RESO Web API: metadata ========
app.get('/webapi/metadata', async (_req, res) => {
  try {
    const token = await getAccessToken('api');
    const url = `${TRESTLE_HOST}/trestle/odata/$metadata`;
    const r = await fetch(url, {
      headers: { Authorization: `Bearer ${token}`, Accept: 'application/xml' }
    });
    const xml = await r.text();
    if (!r.ok) return res.status(r.status).send(xml);
    res.type('application/xml').send(xml);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ======== RESO Web API: simple Property by city ========
// GET /webapi/property/by-city?city=San%20Dimas&top=1000&expandMedia=1
app.get('/webapi/property/by-city', async (req, res) => {
  try {
    const city = req.query.city;
    if (!city) return res.status(400).json({ error: 'city query param is required' });

    const top = Math.min(parseInt(req.query.top || '1000', 10), 1000);
    const expandMedia = req.query.expandMedia === '1';

    const params = new URLSearchParams();
    params.set('$count', 'true');
    params.set('$top', String(top));
    params.set('$orderby', 'ListingKey'); // stable order helps paging
    params.set(
      '$select',
      [
        'ListingKey',
        'ListingContractDate',
        'OriginatingSystemName',
        'StreetName',
        'City',
        'StateOrProvince',
        'PostalCode',
        'PhotosCount',
        'ModificationTimestamp',
        'PhotosChangeTimestamp'
      ].join(',')
    );
    if (expandMedia) {
      // trim Media payload to what we actually need
      params.set('$expand', "Media($select=MediaURL,Order,ResourceRecordKey)");
    }
    params.set('$filter', `City eq '${odString(city)}'`);

    const { count, results } = await fetchODataPaged('Property', params);
    res.json({ count, listings: results });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ======== RESO Web API: from Apr-2025 onward + OriginatingSystemName = CRMLS ========
// GET /webapi/property/crmls-from-apr-2025?top=1000&expandMedia=0
app.get('/webapi/property/crmls-from-apr-2025', async (req, res) => {
  try {
    const top = Math.min(parseInt(req.query.top || '1000', 10), 1000);
    const expandMedia = req.query.expandMedia === '1';

    const params = new URLSearchParams();
    params.set('$count', 'true');
    params.set('$top', String(top));
    params.set('$orderby', 'ListingKey');
    params.set(
      '$select',
      [
        'ListingKey',
        'ListingContractDate',
        'OriginatingSystemName',
        'StreetName',
        'City',
        'StateOrProvince',
        'PostalCode',
        'PhotosCount',
        'ModificationTimestamp',
        'PhotosChangeTimestamp'
      ].join(',')
    );
    if (expandMedia) {
      params.set('$expand', "Media($select=MediaURL,Order,ResourceRecordKey)");
    }

    // robust: "from 2025-04 onward" while scoping to CRMLS
    const filter =
      "(year(ListingContractDate) gt 2025 or " +
      "(year(ListingContractDate) eq 2025 and month(ListingContractDate) ge 4)) " +
      "and OriginatingSystemName eq 'CRMLS'";
    params.set('$filter', filter);

    const { count, results } = await fetchODataPaged('Property', params);
    res.json({ count, listings: results });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ======== RESO Web API: Media URLs for a Listing ========
// GET /webapi/media/urls/:listingKey
app.get('/webapi/media/urls/:listingKey', async (req, res) => {
  try {
    const listingKey = req.params.listingKey;
    if (!listingKey) return res.status(400).json({ error: 'listingKey is required' });

    const params = new URLSearchParams();
    params.set('$count', 'true');
    params.set('$top', '1000'); // 1000 is fine here; usually far fewer media rows per listing
    params.set('$orderby', 'Order');
    params.set('$select', 'MediaURL,Order,ResourceRecordKey,MediaKey');
    params.set('$filter', `ResourceRecordKey eq '${odString(listingKey)}'`);

    const { count, results } = await fetchODataPaged('Media', params);
    // Keep just ordered URLs (and include order for convenience)
    const media = results
      .filter(x => x.MediaURL)
      .map(x => ({ order: x.Order, url: x.MediaURL, mediaKey: x.MediaKey }));
    res.json({ count, media });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ======== start ========
app.listen(PORT, () => console.log(`Listening on ${PORT}`));
