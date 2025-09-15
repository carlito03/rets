// server.js
// Minimal Cloud Run API with CORS + API key auth for Trestle WebAPI

const express = require('express');
const cors = require('cors');
const fetch = (...args) => import('node-fetch').then(({ default: f }) => f(...args));

const app = express();

/* -------------------------- Config via env vars -------------------------- */
const {
  PORT = 8080,

  // Trestle creds (keep in Secret Manager -> env var)
  TRESTLE_CLIENT_ID,
  TRESTLE_CLIENT_SECRET,

  // CORS allowlist: comma-separated, e.g. "https://client1.com,https://client2.com"
  ALLOWED_ORIGINS = '*',

  // API keys: comma-separated list, e.g. "k1,k2,k3"
  API_KEYS = '',

  // Optional: tweak selection / enum prettifying
  PRETTY_ENUMS = 'true'
} = process.env;

if (!TRESTLE_CLIENT_ID || !TRESTLE_CLIENT_SECRET) {
  console.error('Missing TRESTLE_CLIENT_ID / TRESTLE_CLIENT_SECRET');
}

const API_KEY_SET = new Set(
  API_KEYS
    .split(',')
    .map(s => s.trim())
    .filter(Boolean)
);

const ODATA_BASE = 'https://api-trestle.corelogic.com/trestle/odata';
const TOKEN_URL = 'https://api-trestle.corelogic.com/trestle/oidc/connect/token';

/* ------------------------------- CORS setup ------------------------------ */
// If "*" then allow all (no credentials). Otherwise allow only listed origins.
let allowed = ALLOWED_ORIGINS.split(',').map(s => s.trim()).filter(Boolean);
const isWildcard = allowed.length === 0 || (allowed.length === 1 && allowed[0] === '*');

const corsOptions = {
  origin(origin, cb) {
    if (!origin) return cb(null, true); // non-browser clients
    if (isWildcard) return cb(null, true);
    return cb(null, allowed.includes(origin));
  },
  methods: ['GET', 'OPTIONS'],
  allowedHeaders: ['x-api-key', 'content-type'],
  maxAge: 86400
};
app.use(cors(corsOptions));
app.options('*', cors(corsOptions)); // preflight

/* --------------------------- API key middleware -------------------------- */
function requireApiKey(req, res, next) {
  // Allow healthcheck without key
  if (req.path === '/health') return next();

  const key = req.header('x-api-key');
  if (!API_KEY_SET.size) {
    // No keys configured: deny by default to avoid accidental exposure
    return res.status(401).json({ error: 'API key required' });
  }
  if (!key || !API_KEY_SET.has(key)) {
    return res.status(403).json({ error: 'Invalid API key' });
  }
  return next();
}
app.use(requireApiKey);

/* -------------------------- Trestle token cache -------------------------- */
let tokenCache = { access_token: null, expiresAt: 0 };

async function getTrestleToken() {
  const now = Date.now();
  if (tokenCache.access_token && now < tokenCache.expiresAt) {
    return tokenCache.access_token;
  }

  const body = new URLSearchParams();
  body.set('client_id', TRESTLE_CLIENT_ID);
  body.set('client_secret', TRESTLE_CLIENT_SECRET);
  body.set('grant_type', 'client_credentials');
  body.set('scope', 'api');

  const resp = await fetch(TOKEN_URL, {
    method: 'POST',
    headers: { 'content-type': 'application/x-www-form-urlencoded' },
    body
  });
  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`Token error ${resp.status}: ${text}`);
  }
  const json = await resp.json();
  const ttl = Number(json.expires_in || 28800); // seconds
  tokenCache = {
    access_token: json.access_token,
    // refresh 60s early
    expiresAt: Date.now() + (ttl - 60) * 1000
  };
  return tokenCache.access_token;
}

async function trestleFetch(path, { accept = 'json' } = {}) {
  const token = await getTrestleToken();
  const url = path.startsWith('http') ? path : `${ODATA_BASE}${path}`;
  const resp = await fetch(url, {
    headers: {
      Authorization: `Bearer ${token}`,
      'User-Agent': 'IDXPlus-CloudRun/1.0'
    }
  });
  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`Trestle ${resp.status}: ${text}`);
  }
  if (accept === 'xml') return resp.text();
  return resp.json();
}

/* --------------------------------- Routes -------------------------------- */
app.get('/health', (req, res) => {
  res.set('Cache-Control', 'no-store').json({ ok: true, service: 'IDXPlus Cloud Run' });
});

// $metadata (XML)
app.get('/webapi/metadata', async (req, res) => {
  try {
    const xml = await trestleFetch('/$metadata', { accept: 'xml' });
    res.set('Content-Type', 'application/xml');
    res.set('Cache-Control', 'public, max-age=600');
    res.status(200).send(xml);
  } catch (err) {
    console.error(err);
    res.status(502).json({ error: 'Metadata fetch failed' });
  }
});

// Property by city (gentle on quotas; default last 90 days, Active)
app.get('/webapi/property/by-city', async (req, res) => {
  try {
    const cityRaw = String(req.query.city || '').trim();
    if (!cityRaw) return res.status(400).json({ error: 'city is required' });

    const top = Math.min(Math.max(parseInt(req.query.top || '100', 10), 1), 1000);
    const days = Math.min(Math.max(parseInt(req.query.days || '90', 10), 1), 365);
    const status = String(req.query.status || 'Active').trim();

    const since = new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString(); // UTC
    const esc = s => s.replace(/'/g, "''"); // OData single-quote escape

    const select = [
      'ListingKey',
      'StandardStatus',
      'City',
      'PostalCode',
      'StateOrProvince',
      'ListPrice',
      'BedroomsTotal',
      'BathroomsTotalInteger',
      'LivingArea',
      'ModificationTimestamp',
      'PhotosChangeTimestamp'
    ].join(',');

    const filter = [
      `City eq '${esc(cityRaw)}'`,
      `StandardStatus eq '${esc(status)}'`,
      `InternetEntireListingDisplayYN eq true`,
      `ModificationTimestamp ge ${since}`
    ].join(' and ');

    const params = new URLSearchParams();
    params.set('$select', select);
    params.set('$filter', filter);
    params.set('$orderby', 'ModificationTimestamp desc');
    params.set('$top', String(top));
    if (PRETTY_ENUMS === 'true') params.set('PrettyEnums', 'true');

    const data = await trestleFetch(`/Property?${params.toString()}`);
    const listings = (data.value || []).map(v => ({
      ListingKey: v.ListingKey,
      City: v.City,
      PostalCode: v.PostalCode,
      StateOrProvince: v.StateOrProvince,
      StandardStatus: v.StandardStatus,
      ListPrice: v.ListPrice,
      BedroomsTotal: v.BedroomsTotal,
      BathroomsTotalInteger: v.BathroomsTotalInteger,
      LivingArea: v.LivingArea,
      ModificationTimestamp: v.ModificationTimestamp,
      PhotosChangeTimestamp: v.PhotosChangeTimestamp
    }));

    res.set('Cache-Control', 'private, max-age=30');
    res.json({
      city: cityRaw,
      status,
      since,
      returned: listings.length,
      listings
    });
  } catch (err) {
    console.error(err);
    res.status(502).json({ error: 'Property query failed' });
  }
});

/* --------------------------------- Server -------------------------------- */
app.listen(PORT, () => {
  console.log(`Server listening on :${PORT}`);
});
