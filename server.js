// server.js — Minimal RESO auth + metadata
// Requires: Node 18+ (global fetch), express

const express = require('express');
const app = express();
const PORT = process.env.PORT || 8080;

// --- Config (defaults match Trestle docs) ---
const TRESTLE_HOST  = process.env.TRESTLE_HOST  || 'https://api-trestle.corelogic.com';
const TOKEN_URL     = `${TRESTLE_HOST}/trestle/oidc/connect/token`;
const TRESTLE_SCOPE = process.env.TRESTLE_SCOPE || 'api';

// --- In-memory token cache ---
let tokenCache = { access_token: null, expires_at: 0 }; // ms epoch

async function fetchAccessToken() {
  const client_id = process.env.TRESTLE_CLIENT_ID;
  const client_secret = process.env.TRESTLE_CLIENT_SECRET;
  if (!client_id || !client_secret) {
    throw new Error('Missing TRESTLE_CLIENT_ID or TRESTLE_CLIENT_SECRET');
  }

  // If we still have a token that expires in >60s, reuse it
  const now = Date.now();
  if (tokenCache.access_token && tokenCache.expires_at - 60_000 > now) {
    return tokenCache.access_token;
  }

  const form = new URLSearchParams();
  form.set('client_id', client_id);
  form.set('client_secret', client_secret);
  form.set('grant_type', 'client_credentials');
  form.set('scope', TRESTLE_SCOPE); // 'api' for RESO Web API

  const resp = await fetch(TOKEN_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'Accept': 'application/json' },
    body: form
  });

  const text = await resp.text();
  if (!resp.ok) {
    throw new Error(`Token request failed: ${resp.status} ${resp.statusText} - ${text}`);
  }

  const json = JSON.parse(text);
  const expiresIn = typeof json.expires_in === 'number' ? json.expires_in : 3600;
  tokenCache = {
    access_token: json.access_token,
    expires_at: Date.now() + expiresIn * 1000
  };
  return json.access_token;
}

// --- Routes ---
app.get('/', (_req, res) => res.send('RESO Web API helper: /auth/token, /webapi/metadata'));

app.get('/healthz', (_req, res) => res.json({ ok: true }));

// Get a token (uses cache). For debugging only; lock down in prod.
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

// Proxy metadata (XML) from RESO Web API
app.get('/webapi/metadata', async (_req, res) => {
  try {
    const token = await fetchAccessToken();
    const url = `${TRESTLE_HOST}/trestle/odata/$metadata`;
    const r = await fetch(url, {
      headers: {
        Authorization: `Bearer ${token}`,
        Accept: 'application/xml',
        'User-Agent': 'IdeasPlusActions/1.0'
      }
    });
    const body = await r.text();
    if (!r.ok) {
      return res.status(r.status).send(body);
    }
    res.type('application/xml').send(body);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: String(err.message || err) });
  }
});

app.listen(PORT, () => console.log(`Listening on ${PORT}`));
