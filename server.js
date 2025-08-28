// server.js
const express = require('express');
const app = express();

const PORT = process.env.PORT || 8080;
const TRESTLE_HOST = process.env.TRESTLE_HOST || 'https://api-prod.corelogic.com';
const TOKEN_URL = `${TRESTLE_HOST}/trestle/oidc/connect/token`;

// Set SCOPE=api for Web API; use SCOPE=rets if you’re calling RETS endpoints later.
const SCOPE = process.env.TRESTLE_SCOPE || 'api';

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
      'Authorization': `Basic ${basic}`,
      'Content-Type': 'application/x-www-form-urlencoded',
      'Accept': 'application/json'
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

// Basic routes
app.get('/', (_req, res) =>
  res.send('Hello from Cloud Run + Node.js! Trestle proxy is running.')
);

app.get('/healthz', (_req, res) => res.json({ ok: true }));

// ⚠️ For testing only — don’t leave this public in production
app.get('/auth/token', async (_req, res) => {
  try {
    const token = await getAccessToken();
    res.json({ access_token: token, token_type: 'Bearer', expires_at: tokenExpiresAt });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Example: call OData with the token (Web API)
app.get('/odata/top5', async (_req, res) => {
  try {
    const token = await getAccessToken();
    const url = `${TRESTLE_HOST}/trestle/odata/Property?$top=5`; // sample
    const r = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
    const text = await r.text();
    res.type(r.headers.get('content-type') || 'application/json').send(text);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.listen(PORT, () => console.log(`Listening on ${PORT}`));
