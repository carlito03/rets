// server.js
const express = require("express");
const axios = require("axios");
const { ClientCredentials } = require("simple-oauth2");

const app = express();
const PORT = process.env.PORT || 8080;

// === Config ===
const TRESTLE_TOKEN_URL =
  "https://api-trestle.corelogic.com/trestle/oidc/connect/token";
const TRESTLE_API_BASE = "https://api-trestle.corelogic.com";
const CLIENT_ID = process.env.TRESTLE_CLIENT_ID;
const CLIENT_SECRET = process.env.TRESTLE_CLIENT_SECRET;
// Scope: "api" for WebAPI (default), "rets" for RETS feeds
const SCOPE = process.env.TRESTLE_SCOPE || "api";
const TOKEN_SAFETY_SECONDS = Number(process.env.TOKEN_REFRESH_SAFETY_SECONDS || 300);
const ALLOWED_ORIGIN = process.env.ALLOWED_ORIGIN || "*";

// === Middleware: CORS ===
app.use((req, res, next) => {
  res.header("Access-Control-Allow-Origin", ALLOWED_ORIGIN);
  res.header("Access-Control-Allow-Methods", "GET,POST,PUT,PATCH,DELETE,OPTIONS");
  res.header("Access-Control-Allow-Headers", "Content-Type,Authorization");
  if (req.method === "OPTIONS") return res.sendStatus(204);
  next();
});

app.use(express.json({ limit: "2mb" }));
app.use(express.urlencoded({ extended: true }));

// === Token cache (in-memory) ===
let tokenCache = {
  accessToken: null,
  expiresAt: 0, // epoch ms
};

// === OAuth2 client ===
const oauthClient = new ClientCredentials({
  client: { id: CLIENT_ID, secret: CLIENT_SECRET },
  auth: {
    tokenHost: "https://api-trestle.corelogic.com",
    tokenPath: "/trestle/oidc/connect/token",
  },
});

async function getAccessToken() {
  const now = Date.now();

  if (
    tokenCache.accessToken &&
    tokenCache.expiresAt - TOKEN_SAFETY_SECONDS * 1000 > now
  ) {
    console.log(
      "[trestle] Reusing cached access token. ExpiresAt:",
      new Date(tokenCache.expiresAt).toISOString()
    );
    return tokenCache.accessToken;
  }

  if (!CLIENT_ID || !CLIENT_SECRET) {
    throw new Error("Missing TRESTLE_CLIENT_ID or TRESTLE_CLIENT_SECRET");
  }

  const access = await oauthClient.getToken({ scope: SCOPE });
  const { access_token, expires_in, token_type } = access.token || {};

  if (!access_token || token_type !== "Bearer") {
    throw new Error("Unexpected token response from Trestle.");
  }

  tokenCache.accessToken = access_token;
  tokenCache.expiresAt = now + Number(expires_in || 0) * 1000;

  console.log(
    "[trestle] Fetched NEW access token. ExpiresAt:",
    new Date(tokenCache.expiresAt).toISOString()
  );

  return access_token;
}

// === Root & health ===
app.get("/", (req, res) => {
  res.send("Hello from Cloud Run + Node.js! Trestle proxy is running.");
});

app.get("/healthz", (req, res) => {
  res.json({ ok: true, time: new Date().toISOString() });
});

// === Debug (safe, no token disclosure) ===
app.get("/_debug/token-cache", (req, res) => {
  const now = Date.now();
  res.json({
    hasToken: Boolean(tokenCache.accessToken),
    expiresAt: tokenCache.expiresAt
      ? new Date(tokenCache.expiresAt).toISOString()
      : null,
    msUntilExpiry: tokenCache.expiresAt ? tokenCache.expiresAt - now : null,
    safetyWindowSeconds: TOKEN_SAFETY_SECONDS,
    scope: SCOPE,
  });
});

// === Trestle proxy ===
// Call like: GET /api/trestle/trestle/odata/Property?$top=1
app.all("/api/trestle/*", async (req, res) => {
  try {
    const targetPath = req.params[0] || "";
    const normalized = `/${targetPath}`.replace(/\/{2,}/g, "/");

    // Block direct calls to token endpoint
    if (normalized.toLowerCase().includes("/trestle/oidc/connect/token")) {
      return res.status(403).json({ error: "Forbidden path." });
    }

    const token = await getAccessToken();

    // Build target URL
    const url = new URL(TRESTLE_API_BASE);
    url.pathname = normalized;
    const qs = new URLSearchParams(req.query || {});
    const targetUrl = qs.toString() ? `${url.toString()}?${qs}` : url.toString();

    const trestleResp = await axios({
      method: req.method,
      url: targetUrl,
      headers: {
        Authorization: `Bearer ${token}`,
        Accept: req.headers["accept"] || "application/json",
        "Content-Type": req.headers["content-type"] || "application/json",
      },
      data: ["GET", "HEAD"].includes(req.method) ? undefined : req.body,
      timeout: 20000,
      validateStatus: () => true,
    });

    // Retry once if unauthorized
    if (trestleResp.status === 401) {
      tokenCache.accessToken = null;
      const fresh = await getAccessToken();
      const retry = await axios({
        method: req.method,
        url: targetUrl,
        headers: {
          Authorization: `Bearer ${fresh}`,
          Accept: req.headers["accept"] || "application/json",
          "Content-Type": req.headers["content-type"] || "application/json",
        },
        data: ["GET", "HEAD"].includes(req.method) ? undefined : req.body,
        timeout: 20000,
        validateStatus: () => true,
      });
      return res.status(retry.status).set(retry.headers).send(retry.data);
    }

    return res.status(trestleResp.status).set(trestleResp.headers).send(trestleResp.data);
  } catch (err) {
    console.error("Proxy error:", err?.response?.status, err?.message);
    const status =
      err?.response?.status ||
      (String(err?.message || "").includes("timeout") ? 504 : 500);
    const detail = err?.response?.data || {
      error: "ProxyError",
      message: err?.message || "Unknown error",
    };
    return res.status(status).json(detail);
  }
});

app.listen(PORT, () => {
  console.log(`Server listening on port ${PORT}`);
});
