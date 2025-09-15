// server.js
import express from "express";
import axios from "axios";
import cors from "cors";
import qs from "qs";

const {
  PORT = 8080,
  API_KEY,                               // e.g. "k1"
  ALLOWED_ORIGINS = "",                   // comma-separated, e.g. "https://example.com,null"
  TRESTLE_CLIENT_ID,
  TRESTLE_CLIENT_SECRET,
} = process.env;

if (!API_KEY || !TRESTLE_CLIENT_ID || !TRESTLE_CLIENT_SECRET) {
  console.error("Missing required env: API_KEY, TRESTLE_CLIENT_ID, TRESTLE_CLIENT_SECRET");
  process.exit(1);
}

const app = express();

/* ---------- CORS ---------- */
const allowedOriginsSet = new Set(
  ALLOWED_ORIGINS
    .split(",")
    .map(s => s.trim())
    .filter(Boolean)
);
// Tip: include "null" to allow local file:// testing
app.use(cors({
  origin(origin, cb) {
    if (!origin) return cb(null, true); // allow curl / server-to-server
    if (allowedOriginsSet.has(origin)) return cb(null, true);
    if (allowedOriginsSet.has("null") && origin === "null") return cb(null, true);
    return cb(new Error(`CORS blocked for origin: ${origin}`));
  }
}));

/* ---------- Simple API key gate ---------- */
app.use((req, res, next) => {
  const key = req.header("x-api-key");
  if (key !== API_KEY) return res.status(401).json({ error: "Unauthorized" });
  next();
});

/* ---------- Trestle auth (cached) ---------- */
const TOKEN_URL = "https://api-trestle.corelogic.com/trestle/oidc/connect/token";
const ODATA_BASE = "https://api-trestle.corelogic.com/trestle/odata";
const UA = "CRMLS-Proxy/1.0 (+by-city)";

let cachedToken = null; // { token, expMs }

async function getToken() {
  const now = Date.now();
  if (cachedToken && now < cachedToken.expMs - 60_000) {
    return cachedToken.token;
  }
  const body = qs.stringify({
    client_id: TRESTLE_CLIENT_ID,
    client_secret: TRESTLE_CLIENT_SECRET,
    grant_type: "client_credentials",
    scope: "api",
  });
  const resp = await axios.post(TOKEN_URL, body, {
    headers: { "Content-Type": "application/x-www-form-urlencoded" }
  });
  const { access_token, expires_in } = resp.data;
  cachedToken = {
    token: access_token,
    expMs: now + (expires_in * 1000) // usually 8 hours
  };
  return cachedToken.token;
}

/* ---------- Utilities ---------- */
function escapeODataString(s) {
  // OData escapes single quotes by doubling them
  return String(s).replace(/'/g, "''");
}

function isoDaysAgo(days) {
  const d = new Date();
  d.setUTCDate(d.getUTCDate() - Number(days || 0));
  // OData v4 accepts ISO8601 without quotes for DateTimeOffset
  return d.toISOString().replace(/\.\d{3}Z$/, "Z");
}

function clamp(n, min, max) {
  n = Number(n ?? 0);
  if (Number.isNaN(n)) n = min;
  return Math.max(min, Math.min(n, max));
}

async function axiosWithAuth(url, params, token) {
  return axios.get(url, {
    headers: {
      Authorization: `Bearer ${token}`,
      "User-Agent": UA,
      Accept: "application/json;odata.metadata=minimal"
    },
    params
  });
}

/* Fetch first MediaURL (primary) for a given listing */
async function fetchPrimaryPhotoUrl(listingKey, token) {
  try {
    const params = {
      $select: "MediaURL,Order",
      $filter: `ResourceRecordKey eq '${escapeODataString(listingKey)}'`,
      $orderby: "Order",
      $top: 1
    };
    const resp = await axiosWithAuth(`${ODATA_BASE}/Media`, params, token);
    const first = resp.data?.value?.[0];
    return first?.MediaURL || null;
  } catch (e) {
    // If media fails, don't fail the whole request—just return null
    return null;
  }
}

/* Concurrency-limited mapping (keeps media calls gentle) */
async function mapWithLimit(items, limit, fn) {
  const out = new Array(items.length);
  let i = 0;
  async function worker() {
    while (i < items.length) {
      const idx = i++;
      out[idx] = await fn(items[idx], idx);
    }
  }
  const workers = Array.from({ length: limit }, worker);
  await Promise.all(workers);
  return out;
}

/* ---------- Routes ---------- */

// Health
app.get("/healthz", (_req, res) => res.json({ ok: true }));

// Metadata passthrough (handy for quick checks)
app.get("/webapi/metadata", async (_req, res) => {
  try {
    const token = await getToken();
    const resp = await axios.get(`${ODATA_BASE}/$metadata`, {
      headers: {
        Authorization: `Bearer ${token}`,
        "User-Agent": UA,
        Accept: "application/xml"
      }
    });
    res.type("application/xml").send(resp.data);
  } catch (e) {
    res.status(500).json({ error: "metadata_fetch_failed", detail: e?.response?.data || e.message });
  }
});

// By-city search (thin listing + primaryPhotoUrl)
app.get("/webapi/property/by-city", async (req, res) => {
  try {
    const city = String(req.query.city || "").trim();
    if (!city) return res.status(400).json({ error: "Missing required query param: city" });

    const top = clamp(req.query.top, 1, 1000);
    const days = clamp(req.query.days ?? 90, 0, 3650); // default 90
    const statusParam = (req.query.status || "Active").toString();
    const includePhoto = (req.query.photo ?? "1") !== "0"; // allow ?photo=0 to skip media calls

    const token = await getToken();

    // Build filter (case-insensitive city; optional status; last-N-days by ModificationTimestamp)
    const cityLower = city.toLowerCase();
    const parts = [`tolower(City) eq '${escapeODataString(cityLower)}'`];

    if (statusParam) {
      const statuses = statusParam.split(",").map(s => s.trim()).filter(Boolean);
      if (statuses.length > 1) {
        parts.push(`StandardStatus in (${statuses.map(s => `'${escapeODataString(s)}'`).join(",")})`);
      } else if (statuses.length === 1) {
        parts.push(`StandardStatus eq '${escapeODataString(statuses[0])}'`);
      }
    }

    if (days > 0) {
      parts.push(`ModificationTimestamp ge ${isoDaysAgo(days)}`);
    }

    const $filter = parts.join(" and ");

    const $select = [
      "ListingKey",
      "City",
      "PostalCode",
      "StateOrProvince",
      "StandardStatus",
      "ListPrice",
      "BedroomsTotal",
      "BathroomsTotalInteger",
      "LivingArea",
      "ModificationTimestamp",
      "PhotosChangeTimestamp"
    ].join(",");

    const propResp = await axiosWithAuth(`${ODATA_BASE}/Property`, {
      $select,
      $filter,
      $top: top,
      $count: false
    }, token);

    let listings = (propResp.data?.value || []).map(row => ({
      ListingKey: row.ListingKey,
      City: row.City,
      PostalCode: row.PostalCode,
      StateOrProvince: row.StateOrProvince,
      StandardStatus: row.StandardStatus,
      ListPrice: row.ListPrice,
      BedroomsTotal: row.BedroomsTotal,
      BathroomsTotalInteger: row.BathroomsTotalInteger,
      LivingArea: row.LivingArea,
      ModificationTimestamp: row.ModificationTimestamp,
      PhotosChangeTimestamp: row.PhotosChangeTimestamp,
      primaryPhotoUrl: null
    }));

    // Attach primary photo URL (first image) with small concurrency to protect quotas.
    if (includePhoto && listings.length) {
      // Limit concurrency to 8; adjust if needed.
      listings = await mapWithLimit(listings, 8, async (l) => {
        l.primaryPhotoUrl = await fetchPrimaryPhotoUrl(l.ListingKey, token);
        return l;
      });
    }

    res.json({
      city,
      count: listings.length,
      listings
    });
  } catch (e) {
    const status = e?.response?.status || 500;
    res.status(status).json({
      error: "by_city_failed",
      status,
      detail: e?.response?.data || e.message
    });
  }
});

/* ---------- Start ---------- */
app.listen(PORT, () => {
  console.log(`Server listening on :${PORT}`);
});
