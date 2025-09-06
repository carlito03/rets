// server.js
import express from "express";
import fetch from "node-fetch";

const app = express();
const PORT = process.env.PORT || 8080; // Cloud Run will set this

// Replace with your CRMLS/Trestle credentials via Cloud Run secrets or env vars
const CLIENT_ID = process.env.TRESTLE_CLIENT_ID;
const CLIENT_SECRET = process.env.TRESTLE_CLIENT_SECRET;
const SCOPE = "api"; // CRMLS usually accepts "api"

// --- Get access token ---
async function getAccessToken() {
  const url = `https://rets-803095560818.us-central1.run.app/auth/token?scope=${SCOPE}`;

  const res = await fetch(url, {
    headers: {
      Authorization:
        "Basic " +
        Buffer.from(`${CLIENT_ID}:${CLIENT_SECRET}`).toString("base64"),
    },
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Token request failed: ${res.status} ${text}`);
  }

  const data = await res.json();
  return data.access_token;
}

// --- Fetch listings filtered by city ---
async function fetchListings(city = "Long Beach") {
  const token = await getAccessToken();

  const endpoint = `https://api-prod.corelogic.com/trestle/odata/Property?$filter=City eq '${city}' and MlsStatus eq 'Active'&$orderby=ModificationTimestamp desc&$top=25`;

  const res = await fetch(endpoint, {
    headers: {
      Authorization: `Bearer ${token}`,
    },
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Listing request failed: ${res.status} ${text}`);
  }

  return res.json();
}

// --- Express routes ---
app.get("/", (req, res) => {
  res.send("CRMLS Listing API Proxy is running on Cloud Run 🚀");
});

app.get("/listings", async (req, res) => {
  try {
    const city = req.query.city || "Long Beach";
    const listings = await fetchListings(city);
    res.json(listings);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

// --- Start server ---
app.listen(PORT, "0.0.0.0", () => {
  console.log(`✅ Server running on port ${PORT}`);
});
