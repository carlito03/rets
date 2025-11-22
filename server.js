
// ===== PHASE 2 (fixed) =====

// server.js
// Minimal Cloud Run API with CORS + API key auth for Trestle WebAPI (CommonJS)

const express = require('express');
const cors = require('cors');
// Prefer built-in fetch on Node 18+, fall back to node-fetch
const fetch = global.fetch || ((...args) => import('node-fetch').then(({ default: f }) => f(...args)));

const app = express();

const { QueryCommand } = require('@aws-sdk/lib-dynamodb');

/* -------------------------- AWS -------------------------- */
const { DynamoDBClient, DescribeTableCommand } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, UpdateCommand, BatchWriteCommand } = require('@aws-sdk/lib-dynamodb');
const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');
const { SQSClient, SendMessageBatchCommand, SendMessageCommand } = require('@aws-sdk/client-sqs');

const {
  AWS_REGION = 'us-east-1',
  DDB_TABLE_LISTINGS = 'Listings'
} = process.env;

const ddb = DynamoDBDocumentClient.from(new DynamoDBClient({ region: AWS_REGION }));

const {
  MEDIA_BUCKET = 'listings-media-437184912387-use1',
  CDN_BASE = '' // e.g. https://d1234abcd.cloudfront.net
} = process.env;

// Prefer standard AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY.
// If you insist on custom names, we read them explicitly below.
const accessKeyId =
  process.env.AWS_ACCESS_KEY_ID || process.env.AWS_SECRET_IMAGE_WRITER_ID;
const secretAccessKey =
  process.env.AWS_SECRET_ACCESS_KEY || process.env.IMAGE_WRITER_SECRET;

const s3 = new S3Client({
  region: AWS_REGION,
  credentials: (accessKeyId && secretAccessKey)
    ? { accessKeyId, secretAccessKey }
    : undefined // falls back to default provider chain if you used standard names
});

// SQS queue for image jobs
const { QUEUE_URL = '' } = process.env;

// Reuse the same credentials pattern as S3
const sqs = new SQSClient({
  region: AWS_REGION,
  credentials: (accessKeyId && secretAccessKey)
    ? { accessKeyId, secretAccessKey }
    : undefined
});

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
  PRETTY_ENUMS = 'true',

MAX_WRITE_PER_INGEST = '500'

} = process.env;

/* -------------------------- Commercial  -------------------------- */

const {
    DDB_TABLE_COMMERCIAL_LISTINGS = 'CommercialListings',
    RAPIDAPI_KEY
  } = process.env;

if (!TRESTLE_CLIENT_ID || !TRESTLE_CLIENT_SECRET) {
  console.error('Missing TRESTLE_CLIENT_ID / TRESTLE_CLIENT_SECRET');
}

const API_KEY_SET = new Set(
  API_KEYS.split(',').map(s => s.trim()).filter(Boolean)
);

const ODATA_BASE = 'https://api-trestle.corelogic.com/trestle/odata';
const TOKEN_URL = 'https://api-trestle.corelogic.com/trestle/oidc/connect/token';

/* ------------------------------- CORS setup ------------------------------ */
let allowed = ALLOWED_ORIGINS.split(',').map(s => s.trim()).filter(Boolean);
const isWildcard = allowed.length === 0 || (allowed.length === 1 && allowed[0] === '*');

const corsOptions = {
  origin(origin, cb) {
    if (!origin) return cb(null, true); // non-browser clients & curl
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
  // open routes that don't need authentication
  if (req.path === '/health') return next();
  if (req.path === '/public/ddb/find-by-address') return next();  // <-- NEW: allow frontend public route

  const key = req.header('x-api-key');
  if (!API_KEY_SET.size) return res.status(401).json({ error: 'API key required' });
  if (!key || !API_KEY_SET.has(key)) return res.status(403).json({ error: 'Invalid API key' });
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
    expiresAt: Date.now() + (ttl - 60) * 1000 // refresh 60s early
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

async function rapidApiFetch(path, body = {}) {
    if (!RAPIDAPI_KEY) {
      throw new Error('RAPIDAPI_KEY is not set');
    }
  
    const url = `https://loopnet-api.p.rapidapi.com${path}`;
    const resp = await fetch(url, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-rapidapi-key': RAPIDAPI_KEY,
        'x-rapidapi-host': 'loopnet-api.p.rapidapi.com'
      },
      body: JSON.stringify(body)
    });
  
    if (!resp.ok) {
      const text = await resp.text();
      throw new Error(`RapidAPI ${resp.status}: ${text}`);
    }
    return resp.json();
  }

/* --------------------------------- helpers -------------------------------- */




const sleep = ms => new Promise(r => setTimeout(r, ms));
const chunk25 = arr => { const out = []; for (let i = 0; i < arr.length; i += 25) out.push(arr.slice(i, i + 25)); return out; };
const toNorm = s => String(s || '').trim().toLowerCase();
const toIsoUtc = s => { try { return new Date(s).toISOString(); } catch { return s; } };
const toEpoch = iso => Math.floor(new Date(iso || Date.now()).getTime() / 1000);


function isImageStale(item) {
    // Needs work if we’ve never built primary OR never marked ImagesUpdatedAt
    if (!item?.CdnPrimary400 || !item?.ImagesUpdatedAt) return true;
    // If we have a photo-change timestamp, and it’s newer than last image build, it’s stale
    if (item?.PhotosChangeTimestamp) {
      return new Date(item.PhotosChangeTimestamp).getTime() >
             new Date(item.ImagesUpdatedAt).getTime();
    }
    return false;
  }


// Enqueue primary-400 image jobs in batches of 10
async function enqueuePrimaryBatch(listingKeys = []) {
  if (!QUEUE_URL) throw new Error('QUEUE_URL is not set');

  let enqueued = 0;
  for (let i = 0; i < listingKeys.length; i += 10) {
    const slice = listingKeys.slice(i, i + 10);
    const Entries = slice.map((key, idx) => ({
      Id: `${i + idx}`,
      MessageBody: JSON.stringify({ type: 'primary', listingKey: String(key), width: 400 })
    }));

    const resp = await sqs.send(new SendMessageBatchCommand({ QueueUrl: QUEUE_URL, Entries }));
    enqueued += (Entries.length - (resp.Failed?.length || 0));
    if (resp.Failed?.length) console.warn('SQS failed entries', resp.Failed);
    await sleep(50);
  }
  return enqueued;
}

// --- add below enqueuePrimaryBatch ---
async function enqueueGalleryBatch(listingKeys = [], per = 10) {
  if (!QUEUE_URL) throw new Error('QUEUE_URL is not set');

  let enqueued = 0;
  for (let i = 0; i < listingKeys.length; i += 10) {
    const slice = listingKeys.slice(i, i + 10);
    const Entries = slice.map((key, idx) => ({
      Id: `${i + idx}`,
      MessageBody: JSON.stringify({ type: 'gallery', listingKey: String(key), width: 400, per }),
    }));

    const resp = await sqs.send(new SendMessageBatchCommand({ QueueUrl: QUEUE_URL, Entries }));
    enqueued += (Entries.length - (resp.Failed?.length || 0));
    if (resp.Failed?.length) console.warn('SQS failed entries', resp.Failed);
    await sleep(50);
  }
  return enqueued;
}

/* ---------------------------- idempotent upsert --------------------------- */
async function upsertThinListing(item) {
  const {
    ListingKey,
    City,
    CountyOrParish,                 // NEW
    PostalCode,
    StateOrProvince,
    StandardStatus,
    PropertyType,
    PropertySubType,
    ListPrice,
    BedroomsTotal,
    BathroomsTotalInteger,
    LivingArea,
    ModificationTimestamp,
    PhotosChangeTimestamp,
    PrimaryPhotoUrl,
    // NEW (address fields)
    UnparsedAddress,
    InternetAddressDisplayYN,
    // NEW: special listing conditions (array of strings)
    SpecialListingConditions
  } = item;

  const CityNorm = toNorm(City);
  const CityStatus = `${CityNorm}#${StandardStatus || ''}`;
  const ModEpoch = toEpoch(ModificationTimestamp || new Date().toISOString());
  const PriceSortDesc = typeof ListPrice === 'number' ? -ListPrice : null;
  const LastSeenAt = Math.floor(Date.now() / 1000);
  const IsActive = StandardStatus === 'Active';

  const UpdateExpression = `
    SET City = :City,
        CityNorm = :CityNorm,
        CountyOrParish = :CountyOrParish,
        PostalCode = :PostalCode,
        StateOrProvince = :StateOrProvince,
        PropertyType = :PropertyType,
        PropertySubType = :PropertySubType,
        StandardStatus = :StandardStatus,
        ListPrice = :ListPrice,
        BedroomsTotal = :BedroomsTotal,
        BathroomsTotalInteger = :BathroomsTotalInteger,
        LivingArea = :LivingArea,
        ModificationTimestamp = :ModificationTimestamp,
        PhotosChangeTimestamp = :PhotosChangeTimestamp,
        PrimaryPhotoUrl = :PrimaryPhotoUrl,
        CityStatus = :CityStatus,
        ModEpoch = :ModEpoch,
        PriceSortDesc = :PriceSortDesc,
        LastSeenAt = :LastSeenAt,
        IsActive = :IsActive,
        UnparsedAddress = :UnparsedAddress,
        InternetAddressDisplayYN = :InternetAddressDisplayYN,
        SpecialListingConditions = :SpecialListingConditions
  `.replace(/\s+/g, ' ').trim();

  const params = {
    TableName: DDB_TABLE_LISTINGS,
    Key: { ListingKey },
    UpdateExpression,
    ConditionExpression: 'attribute_not_exists(ModEpoch) OR ModEpoch <= :ModEpoch',
    ExpressionAttributeValues: {
      ':City': City || null,
      ':CityNorm': CityNorm,
      ':CountyOrParish': CountyOrParish ?? null,          // NEW
      ':PropertyType': PropertyType ?? null,
      ':PropertySubType': PropertySubType ?? null,
      ':PostalCode': PostalCode ?? null,
      ':StateOrProvince': StateOrProvince ?? null,
      ':StandardStatus': StandardStatus ?? null,
      ':ListPrice': typeof ListPrice === 'number' ? ListPrice : null,
      ':BedroomsTotal': BedroomsTotal ?? null,
      ':BathroomsTotalInteger': BathroomsTotalInteger ?? null,
      ':LivingArea': LivingArea ?? null,
      ':ModificationTimestamp': ModificationTimestamp || null,
      ':PhotosChangeTimestamp': PhotosChangeTimestamp || null,
      ':PrimaryPhotoUrl': PrimaryPhotoUrl ?? null,
      ':CityStatus': CityStatus,
      ':ModEpoch': ModEpoch,
      ':PriceSortDesc': PriceSortDesc,
      ':LastSeenAt': LastSeenAt,
      ':IsActive': IsActive,
      ':UnparsedAddress': UnparsedAddress ?? null,
      ':InternetAddressDisplayYN': (typeof InternetAddressDisplayYN === 'boolean') ? InternetAddressDisplayYN : null,
      ':SpecialListingConditions': Array.isArray(SpecialListingConditions) ? SpecialListingConditions : []
    }
  };

  try {
    await ddb.send(new UpdateCommand(params));
    return { ok: true };
  } catch (e) {
    if (e.name === 'ConditionalCheckFailedException') return { ok: true, skipped: true };
    throw e;
  }
}

async function upsertCommercialListing(item) {
  const {
    ListingId,
    City,
    CityNorm,
    State,          // we will alias this
    PostalCode,
    Address,
    Title,
    ListingType,
    PriceRaw,
    ShortSummary,
    PrimaryPhotoUrl,
    IsActive = true,
    ModificationTimestamp,
    ModEpoch,
    LastSeenAt
  } = item;

  const params = {
    TableName: DDB_TABLE_COMMERCIAL_LISTINGS,
    Key: { ListingId },
    UpdateExpression: `
      SET City = :City,
          CityNorm = :CityNorm,
          #S = :State,
          PostalCode = :PostalCode,
          Address = :Address,
          Title = :Title,
          ListingType = :ListingType,
          PriceRaw = :PriceRaw,
          ShortSummary = :ShortSummary,
          PrimaryPhotoUrl = :PrimaryPhotoUrl,
          IsActive = :IsActive,
          ModificationTimestamp = :ModificationTimestamp,
          ModEpoch = :ModEpoch,
          LastSeenAt = :LastSeenAt
    `.replace(/\s+/g, ' ').trim(),
    ExpressionAttributeNames: {
      '#S': 'State'
    },
    ExpressionAttributeValues: {
      ':City': City || null,
      ':CityNorm': CityNorm || null,
      ':State': State || null,
      ':PostalCode': PostalCode || null,
      ':Address': Address || null,
      ':Title': Title || null,
      ':ListingType': ListingType || null,
      ':PriceRaw': PriceRaw || null,
      ':ShortSummary': ShortSummary || null,
      ':PrimaryPhotoUrl': PrimaryPhotoUrl || null,
      ':IsActive': !!IsActive,
      ':ModificationTimestamp': ModificationTimestamp || null,
      ':ModEpoch': ModEpoch || Math.floor(Date.now() / 1000),
      ':LastSeenAt': LastSeenAt || Math.floor(Date.now() / 1000)
    }
  };

  await ddb.send(new UpdateCommand(params));
}


//tiny helper to shape one RapidAPI bulkDetails record into that structure

  function normalizeCommercialFromBulk(bulkItem, ctx = {}) {
    // ctx can contain { city, state } from the query
    const nowIso = new Date().toISOString();
    const nowEpoch = Math.floor(Date.now() / 1000);
  
    const listingId = String(bulkItem.listingId);
    const loc = bulkItem.location || {};
    const titleArr = Array.isArray(bulkItem.title) ? bulkItem.title.filter(Boolean) : [];
    const cityState = loc.cityState || '';
    let city = ctx.city || '';
    let state = ctx.state || '';
  
    // try to parse "Chicago, IL"
    if ((!city || !state) && cityState) {
      const parts = cityState.split(',').map(s => s.trim());
      if (parts.length >= 2) {
        city = city || parts[0];
        state = state || parts[1];
      }
    }
  
    const cityNorm = String(city || '').trim().toLowerCase() || null;
  
    return {
      ListingId: listingId,
      City: city || null,
      CityNorm: cityNorm,
      State: state || null,
      PostalCode: loc.postalCode || null,
      Address: loc.address || titleArr[0] || null,
      Title: titleArr.length ? titleArr.join(', ') : null,
      ListingType: bulkItem.listingType || null,
      PriceRaw: bulkItem.price || null,
      ShortSummary: bulkItem.shortSummary || null,
      PrimaryPhotoUrl: bulkItem.photo || null,
      IsActive: true,
      ModificationTimestamp: nowIso,
      ModEpoch: nowEpoch,
      LastSeenAt: nowEpoch
    };
  }

// --- helpers to shape detail + gallery payloads ---
function shapeDetails(v) {
    // Keep this “thin-ish” but useful. You can expand later.
    return {
      ListingKey: v.ListingKey,
      StandardStatus: v.StandardStatus,
      PropertyType: v.PropertyType ?? null,
      PropertySubType: v.PropertySubType ?? null,
  
      // Address
      UnparsedAddress: v.UnparsedAddress ?? null,
      StreetNumber: v.StreetNumber ?? null,
      StreetDirPrefix: v.StreetDirPrefix ?? null,
      StreetName: v.StreetName ?? null,
      StreetSuffix: v.StreetSuffix ?? null,
      UnitNumber: v.UnitNumber ?? null,
      City: v.City ?? null,
      StateOrProvince: v.StateOrProvince ?? null,
      PostalCode: v.PostalCode ?? null,
  
      // Location
      Latitude: v.Latitude ?? null,
      Longitude: v.Longitude ?? null,
  
      // Facts
      ListPrice: v.ListPrice ?? null,
      BedroomsTotal: v.BedroomsTotal ?? null,
      BathroomsTotalInteger: v.BathroomsTotalInteger ?? null,
      LivingArea: v.LivingArea ?? null,
      YearBuilt: v.YearBuilt ?? null,
      LotSizeAcres: v.LotSizeAcres ?? null,
      StoriesTotal: v.StoriesTotal ?? null,
      Cooling: v.Cooling ?? null,
      Heating: v.Heating ?? null,
  
      // Remarks
      PublicRemarks: v.PublicRemarks ?? null,
  
      // timestamps
      ModificationTimestamp: v.ModificationTimestamp ?? null,
      PhotosChangeTimestamp: v.PhotosChangeTimestamp ?? null
    };
  }
  
  function shapeGallery(media = [], top = 10) {
    const list = Array.isArray(media) ? media.slice(0, top) : [];
    return list
      .filter(m => !!m?.MediaURL)
      .sort((a, b) => (a.Order ?? 0) - (b.Order ?? 0))
      .map(m => ({ url: m.MediaURL, order: m.Order ?? null }));
  }

/* --------------------------------- Routes -------------------------------- */
app.get('/health', (req, res) => {
  res.set('Cache-Control', 'no-store').json({ ok: true, service: 'IDXPlus Cloud Run' });
});

// $metadata (XML passthrough)
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

//
// Admin: ingest commercial listings for a city from RapidAPI (LoopNet)
// Admin: ingest commercial listings for a city from RapidAPI (LoopNet)
app.get('/admin/commercial/ingest/city', async (req, res) => {
  try {
    const cityRaw = String(req.query.city || '').trim();
    const stateRaw = String(req.query.state || 'CA').trim();
    const page = Math.max(parseInt(req.query.page || '1', 10), 1);
    const BATCH_SIZE = 20; // RapidAPI says max 20
    const debug = req.query.debug === '1';

    if (!cityRaw) {
      return res.status(400).json({ error: 'city is required' });
    }
    if (!RAPIDAPI_KEY) {
      return res.status(500).json({ error: 'RAPIDAPI_KEY not set in env' });
    }

    const searchBody = {
      country: 'US',
      state: stateRaw,
      city: cityRaw,
      county: null,
      zipCode: null,
      page
    };

    const searchResp = await rapidApiFetch('/loopnet/v2/sale/searchByAddress', searchBody);
    const first = Array.isArray(searchResp.data) ? searchResp.data[0] : null;
    if (!first) {
      return res.json({ city: cityRaw, state: stateRaw, totalIds: 0, written: 0, skipped: 0, errors: 0 });
    }

    const allIds = Array.isArray(first.allListingIds) ? first.allListingIds.map(String) : [];

    let written = 0;
    let skipped = 0;
    let errors = 0;
    const errorDetails = [];

    for (let i = 0; i < allIds.length; i += BATCH_SIZE) {
      const slice = allIds.slice(i, i + BATCH_SIZE);

      try {
        const bulkResp = await rapidApiFetch('/loopnet/property/bulkDetails', {
          listingIds: slice
        });

        const items = Array.isArray(bulkResp.data) ? bulkResp.data : [];
        for (const it of items) {
          const norm = normalizeCommercialFromBulk(it, { city: cityRaw, state: stateRaw });
          await upsertCommercialListing(norm);
          written += 1;
        }
      } catch (e) {
        errors += 1;
        const msg = e?.message || String(e);
        console.error('bulkDetails batch failed', { batchStart: i, message: msg });
        if (debug) {
          errorDetails.push({ batchStart: i, message: msg });
        }
      }

      await sleep(80);
    }

    const out = {
      city: cityRaw,
      state: stateRaw,
      totalIds: allIds.length,
      written,
      skipped,
      errors
    };
    if (debug) out.errorDetails = errorDetails;

    res.set('Cache-Control', 'no-store').json(out);
  } catch (err) {
    console.error(err);
    res.status(502).json({ error: 'commercial ingest failed', message: err?.message || String(err) });
  }
});

// Property by city (case-insensitive; default last 90d; Active; includes PrimaryPhotoUrl via $expand)
app.get('/webapi/property/by-city', async (req, res) => {
  try {
    const cityRaw = String(req.query.city || '').trim();
    if (!cityRaw) return res.status(400).json({ error: 'city is required' });

    const top = Math.min(Math.max(parseInt(req.query.top || '100', 10), 1), 1000);
    const days = Math.min(Math.max(parseInt(req.query.days || '90', 10), 1), 365);
    const status = String(req.query.status || 'Active').trim();
    const includePhoto = String(req.query.includePhoto || '1') === '1';
    const prettyEnums = (req.query.prettyEnums ?? PRETTY_ENUMS) === 'true';

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
      'PhotosChangeTimestamp',
      // NEW address fields
      'UnparsedAddress',
      'InternetAddressDisplayYN'
    ].join(',');

    const filter = [
      `tolower(City) eq '${esc(cityRaw.toLowerCase())}'`,
      `StandardStatus eq '${esc(status)}'`,
      `InternetEntireListingDisplayYN eq true`,
      `ModificationTimestamp ge ${since}`
    ].join(' and ');

    const params = new URLSearchParams();
    params.set('$select', select);
    params.set('$filter', filter);
    params.set('$orderby', 'ModificationTimestamp desc');
    params.set('$top', String(top));
    if (includePhoto) {
      params.set('$expand', 'Media($select=MediaURL,Order;$orderby=Order;$top=1)');
    }
    if (prettyEnums) params.set('PrettyEnums', 'true');

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
      PhotosChangeTimestamp: v.PhotosChangeTimestamp,
      // NEW: respect address display
      address: (v.InternetAddressDisplayYN === false) ? null : v.UnparsedAddress ?? null,
      PrimaryPhotoUrl: Array.isArray(v.Media) && v.Media.length ? v.Media[0].MediaURL : null
    }));

    res.set('Cache-Control', 'private, max-age=30');
    res.json({ city: cityRaw, status, since, returned: listings.length, listings });
  } catch (err) {
    console.error(err);
    res.status(502).json({ error: 'Property query failed' });
  }
});

// Admin: ingest thin listings for a city into DynamoDB (idempotent upsert per item)
// Admin: ingest thin listings for a city OR county into DynamoDB (idempotent upsert per item)
app.get('/admin/ingest/city', async (req, res) => {
  try {
    const cityRaw = String(req.query.city || '').trim();
    const countyRaw = String(req.query.county || '').trim();   // NEW
    if (!cityRaw && !countyRaw) {
      return res.status(400).json({ error: 'city or county is required' });
    }

    const days = Math.min(Math.max(parseInt(req.query.days || '90', 10), 1), 365);
    const status = String(req.query.status || 'Active').trim();
    const propertyType = String(req.query.propertyType || '').trim(); // NEW

    // NEW: Optional special-listing-conditions filter. Supports comma-separated.
    const slcRaw = String(req.query.slc || '').trim();
    const slcList = slcRaw ? slcRaw.split(',').map(s => s.trim()).filter(Boolean) : [];

    const since = new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString();
    const esc = s => s.replace(/'/g, "''");

    const select = [
      'ListingKey',
      'StandardStatus',
      'City',
      'CountyOrParish',           // NEW – we want to store county too
      'PostalCode',
      'StateOrProvince',
      'ListPrice',
      'PropertyType',
      'PropertySubType',
      'BedroomsTotal',
      'BathroomsTotalInteger',
      'LivingArea',
      'ModificationTimestamp',
      'PhotosChangeTimestamp',
      'UnparsedAddress',
      'InternetAddressDisplayYN',
      'SpecialListingConditions'
    ].join(',');

    const filterParts = [
      `StandardStatus eq '${esc(status)}'`,
      `InternetEntireListingDisplayYN eq true`,
      `ModificationTimestamp ge ${since}`
    ];

    // if city provided, add city filter
    if (cityRaw) {
      filterParts.push(`tolower(City) eq '${esc(cityRaw.toLowerCase())}'`);
    }

    // if county provided, add county filter
    if (countyRaw) {
      filterParts.push(`CountyOrParish eq '${esc(countyRaw)}'`);
    }

    // optional: lock to CRMLS only
    // filterParts.push(`OriginatingSystemName eq 'CRMLS'`);

    // if propertyType provided, add it
    if (propertyType) {
      filterParts.push(`PropertyType eq '${esc(propertyType)}'`);
    }

    // add SLC filter if requested. For multiple, OR them together.
    if (slcList.length) {
      const anyClauses = slcList.map(v =>
        `SpecialListingConditions/any(s: s eq '${esc(v)}')`
      );
      filterParts.push(`(${anyClauses.join(' or ')})`);
    }

    const filter = filterParts.join(' and ');

    const baseParams = new URLSearchParams();
    baseParams.set('$select', select);
    baseParams.set('$filter', filter);
    baseParams.set('$orderby', 'ModificationTimestamp desc');
    baseParams.set('$top', '100');
    if (PRETTY_ENUMS === 'true') baseParams.set('PrettyEnums', 'true');

    // Pull all pages
    let url = `/Property?${baseParams.toString()}`;
    const all = [];
    while (url) {
      const data = await trestleFetch(url);
      const pageItems = (data.value || []).map(v => ({
        ListingKey: v.ListingKey,
        City: v.City,
        CityNorm: toNorm(v.City),
        CountyOrParish: v.CountyOrParish ?? null,  // NEW
        PostalCode: v.PostalCode,
        StateOrProvince: v.StateOrProvince,
        PropertyType: v.PropertyType,
        PropertySubType: v.PropertySubType,
        StandardStatus: v.StandardStatus,
        ListPrice: v.ListPrice,
        BedroomsTotal: v.BedroomsTotal,
        BathroomsTotalInteger: v.BathroomsTotalInteger,
        LivingArea: v.LivingArea,
        ModificationTimestamp: toIsoUtc(v.ModificationTimestamp),
        PhotosChangeTimestamp: toIsoUtc(v.PhotosChangeTimestamp),
        // NEW: store null if address display is disallowed
        UnparsedAddress: (v.InternetAddressDisplayYN === false) ? null : (v.UnparsedAddress ?? null),
        InternetAddressDisplayYN: v.InternetAddressDisplayYN ?? null,
        SpecialListingConditions: Array.isArray(v.SpecialListingConditions) ? v.SpecialListingConditions : [],
        PrimaryPhotoUrl: null // fill later via media job
      }));
      all.push(...pageItems);

      url = data['@odata.nextLink'] || null;
      if (url) await sleep(150);
    }

    let written = 0;
    let skipped = 0;
    for (const it of all) {
      const r = await upsertThinListing(it);
      if (r.skipped) skipped += 1;
      else written += 1;
    }

    res.set('Cache-Control', 'no-store').json({
      city: cityRaw || null,
      county: countyRaw || null,
      status,
      since,
      fetched: all.length,
      written,
      skipped
    });
  } catch (err) {
    console.error(err);
    const payload = { error: 'Ingest failed' };
    if (req.query.debug === '1') payload.details = { name: err.name, message: err.message };
    res.status(502).json(payload);
  }
});

// quick DDB ping
app.get('/admin/ddb/ping', async (req, res) => {
  try {
    const meta = await ddb.send(new DescribeTableCommand({ TableName: DDB_TABLE_LISTINGS }));
    const item = {
      City: '__ping',
      CityNorm: '__ping',
      ListingKey: String(Date.now()),
      StandardStatus: 'Test',
      ModificationTimestamp: new Date().toISOString()
    };
    await ddb.send(new BatchWriteCommand({
      RequestItems: { [DDB_TABLE_LISTINGS]: [{ PutRequest: { Item: item } }] }
    }));

    res.json({
      ok: true,
      region: AWS_REGION,
      table: DDB_TABLE_LISTINGS,
      status: meta.Table.TableStatus,
      wroteItem: { listingKey: item.ListingKey }
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({
      ok: false,
      region: AWS_REGION,
      table: DDB_TABLE_LISTINGS,
      name: err.name,
      message: err.message
    });
  }
});

app.get('/admin/s3/ping', async (req, res) => {
  try {
    // 1x1 transparent PNG (base64)
    const b64 = 'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/wwAAn8B9C7FQ2UAAAAASUVORK5CYII=';
    const Body = Buffer.from(b64, 'base64');

    const Key = 'primary/400/ping.png';
    await s3.send(new PutObjectCommand({
      Bucket: MEDIA_BUCKET,
      Key,
      Body,
      ContentType: 'image/png',
      CacheControl: 'public, max-age=31536000, immutable'
    }));

    const cdn = CDN_BASE || '(set CDN_BASE to your CloudFront URL)';
    res.json({
      ok: true,
      bucket: MEDIA_BUCKET,
      key: Key,
      cdnUrl: CDN_BASE ? `${CDN_BASE}/${Key}` : null,
      tip: CDN_BASE ? 'Open cdnUrl in your browser' : 'Set CDN_BASE to your CloudFront domain'
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ ok: false, name: err.name, message: err.message });
  }
});

// Search thin listings by city (supports optional propertyType, e.g. "Residential Lease")
app.get('/api/search/city', async (req, res) => {
  try {
    const city = String(req.query.city || '').trim().toLowerCase();
    if (!city) return res.status(400).json({ error: 'city is required' });

    const status = String(req.query.status || 'Active').trim();
    const propertyType = String(req.query.propertyType || '').trim(); // NEW
    const limit = Math.min(Math.max(parseInt(req.query.limit || '50', 10), 1), 100);

    // pagination cursor (opaque)
    const cursor = req.query.cursor
      ? JSON.parse(Buffer.from(String(req.query.cursor), 'base64').toString('utf8'))
      : undefined;

    // We’ll always filter Status in Dynamo,
    // but handle PropertyType in Node so we can keep paging until we have enough matches.
    const baseParams = {
      TableName: DDB_TABLE_LISTINGS,
      IndexName: 'CityNorm-ModificationTimestamp-index',
      KeyConditionExpression: 'CityNorm = :c',
      FilterExpression: 'StandardStatus = :s',
      ExpressionAttributeValues: { ':c': city, ':s': status },
      ScanIndexForward: false,          // newest first
      Limit: limit,                     // per-page read size (we may loop)
      ExclusiveStartKey: cursor
    };

    const listings = [];
    let lastKey = cursor;
    let finished = false;

    while (!finished && listings.length < limit) {
      const params = { ...baseParams, ExclusiveStartKey: lastKey };
      const resp = await ddb.send(new QueryCommand(params));

      const pageItems = resp.Items || [];

      // apply optional PropertyType filter here
      const filtered = propertyType
        ? pageItems.filter(it => (it.PropertyType || '') === propertyType)
        : pageItems;

      // map thin fields the UI needs
      for (const v of filtered) {
        if (listings.length >= limit) break;
        listings.push({
          ListingKey: v.ListingKey,
          City: v.City,
          propertyType: v.PropertyType ?? null,
          propertySubType: v.PropertySubType ?? null,
          PostalCode: v.PostalCode,
          StateOrProvince: v.StateOrProvince,
          StandardStatus: v.StandardStatus,
          ListPrice: v.ListPrice,
          BedroomsTotal: v.BedroomsTotal,
          BathroomsTotalInteger: v.BathroomsTotalInteger,
          LivingArea: v.LivingArea,
          ModificationTimestamp: v.ModificationTimestamp,
          PhotosChangeTimestamp: v.PhotosChangeTimestamp,
          // address only if allowed
          address: (v.InternetAddressDisplayYN === false) ? null : (v.UnparsedAddress ?? null),
          primaryPhotoUrl: v.CdnPrimary400 ?? v.PrimaryPhotoUrl ?? null
        });
      }

      lastKey = resp.LastEvaluatedKey;
      if (!lastKey || listings.length >= limit) {
        finished = true;
      }
    }

    const next =
      lastKey
        ? Buffer.from(JSON.stringify(lastKey)).toString('base64')
        : null;

    res.set('Cache-Control', 'private, max-age=15');
    res.json({
      city,
      status,
      propertyType: propertyType || null,
      returned: listings.length,
      cursor: next,
      listings
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'search failed', message: err?.message || String(err) });
  }
});

// Fetch full details + top-N media from Trestle NOW,
// store a compact "Details" snapshot in Dynamo for next time.
// Fetch full details + top-N media from Trestle now,
// then store a compact "Details" snapshot in Dynamo for next time.

// Seed SQS with gallery-400 jobs for a city (build up to 10 photos per listing)


app.get('/admin/seed/gallery', async (req, res) => {
  try {
    const city = String(req.query.city || '').trim().toLowerCase();
    if (!city) return res.status(400).json({ error: 'city is required' });

    const status = String(req.query.status || 'Active').trim();
    const limit = Math.min(Math.max(parseInt(req.query.limit || '200', 10), 1), 500);

    // how many photos to build per listing (default 10)
    const per = Math.min(Math.max(parseInt(req.query.per || '10', 10), 1), 20);

    // pagination cursor (opaque)
    const cursor = req.query.cursor
      ? JSON.parse(Buffer.from(String(req.query.cursor), 'base64').toString('utf8'))
      : undefined;

    const params = {
      TableName: DDB_TABLE_LISTINGS,
      IndexName: 'CityNorm-ModificationTimestamp-index',
      KeyConditionExpression: 'CityNorm = :c',
      // Needs gallery or is stale if photos changed after last image build
      FilterExpression:
        'StandardStatus = :s AND (attribute_not_exists(Gallery400Count) OR PhotosChangeTimestamp > ImagesUpdatedAt)',
      ExpressionAttributeValues: { ':c': city, ':s': status },
      ScanIndexForward: false,
      Limit: limit,
      ExclusiveStartKey: cursor
    };

    const resp = await ddb.send(new QueryCommand(params));
    const keys = (resp.Items || []).map(i => i.ListingKey).filter(Boolean);

    const enqueued = await enqueueGalleryBatch(keys, per);
    const next = resp.LastEvaluatedKey
      ? Buffer.from(JSON.stringify(resp.LastEvaluatedKey)).toString('base64')
      : null;

    res.json({ ok: true, city, status, scanned: (resp.Count || 0), enqueued, cursor: next });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'seed gallery failed', name: err.name, message: err.message });
  }
});

// Detail by ListingKey: cache → else Trestle → cache, plus opportunistic image jobs
// Detail by ListingKey: cache → else Trestle → cache, plus opportunistic image jobs
app.get('/webapi/property/by-id', async (req, res) => {
    try {
      const listingKey = String(req.query.listingKey || '').trim();
      if (!listingKey) return res.status(400).json({ error: 'listingKey is required' });
  
      const per = Math.min(Math.max(parseInt(req.query.gallery || '0', 10), 0), 20); // how many gallery items to return
      const force = String(req.query.force || '0') === '1';
  
      // 1) Try cache (Dynamo)
      const { GetCommand, UpdateCommand } = require('@aws-sdk/lib-dynamodb');
      const { Item } = await ddb.send(new GetCommand({
        TableName: DDB_TABLE_LISTINGS,
        Key: { ListingKey: listingKey },
        ProjectionExpression: [
          'ListingKey','City','StateOrProvince','PostalCode','ListPrice',
          'BedroomsTotal','BathroomsTotalInteger','LivingArea','StandardStatus',
          'PrimaryPhotoUrl','CdnPrimary400','Gallery400Count',
          'PhotosChangeTimestamp','ImagesUpdatedAt',
          'Detail','DetailCachedAt'
        ].join(',')
      }));
  
      // Precompute a CDN gallery (if we have a count already)
      const cdnGallery = [];
      if (Item?.Gallery400Count && CDN_BASE) {
        const count = Math.min(Item.Gallery400Count, per || Item.Gallery400Count);
        for (let i = 1; i <= count; i++) {
          cdnGallery.push(`${CDN_BASE}/gallery/400/${listingKey}/${i}.webp`);
        }
      }
  
      // ---------------- cache HIT ----------------
      if (Item?.Detail && !force) {
        // opportunistic background jobs (non-blocking)
        (async () => {
          try {
            if (QUEUE_URL) {
              const stale = isImageStale(Item || {});
              if (!Item.CdnPrimary400 || stale) {
                await sqs.send(new SendMessageCommand({
                  QueueUrl: QUEUE_URL,
                  MessageBody: JSON.stringify({ type: 'primary', listingKey, width: 400 })
                }));
              }
              if (per > 0 && (stale || (Item.Gallery400Count || 0) < per)) {
                await sqs.send(new SendMessageCommand({
                  QueueUrl: QUEUE_URL,
                  MessageBody: JSON.stringify({ type: 'gallery', listingKey, width: 400, per })
                }));
              }
            }
          } catch (e) {
            console.warn('opportunistic enqueue failed', e?.message || e);
          }
        })();
  
        let galleryOut = cdnGallery;
        let primaryOut = Item.CdnPrimary400 || Item.PrimaryPhotoUrl || null;
  
        // NEW: if caller asked for gallery and we don't have CDN yet, fetch top-N from Trestle now
        if (per > 0 && cdnGallery.length === 0) {
          try {
            const esc = s => s.replace(/'/g, "''");
            const p = new URLSearchParams();
            p.set('$filter', `ListingKey eq '${esc(listingKey)}'`);
            p.set('$select', 'ListingKey,PhotosChangeTimestamp');
            p.set('$expand', `Media($select=MediaURL,Order,ModificationTimestamp;$orderby=Order;$top=${per})`);
  
            const data = await trestleFetch(`/Property?${p.toString()}`);
            const v = data?.value?.[0];
            const mediaSrc = Array.isArray(v?.Media) ? v.Media.map(m => m?.MediaURL).filter(Boolean) : [];
            galleryOut = mediaSrc.slice(0, per);
            if (!primaryOut) primaryOut = galleryOut[0] || null;
  
            // Backfill PrimaryPhotoUrl if missing
            if (!Item.PrimaryPhotoUrl && galleryOut[0]) {
              await ddb.send(new UpdateCommand({
                TableName: DDB_TABLE_LISTINGS,
                Key: { ListingKey: listingKey },
                UpdateExpression: 'SET PrimaryPhotoUrl = if_not_exists(PrimaryPhotoUrl, :p)',
                ExpressionAttributeValues: { ':p': galleryOut[0] }
              }));
            }
          } catch (e) {
            console.warn('fallback trestle media failed', e?.message || e);
          }
        }
  
        return res.set('Cache-Control', 'private, max-age=15').json({
          cache: 'hit',
          listingKey,
          property: Item.Detail,
          primary: primaryOut,
          gallery: galleryOut
        });
      }
  
      // ---------------- cache MISS or forced refresh ----------------
const select = [
  'ListingKey','ListingId','StandardStatus',
  'UnparsedAddress','StreetNumber','StreetNumberNumeric','StreetDirPrefix','StreetName','StreetSuffix','UnitNumber',
  'City','StateOrProvince','PostalCode',
  'ListPrice','BedroomsTotal','BathroomsTotalInteger','LivingArea','LotSizeArea','YearBuilt',
  'PropertySubType','PropertyType','PublicRemarks',
  // --- Minimal agent/office fields ---
  'ListAgentFullName',
  'ListAgentStateLicense',
  'ListOfficeName',
  // --- Timestamps ---
  'ModificationTimestamp','PhotosChangeTimestamp'
].join(',');

  
      const esc = s => s.replace(/\'/g, "''");
      const params = new URLSearchParams();
      params.set('$select', select);
      params.set('$filter', `ListingKey eq '${esc(listingKey)}'`);
      if (per > 0) {
        params.set('$expand', `Media($select=MediaURL,Order,ModificationTimestamp;$orderby=Order;$top=${per})`);
      }
      if (PRETTY_ENUMS === 'true') params.set('PrettyEnums', 'true');
  
      const data = await trestleFetch(`/Property?${params.toString()}`);
      const v = (data.value || [])[0];
      if (!v) return res.status(404).json({ error: 'not found' });
  
      const detail = {
  ListingKey: v.ListingKey,
  ListingId: v.ListingId ?? null,
  StandardStatus: v.StandardStatus ?? null,

  Address: {
    Unparsed: v.UnparsedAddress ?? null,
    StreetNumber: v.StreetNumber ?? null,
    StreetNumberNumeric: v.StreetNumberNumeric ?? null,
    StreetDirPrefix: v.StreetDirPrefix ?? null,
    StreetName: v.StreetName ?? null,
    StreetSuffix: v.StreetSuffix ?? null,
    UnitNumber: v.UnitNumber ?? null,
    City: v.City ?? null,
    StateOrProvince: v.StateOrProvince ?? null,
    PostalCode: v.PostalCode ?? null
  },

  Facts: {
    ListPrice: v.ListPrice ?? null,
    BedroomsTotal: v.BedroomsTotal ?? null,
    BathroomsTotalInteger: v.BathroomsTotalInteger ?? null,
    LivingArea: v.LivingArea ?? null,
    LotSizeArea: v.LotSizeArea ?? null,
    YearBuilt: v.YearBuilt ?? null,
    PropertyType: v.PropertyType ?? null,
    PropertySubType: v.PropertySubType ?? null
  },

  // --- Minimal agent info ---
  Agent: {
    Name: v.ListAgentFullName ?? null,
    License: v.ListAgentStateLicense ?? null
  },

  // --- Minimal office info ---
  Office: {
    Name: v.ListOfficeName ?? null
  },

  PublicRemarks: v.PublicRemarks ?? null,
  ModificationTimestamp: v.ModificationTimestamp ?? null,
  PhotosChangeTimestamp: v.PhotosChangeTimestamp ?? null
};

  
      const mediaSrc = Array.isArray(v.Media) ? v.Media.map(m => m?.MediaURL).filter(Boolean) : [];
      const primarySrc = mediaSrc[0] || null;
  
      await ddb.send(new UpdateCommand({
        TableName: DDB_TABLE_LISTINGS,
        Key: { ListingKey: listingKey },
        UpdateExpression: `
          SET Detail = :detail,
              DetailCachedAt = :now,
              PrimaryPhotoUrl = if_not_exists(PrimaryPhotoUrl, :primary),
              PhotosChangeTimestamp = if_not_exists(PhotosChangeTimestamp, :pct)
        `.replace(/\s+/g, ' ').trim(),
        ExpressionAttributeValues: {
          ':detail': detail,
          ':now': new Date().toISOString(),
          ':primary': primarySrc ?? null,
          ':pct': v.PhotosChangeTimestamp ?? null
        }
      }));
  
      // opportunistic jobs
      (async () => {
        try {
          if (QUEUE_URL) {
            const stale = isImageStale(Item || {});
            if (!Item?.CdnPrimary400 || stale) {
              await sqs.send(new SendMessageCommand({
                QueueUrl: QUEUE_URL,
                MessageBody: JSON.stringify({ type: 'primary', listingKey, width: 400 })
              }));
            }
            if (per > 0 && (stale || (Item?.Gallery400Count || 0) < per)) {
              await sqs.send(new SendMessageCommand({
                QueueUrl: QUEUE_URL,
                MessageBody: JSON.stringify({ type: 'gallery', listingKey, width: 400, per })
              }));
            }
          }
        } catch (e) {
          console.warn('opportunistic enqueue failed', e?.message || e);
        }
      })();
  
      res.set('Cache-Control', 'private, max-age=15').json({
        cache: 'miss',
        listingKey,
        property: detail,
        primary: (Item?.CdnPrimary400 || Item?.PrimaryPhotoUrl || null) ?? primarySrc,
        gallery: mediaSrc.slice(0, per)
      });
    } catch (err) {
      console.error(err);
      res.status(502).json({ error: 'detail fetch failed', message: err?.message || String(err) });
    }
  });

// Seed SQS with primary-400 image jobs for a city
app.get('/admin/seed/primary', async (req, res) => {
  try {
    const city = String(req.query.city || '').trim().toLowerCase();
    if (!city) return res.status(400).json({ error: 'city is required' });

    const status = String(req.query.status || 'Active').trim();
    const limit = Math.min(Math.max(parseInt(req.query.limit || '200', 10), 1), 500);

    // pagination cursor (opaque)
    const cursor = req.query.cursor
      ? JSON.parse(Buffer.from(String(req.query.cursor), 'base64').toString('utf8'))
      : undefined;

    const params = {
      TableName: DDB_TABLE_LISTINGS,
      IndexName: 'CityNorm-ModificationTimestamp-index',
      KeyConditionExpression: 'CityNorm = :c',
      FilterExpression: 'StandardStatus = :s AND attribute_not_exists(CdnPrimary400)',
      ExpressionAttributeValues: { ':c': city, ':s': status },
      ScanIndexForward: false, // newest first
      Limit: limit,
      ExclusiveStartKey: cursor
    };

    const resp = await ddb.send(new QueryCommand(params));
    const keys = (resp.Items || []).map(i => i.ListingKey).filter(Boolean);

    const enqueued = keys.length ? await enqueuePrimaryBatch(keys) : 0;
    const next =
      resp.LastEvaluatedKey
        ? Buffer.from(JSON.stringify(resp.LastEvaluatedKey)).toString('base64')
        : null;

    res.json({
      ok: true,
      city,
      status,
      scanned: resp.Count || 0,
      enqueued,
      cursor: next
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ ok: false, error: 'seed failed', name: err.name, message: err.message });
  }
});

// commercial 
// GET /api/commercial/search/city?city=Covina&limit=50

app.get('/api/commercial/search/city', async (req, res) => {
    try {
      const city = String(req.query.city || '').trim().toLowerCase();
      if (!city) return res.status(400).json({ error: 'city is required' });
  
      const limit = Math.min(Math.max(parseInt(req.query.limit || '50', 10), 1), 100);
  
      // pagination cursor (opaque)
      const cursor = req.query.cursor
        ? JSON.parse(Buffer.from(String(req.query.cursor), 'base64').toString('utf8'))
        : undefined;
  
      const params = {
        TableName: DDB_TABLE_COMMERCIAL_LISTINGS,
        IndexName: 'CityNorm-ModificationTimestamp-index',
        KeyConditionExpression: 'CityNorm = :c',
        ExpressionAttributeValues: { ':c': city },
        ScanIndexForward: false,  // newest first
        Limit: limit,
        ExclusiveStartKey: cursor
      };
  
      const resp = await ddb.send(new QueryCommand(params));
  
      // map to thin UI shape
      const listings = (resp.Items || [])
        // hide inactive
        .filter(it => it.IsActive !== false)
        .map(v => ({
          listingId: v.ListingId,
          title: v.Title || v.Address || null,
          city: v.City || null,
          state: v.State || null,
          postalCode: v.PostalCode || null,
          address: v.Address || null,
          listingType: v.ListingType || null,
          price: v.PriceRaw || null,
          shortSummary: v.ShortSummary || null,
          primaryPhotoUrl: v.CdnPrimary400 || v.PrimaryPhotoUrl || null,
          modificationTimestamp: v.ModificationTimestamp || null
        }));
  
      const next =
        resp.LastEvaluatedKey
          ? Buffer.from(JSON.stringify(resp.LastEvaluatedKey)).toString('base64')
          : null;
  
      res.set('Cache-Control', 'private, max-age=15');
      res.json({
        city,
        returned: listings.length,
        cursor: next,
        listings
      });
    } catch (err) {
      console.error(err);
      res.status(500).json({ error: 'commercial search failed', message: err?.message || String(err) });
    }
  });

  // Quick peek into DynamoDB to see what ingest wrote
app.get('/admin/ddb/peek', async (req, res) => {
  try {
    const cityRaw = String(req.query.city || '').trim().toLowerCase();
    const countyRaw = String(req.query.county || '').trim();
    const slcRaw = String(req.query.slc || '').trim();
    const limit = Math.min(Math.max(parseInt(req.query.limit || '50', 10), 1), 200);

    // we’ll build a Query if we can, else fall back to Scan (not ideal)
    // best case: user gave city → we can use CityNorm-ModificationTimestamp-index
    if (cityRaw) {
      const params = {
        TableName: DDB_TABLE_LISTINGS,
        IndexName: 'CityNorm-ModificationTimestamp-index',
        KeyConditionExpression: 'CityNorm = :c',
        ExpressionAttributeValues: {
          ':c': cityRaw
        },
        ScanIndexForward: false,
        Limit: limit
      };

      const resp = await ddb.send(new QueryCommand(params));
      return res.json({
        mode: 'city',
        count: resp.Count || 0,
        items: resp.Items || []
      });
    }

    // if county given, we don’t have a county index, so do a small scan
    if (countyRaw) {
      const params = {
        TableName: DDB_TABLE_LISTINGS,
        FilterExpression: 'CountyOrParish = :co',
        ExpressionAttributeValues: {
          ':co': countyRaw
        },
        Limit: limit
      };
      const resp = await ddb.send(new QueryCommand(params)).catch(() => null);

      // QueryCommand can’t filter on non-key attributes without index, so do a Scan instead
      if (!resp) {
        const { ScanCommand } = require('@aws-sdk/lib-dynamodb');
        const scanResp = await ddb.send(new ScanCommand({
          TableName: DDB_TABLE_LISTINGS,
          FilterExpression: 'CountyOrParish = :co',
          ExpressionAttributeValues: { ':co': countyRaw },
          Limit: limit
        }));
        return res.json({
          mode: 'county-scan',
          count: scanResp.Count || 0,
          items: scanResp.Items || []
        });
      }

      return res.json({
        mode: 'county',
        count: resp.Count || 0,
        items: resp.Items || []
      });
    }

    // SLC-only peek (e.g. just “show me probate ones you’ve got”)
    if (slcRaw) {
      const { ScanCommand } = require('@aws-sdk/lib-dynamodb');
      const scanResp = await ddb.send(new ScanCommand({
        TableName: DDB_TABLE_LISTINGS,
        FilterExpression: 'contains(SpecialListingConditions, :slc)',
        ExpressionAttributeValues: { ':slc': slcRaw },
        Limit: limit
      }));
      return res.json({
        mode: 'slc-scan',
        count: scanResp.Count || 0,
        items: scanResp.Items || []
      });
    }

    return res.status(400).json({ error: 'Provide city= or county= or slc=' });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'peek failed', message: err.message });
  }
});

// quick: see what SpecialListingConditions looks like from Trestle
app.get('/admin/tst/slc-values', async (req, res) => {
  try {
    // small sample
    const p = new URLSearchParams();
    p.set('$top', '50');
    p.set('$select', 'ListingKey,SpecialListingConditions');
    if (PRETTY_ENUMS === 'true') p.set('PrettyEnums', 'true');

    const data = await trestleFetch(`/Property?${p.toString()}`);

    const all = (data.value || []).flatMap(v => {
      if (Array.isArray(v.SpecialListingConditions)) return v.SpecialListingConditions;
      if (v.SpecialListingConditions) return [v.SpecialListingConditions];
      return [];
    });

    const unique = [...new Set(all)].sort((a, b) => a.localeCompare(b));
    res.json({ count: unique.length, values: unique });
  } catch (err) {
    console.error(err);
    res.status(502).json({ error: 'Failed to fetch SLC sample', message: err.message });
  }
});

// quick sample of distinct ListingTerms values from recent properties
app.get('/admin/tst/listingterms-values', async (req, res) => {
  try {
    const limit = Math.min(Math.max(parseInt(req.query.top || '200', 10), 1), 1000);
    const days = Math.min(Math.max(parseInt(req.query.days || '30', 10), 1), 365);

    const since = new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString();
    const esc = s => s.replace(/'/g, "''");

    const params = new URLSearchParams();
    params.set('$select', 'ListingKey,ListingTerms,ModificationTimestamp');
    params.set('$filter', `ModificationTimestamp ge ${since} and InternetEntireListingDisplayYN eq true`);
    params.set('$orderby', 'ModificationTimestamp desc');
    params.set('$top', String(limit));
    if (PRETTY_ENUMS === 'true') params.set('PrettyEnums', 'true');

    const data = await trestleFetch(`/Property?${params.toString()}`);
    const rows = Array.isArray(data.value) ? data.value : [];

    // ListingTerms sometimes comes back as:
    // - a single string: "Cash"
    // - a comma string: "Cash,Conventional"
    // - an array (less common, depends on feed)
    const set = new Set();

    for (const r of rows) {
      const lt = r.ListingTerms;
      if (!lt) continue;

      if (Array.isArray(lt)) {
        lt.forEach(v => v && set.add(String(v).trim()));
      } else {
        // assume string, split on comma
        String(lt)
          .split(',')
          .map(s => s.trim())
          .filter(Boolean)
          .forEach(v => set.add(v));
      }
    }

    res.json({
      count: set.size,
      values: Array.from(set).sort()
    });
  } catch (err) {
    console.error(err);
    res.status(502).json({ error: 'listingterms probe failed', message: err.message });
  }
});

// probe remarks for keywords (public+private-ish) to see if MLS is surfacing them
// probe remarks for keywords (public + private) — client-side filter
app.get('/admin/tst/remarks', async (req, res) => {
  try {
    const containsRaw = String(req.query.contains || '').trim();
    if (!containsRaw) {
      return res.status(400).json({ error: 'contains= is required' });
    }

    const needle = containsRaw.toLowerCase();
    const days = Math.min(Math.max(parseInt(req.query.days || '30', 10), 1), 365);
    const top = Math.min(Math.max(parseInt(req.query.top || '200', 10), 1), 1000);

    const since = new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString();
    const esc = s => s.replace(/'/g, "''");

    // only fields we know exist
    const select = [
      'ListingKey',
      'City',
      'CountyOrParish',
      'StandardStatus',
      'PublicRemarks',
      'PrivateRemarks',
      'ModificationTimestamp'
    ].join(',');

    // broad-ish filter: recent + visible
    const filter = [
      `ModificationTimestamp ge ${since}`,
      `InternetEntireListingDisplayYN eq true`
    ].join(' and ');

    const params = new URLSearchParams();
    params.set('$select', select);
    params.set('$filter', filter);
    params.set('$orderby', 'ModificationTimestamp desc');
    params.set('$top', String(top));
    // if you like pretty enums everywhere:
    if (process.env.PRETTY_ENUMS === 'true') params.set('PrettyEnums', 'true');

    const data = await trestleFetch(`/Property?${params.toString()}`);

    const hits = [];
    for (const v of (data.value || [])) {
      const pub = String(v.PublicRemarks || '').toLowerCase();
      const priv = String(v.PrivateRemarks || '').toLowerCase();

      const hitPublic = pub.includes(needle);
      const hitPrivate = priv.includes(needle);

      if (hitPublic || hitPrivate) {
        hits.push({
          ListingKey: v.ListingKey,
          City: v.City,
          CountyOrParish: v.CountyOrParish ?? null,
          StandardStatus: v.StandardStatus,
          hits: {
            public: hitPublic,
            private: hitPrivate
          },
          PublicRemarks: v.PublicRemarks || null,
          PrivateRemarks: v.PrivateRemarks || null,
          ModificationTimestamp: v.ModificationTimestamp
        });
      }
    }

    res.set('Cache-Control', 'no-store').json({
      contains: containsRaw,
      days,
      scanned: (data.value || []).length,
      matched: hits.length,
      items: hits
    });
  } catch (err) {
    console.error(err);
    res.status(502).json({ error: 'remarks probe failed', message: err.message });
  }
});

// find listings in Dynamo by city + partial address
app.get('/admin/ddb/find-by-address', async (req, res) => {
  try {
    const cityRaw = String(req.query.city || '').trim().toLowerCase();
    const containsRaw = String(req.query.contains || '').trim();
    const limit = Math.min(Math.max(parseInt(req.query.limit || '50', 10), 1), 200);

    if (!cityRaw) {
      return res.status(400).json({ error: 'city is required' });
    }
    if (!containsRaw) {
      return res.status(400).json({ error: 'contains is required (street part)' });
    }

    // 1) query by city using the GSI you already use: CityNorm-ModificationTimestamp-index
    const params = {
      TableName: DDB_TABLE_LISTINGS,
      IndexName: 'CityNorm-ModificationTimestamp-index',
      KeyConditionExpression: 'CityNorm = :c',
      ExpressionAttributeValues: {
        ':c': cityRaw
      },
      ScanIndexForward: false,
      Limit: 500 // pull a chunk, we’ll post-filter
    };

    const { QueryCommand } = require('@aws-sdk/lib-dynamodb');
    const resp = await ddb.send(new QueryCommand(params));
    const items = resp.Items || [];

    // 2) post-filter in JS by address substring, case-insensitive
    const needle = containsRaw.toLowerCase();
    const matched = items
      .filter(it => {
        if (it.InternetAddressDisplayYN === false) return false; // respect MLS rules
        const addr = (it.UnparsedAddress || '').toLowerCase();
        return addr.includes(needle);
      })
      .slice(0, limit);

    return res.json({
      city: cityRaw,
      contains: containsRaw,
      returned: matched.length,
      items: matched
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'find-by-address failed', message: err.message });
  }
});

// Public version of address lookup (no API key required)
app.get('/public/ddb/find-by-address', async (req, res) => {
  try {
    const cityRaw = String(req.query.city || '').trim().toLowerCase();
    const containsRaw = String(req.query.contains || '').trim();
    const limit = Math.min(Math.max(parseInt(req.query.limit || '10', 10), 1), 50);

    if (!cityRaw || !containsRaw) {
      return res.status(400).json({ error: 'city and contains are required' });
    }

    // Simple scan (safe for dev, small limit)
    const { ScanCommand } = require('@aws-sdk/lib-dynamodb');
    const resp = await ddb.send(new ScanCommand({
      TableName: DDB_TABLE_LISTINGS,
      FilterExpression: 'CityNorm = :c AND contains(UnparsedAddress, :addr)',
      ExpressionAttributeValues: {
        ':c': cityRaw,
        ':addr': containsRaw
      },
      Limit: limit
    }));

    res.set('Cache-Control', 'no-store').json({
      city: cityRaw,
      contains: containsRaw,
      returned: resp.Count || 0,
      items: resp.Items || []
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'public find failed', message: err.message });
  }
});

// === Open House (Trestle OData) ==============================================
// GET /webapi/openhouse/by-listing?listingKey=12345&days=30&top=50
// Returns upcoming (or recent window) open houses for a listing.
app.get('/webapi/openhouse/by-listing', async (req, res) => {
  try {
    const listingKey = String(req.query.listingKey || '').trim();
    if (!listingKey) return res.status(400).json({ error: 'listingKey is required' });

    const days = Math.min(Math.max(parseInt(req.query.days || '30', 10), 1), 365);
    const top = Math.min(Math.max(parseInt(req.query.top || '50', 10), 1), 200);

    // window: from "now - days" to future (covers upcoming + recent)
    const nowIso = new Date().toISOString();
    const sinceIso = new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString();

    const esc = s => s.replace(/'/g, "''");

    // pick a conservative set of fields across RESO implementations
    const select = [
      'OpenHouseKey',
      'ListingKey',
      'OpenHouseType',             // e.g. "Public"
      'OpenHouseStartTime',        // DateTime
      'OpenHouseEndTime',          // DateTime
      'OpenHouseRemarks',          // Optional text
      'ModificationTimestamp'
    ].join(',');

    // filter by ListingKey and date window (start >= since) and end in future if you want strictly upcoming
    const filter = [
      `ListingKey eq '${esc(listingKey)}'`,
      `OpenHouseStartTime ge ${sinceIso}`
      // If you want strictly future events, use:
      // `OpenHouseEndTime ge ${nowIso}`
    ].join(' and ');

    const p = new URLSearchParams();
    p.set('$select', select);
    p.set('$filter', filter);
    p.set('$orderby', 'OpenHouseStartTime asc');
    p.set('$top', String(top));
    if (PRETTY_ENUMS === 'true') p.set('PrettyEnums', 'true');

    const data = await trestleFetch(`/OpenHouse?${p.toString()}`);
    const rows = Array.isArray(data.value) ? data.value : [];

    res.set('Cache-Control', 'private, max-age=15').json({
      listingKey,
      returned: rows.length,
      items: rows
    });
  } catch (err) {
    console.error(err);
    res.status(502).json({ error: 'openhouse fetch failed', message: err?.message || String(err) });
  }
});

// Optional: RETS search variant (some MLSes prefer this path)
// GET /webapi/openhouse/rets?listingKey=12345&class=OpenHouse
app.get('/webapi/openhouse/rets', async (req, res) => {
  try {
    const listingKey = String(req.query.listingKey || '').trim();
    if (!listingKey) return res.status(400).json({ error: 'listingKey is required' });

    // Pass through to Trestle RETS search endpoint via trestleFetch (absolute URL)
    const url = `https://api-trestle.corelogic.com/trestle/rets/search` +
                `?SearchType=OpenHouse&Class=OpenHouse&Query=(ListingKey=${encodeURIComponent(listingKey)})` +
                `&Format=JSON`;
    const data = await trestleFetch(url); // returns JSON when Format=JSON

    res.set('Cache-Control', 'private, max-age=15').json({
      listingKey,
      items: data?.value || data || []
    });
  } catch (err) {
    console.error(err);
    res.status(502).json({ error: 'openhouse rets fetch failed', message: err?.message || String(err) });
  }
});


/* --------------------------------- Server -------------------------------- */
app.listen(PORT, () => {
  console.log(`Server listening on :${PORT}`);
});

// ===== PHASE 2 END =====