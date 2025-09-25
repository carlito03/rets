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
const { DynamoDBDocumentClient, UpdateCommand, BatchWriteCommand, GetCommand } = require('@aws-sdk/lib-dynamodb');
const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');
const { SQSClient, SendMessageBatchCommand } = require('@aws-sdk/client-sqs');

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

// SQS client (same creds as S3 if you set custom env var names)
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
  PRETTY_ENUMS = 'true'
} = process.env;

// SQS images queue URL (full URL, not ARN), e.g. https://sqs.us-east-1.amazonaws.com/4371.../IDXPlus-Images-Queue
const { QUEUE_URL = '' } = process.env;


if (!TRESTLE_CLIENT_ID || !TRESTLE_CLIENT_SECRET) {
  console.error('Missing TRESTLE_CLIENT_ID / TRESTLE_CLIENT_SECRET');
}

const API_KEY_SET = new Set(
  API_KEYS.split(',').map(s => s.trim()).filter(Boolean)
);

const ODATA_BASE = 'https://api-trestle.corelogic.com/trestle/odata';
const TOKEN_URL = 'https://api-trestle.corelogic.com/trestle/oidc/connect/token';

/* ------------------------------- CORS setup ------------------------------ */
// If "*" then allow all (no credentials). Otherwise allow only listed origins.
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
  if (req.path === '/health') return next(); // healthcheck open

  const key = req.header('x-api-key');
  if (!API_KEY_SET.size) {
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

/* --------------------------------- helpers -------------------------------- */
const sleep = ms => new Promise(r => setTimeout(r, ms));
const chunk25 = arr => { const out = []; for (let i = 0; i < arr.length; i += 25) out.push(arr.slice(i, i + 25)); return out; };
const toNorm = s => String(s || '').trim().toLowerCase();
const toIsoUtc = s => { try { return new Date(s).toISOString(); } catch { return s; } };
const toEpoch = iso => Math.floor(new Date(iso || Date.now()).getTime() / 1000);

/* -------------------------- SQS enqueue helpers --------------------------- */
async function enqueuePrimaryBatch(listingKeys = []) {
    if (!QUEUE_URL) throw new Error('QUEUE_URL is not set');
  
    let enqueued = 0;
    for (let i = 0; i < listingKeys.length; i += 10) {
      const slice = listingKeys.slice(i, i + 10);
      const Entries = slice.map((key, idx) => ({
        Id: `${i + idx}`,
        MessageBody: JSON.stringify({ type: 'primary', listingKey: String(key), width: 400 })
      }));
  
      const resp = await sqs.send(new SendMessageBatchCommand({
        QueueUrl: QUEUE_URL,
        Entries
      }));
  
      enqueued += (Entries.length - (resp.Failed?.length || 0));
      if (resp.Failed?.length) console.warn('SQS failed entries', resp.Failed);
      await sleep(50); // gentle
    }
    return enqueued;
  }
  

/* ---------------------------- idempotent upsert --------------------------- */
async function upsertThinListing(item) {
  const {
    ListingKey,
    City,
    PostalCode,
    StateOrProvince,
    StandardStatus,
    ListPrice,
    BedroomsTotal,
    BathroomsTotalInteger,
    LivingArea,
    ModificationTimestamp,
    PhotosChangeTimestamp,
    PrimaryPhotoUrl
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
        PostalCode = :PostalCode,
        StateOrProvince = :StateOrProvince,
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
        IsActive = :IsActive
  `.replace(/\s+/g, ' ').trim();

  const params = {
    TableName: DDB_TABLE_LISTINGS,
    Key: { ListingKey }, // match your table PK
    UpdateExpression,
    ConditionExpression: 'attribute_not_exists(ModEpoch) OR ModEpoch <= :ModEpoch',
    ExpressionAttributeValues: {
      ':City': City || null,
      ':CityNorm': CityNorm,
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
      ':IsActive': IsActive
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
      'PhotosChangeTimestamp'
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
app.get('/admin/ingest/city', async (req, res) => {
  try {
    const cityRaw = String(req.query.city || '').trim();
    if (!cityRaw) return res.status(400).json({ error: 'city is required' });

    const days = Math.min(Math.max(parseInt(req.query.days || '90', 10), 1), 365);
    const status = String(req.query.status || 'Active').trim();
    const since = new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString();

    const esc = s => s.replace(/'/g, "''");

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
      `tolower(City) eq '${esc(cityRaw.toLowerCase())}'`,
      `StandardStatus eq '${esc(status)}'`,
      `InternetEntireListingDisplayYN eq true`,
      `ModificationTimestamp ge ${since}`
    ].join(' and ');

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
        PostalCode: v.PostalCode,
        StateOrProvince: v.StateOrProvince,
        StandardStatus: v.StandardStatus,
        ListPrice: v.ListPrice,
        BedroomsTotal: v.BedroomsTotal,
        BathroomsTotalInteger: v.BathroomsTotalInteger,
        LivingArea: v.LivingArea,
        ModificationTimestamp: toIsoUtc(v.ModificationTimestamp),
        PhotosChangeTimestamp: toIsoUtc(v.PhotosChangeTimestamp),
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
      city: cityRaw, status, since, fetched: all.length, written, skipped
    });
  } catch (err) {
    console.error(err);
    const payload = { error: 'Ingest failed' };
    if (req.query.debug === '1') payload.details = { name: err.name, message: err.message };
    res.status(502).json(payload);
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
        ScanIndexForward: false,
        Limit: limit,
        ExclusiveStartKey: cursor
      };
  
      const resp = await ddb.send(new QueryCommand(params));
      const keys = (resp.Items || []).map(i => i.ListingKey).filter(Boolean);
  
      const enqueued = await enqueuePrimaryBatch(keys);
      const next = resp.LastEvaluatedKey
        ? Buffer.from(JSON.stringify(resp.LastEvaluatedKey)).toString('base64')
        : null;
  
      res.json({ city, status, scanned: (resp.Count || 0), enqueued, cursor: next });
    } catch (err) {
      console.error(err);
      res.status(500).json({ error: 'seed failed', name: err.name, message: err.message });
    }
  });
  

// quick DDB ping
app.get('/admin/ddb/ping', async (req, res) => {
  try {
    const meta = await ddb.config.client.send(
        new DescribeTableCommand({ TableName: DDB_TABLE_LISTINGS })
      );
      
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
      // This is the URL you should be able to open in the browser
      cdnUrl: CDN_BASE ? `${CDN_BASE}/${Key}` : null,
      tip: CDN_BASE ? 'Open cdnUrl in your browser' : 'Set CDN_BASE to your CloudFront domain'
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ ok: false, name: err.name, message: err.message });
  }
});


app.get('/api/search/city', async (req, res) => {
  try {
    const city = String(req.query.city || '').trim().toLowerCase();
    if (!city) return res.status(400).json({ error: 'city is required' });

    const status = String(req.query.status || 'Active').trim();
    const limit = Math.min(Math.max(parseInt(req.query.limit || '50', 10), 1), 100);

    // pagination cursor (opaque)
    const cursor = req.query.cursor
      ? JSON.parse(Buffer.from(String(req.query.cursor), 'base64').toString('utf8'))
      : undefined;

    const params = {
      TableName: DDB_TABLE_LISTINGS,
      IndexName: 'CityNorm-ModificationTimestamp-index',
      KeyConditionExpression: 'CityNorm = :c',
      FilterExpression: 'StandardStatus = :s',
      ExpressionAttributeValues: { ':c': city, ':s': status },
      ScanIndexForward: false,          // newest first (by ModificationTimestamp)
      Limit: limit,
      ExclusiveStartKey: cursor
    };

    const resp = await ddb.send(new QueryCommand(params));
    const next =
      resp.LastEvaluatedKey
        ? Buffer.from(JSON.stringify(resp.LastEvaluatedKey)).toString('base64')
        : null;

    // map thin fields the UI needs
    const listings = (resp.Items || []).map(v => ({
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
      primaryPhotoUrl: v.CdnPrimary400 ?? v.PrimaryPhotoUrl ?? null
    }));

    res.set('Cache-Control', 'private, max-age=15');
    res.json({ city, status, returned: listings.length, cursor: next, listings });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'search failed' });
  }
});

// Details: get one listing, cache full record in DDB if missing/stale
app.get('/api/listing/:listingKey', async (req, res) => {
    try {
      const ListingKey = String(req.params.listingKey || '').trim();
      if (!ListingKey) return res.status(400).json({ error: 'listingKey required' });
  
      // 1) Try DDB
      let item = (await ddb.send(new GetCommand({
        TableName: DDB_TABLE_LISTINGS,
        Key: { ListingKey }
      }))).Item;
  
      const staleMs = 12 * 3600 * 1000; // 12h
      const needRefresh =
        !item?.Details ||
        !item?.DetailsUpdatedAt ||
        ((Date.now() - new Date(item.DetailsUpdatedAt).getTime()) > staleMs);
  
      if (needRefresh) {
        const esc = s => s.replace(/'/g, "''");
        const filter = `ListingKey eq '${esc(ListingKey)}'`;
        const expand = 'Media($select=MediaURL,Order;$orderby=Order;$top=10)';
        const params = new URLSearchParams();
        params.set('$filter', filter);
        params.set('$expand', expand);
  
        const data = await trestleFetch(`/Property?${params.toString()}`);
        const det = (data.value && data.value[0]) || null;
  
        if (det) {
          const PrimaryPhotoUrl =
            Array.isArray(det.Media) && det.Media.length
              ? det.Media[0].MediaURL
              : (item?.PrimaryPhotoUrl ?? null);
  
          const updated = await ddb.send(new UpdateCommand({
            TableName: DDB_TABLE_LISTINGS,
            Key: { ListingKey },
            UpdateExpression: 'SET Details = :d, DetailsUpdatedAt = :t, PrimaryPhotoUrl = if_not_exists(PrimaryPhotoUrl, :p)',
            ExpressionAttributeValues: {
              ':d': det,
              ':t': new Date().toISOString(),
              ':p': PrimaryPhotoUrl ?? null
            },
            ReturnValues: 'ALL_NEW'
          }));
          item = updated.Attributes;
  
          // No CDN yet? enqueue a primary job so UI can show image soon
          if (!item.CdnPrimary400 && PrimaryPhotoUrl && QUEUE_URL) {
            await enqueuePrimaryBatch([ListingKey]);
          }
        }
      }
  
      res.set('Cache-Control', 'private, max-age=30');
      res.json({ listing: item || { ListingKey } });
    } catch (err) {
      console.error(err);
      res.status(500).json({ error: 'details failed', name: err.name, message: err.message });
    }
  });
  



/* --------------------------------- Server -------------------------------- */
app.listen(PORT, () => {
  console.log(`Server listening on :${PORT}`);
});

// ===== PHASE 2 END =====