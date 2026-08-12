# mediafrik analytics proxy

Serves Ghost 6's Analytics tab without Tinybird.

Ghost talks to Tinybird over exactly two surfaces: it appends page-view events to
a datasource, and it reads a fixed list of pipes at `/v0/pipes/<name>.json`.
Nothing else about Tinybird is load-bearing, so a service that ingests the same
events and answers the same pipe URLs replaces it outright.

Runs on Node 22 with **no dependencies** — only built-ins, including
`node:sqlite` for the store. There is no `package.json` and no install step.

## Two modes

**Relay** (the original behaviour): enrich each hit and forward it to Tinybird.
The browser tracker posts no `session_id`, but the `analytics_events` datasource
declares it non-nullable, so without this every hit is quarantined.

**Store** (set `STORE_PATH`): keep the hits in a local SQLite file and answer the
pipe queries directly. No quota, no external dependency.

Both can run at once — hits land in both places. That is how you build local
history before cutting over, and how you check the numbers agree.

## Configuration

| Variable | Default | Notes |
|---|---|---|
| `PORT` | `3000` | |
| `BIND` | `127.0.0.1` | `0.0.0.0` in a container |
| `ALLOWED_ORIGIN` | `http://localhost:2368` | the site origin; `*` when same-origin behind a reverse proxy |
| `TRUSTED_PROXY_CIDRS` | private ranges | who may set `X-Forwarded-For` |
| `XFF_TRUST_HOPS` | `1` | trusted hops between client and here |
| `TRUST_CF_CONNECTING_IP` | on | set `false` to ignore Cloudflare's header |

**Relay**

| Variable | Default | Notes |
|---|---|---|
| `TB_RELAY` | on | `false` to stop forwarding to Tinybird |
| `TB_TRACKER_TOKEN` | — | required while relaying; falls back to `./.tb-tracker-token` |
| `TB_EVENTS_HOST` | europe-west2 | Tinybird region API base |
| `TB_DATASOURCE` | `analytics_events` | |
| `BATCH_INTERVAL_MS` / `BATCH_MAX_EVENTS` / `BATCH_MAX_BUFFER` | `10000` / `100` / `5000` | `BATCH_INTERVAL_MS=0` disables batching |

**Store**

| Variable | Default | Notes |
|---|---|---|
| `STORE_PATH` | unset | path to the SQLite file; setting it enables the store **and** pipe serving |
| `STATS_JWT_SECRET` | generated | must equal Ghost's `tinybird.adminToken` |
| `STATS_TOKEN` | generated | a static bearer, for curl and the tests |
| `LOG_PIPES` | on | `false` to stop logging pipe requests |

`SESSION_SECRET` must be set explicitly in a container without a volume:
otherwise one is generated at boot and every deploy resets visitor sessions.

## Wiring Ghost to the store

Point Ghost's stats endpoint here and let it mint JWTs as it normally would:

```
tinybird__workspaceId=<any stable string>
tinybird__adminToken=<the same value as STATS_JWT_SECRET>
tinybird__stats__endpoint=https://<domain>/_analytics
tinybird__tracker__endpoint=https://<domain>/_analytics/api/v1/page_hit
tinybird__tracker__datasource=analytics_events
```

**Do not use Ghost's `tinybird.stats.local` mode.** It looks like the natural
fit, but the Admin sends no credential for browser-side pipe calls in that mode,
so every chart reads zero. The JWT path above authenticates every call; the proxy
verifies the signature itself and honours the `site_uuid` pinned in the token's
scopes.

Note that the Admin issues those calls as **`POST`** with the JWT in a `?token=`
query parameter — not `GET` with an `Authorization` header. Tinybird accepts
both; so does this proxy.

## Tests

```bash
node tests/store-test.js
```

Covers the pipe semantics that are easy to get wrong: a session counts in the
bucket of its first pageview (with all of its pageviews), a `pathname` filter
switches `pageviews` to a per-hit count, per-post and gift-link totals ignore the
date range, and a day is 23 hours long across a spring-forward boundary.

## Importing history from Tinybird

Export the events for one site, then push them into the store:

```bash
curl -s -G https://api.<region>.tinybird.co/v0/sql \
  --data-urlencode "q=SELECT timestamp, session_id, action, version, payload \
      FROM analytics_events WHERE site_uuid='<uuid>' ORDER BY timestamp FORMAT JSONEachRow" \
  -H "Authorization: Bearer $TB_ADMIN_TOKEN" -o history.ndjson

STATS_JWT_SECRET=… node import-history.js history.ndjson https://<domain>/_analytics
```

`POST /v0/import` takes that NDJSON directly and is authenticated with
`STATS_JWT_SECRET`. Do **not** replay history through the tracker endpoint: that
path derives `session_id` from the caller's IP and user agent, so every imported
hit would collapse into one session and visit counts would be destroyed.

Re-running is safe. Each hit carries the tracker's `event_id`, which a partial
unique index makes idempotent — a failed import can simply be repeated, and a
duplicate already present in the source is collapsed rather than counted twice.
