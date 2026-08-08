#!/usr/bin/env node
'use strict';

/**
 * Minimal stand-in for Ghost's `analytics-service`, which is not shipped with
 * the Ghost release tarball (it only exists in the TryGhost/Ghost monorepo).
 *
 * The browser tracker (`ghost-stats.min.js`) posts `{timestamp, action, version,
 * payload}` with no `session_id`, but the `analytics_events` datasource declares
 * `session_id` as a non-nullable String, so Tinybird quarantines every raw hit.
 * This service sits between the two and adds what the pipes expect:
 *
 *   - `session_id`      top-level, consumed by mv_hits / filtered_sessions
 *   - `payload.device`  read by mv_hits and api_top_devices
 *   - `payload.referrerSource`         read by mv_hits for source attribution
 *   - `payload.meta.received_timestamp` used to compute ingestion_latency_ms
 *
 * It also keeps the Tinybird append token server-side, which is not optional in
 * production: ghost_head.js only emits `data-token` outside production, so a
 * live site has no way to authenticate to Tinybird from the browser at all.
 *
 * Session identity is derived, never stored: HMAC(daily salt, site|ip|user-agent).
 * The salt rotates at midnight UTC, so yesterday's hashes cannot be re-linked to
 * today's — the same approach Plausible uses. No cookie, no persistent visitor id.
 *
 * Configuration (all optional except the tracker token):
 *   PORT                 listen port                        (default 3000)
 *   BIND                 listen address                     (default 127.0.0.1)
 *   TB_TRACKER_TOKEN     Tinybird append token; falls back to ./.tb-tracker-token
 *   TB_EVENTS_HOST       Tinybird region API base
 *   TB_DATASOURCE        target datasource                  (default analytics_events)
 *   ALLOWED_ORIGIN       CORS origin, or '*' when same-origin behind a reverse proxy
 *   SESSION_SECRET       long-lived salt seed; falls back to ./.session-secret
 *   TRUSTED_PROXY_CIDRS  comma-separated; who may set X-Forwarded-For
 *   XFF_TRUST_HOPS       trusted hops between client and here (default 1)
 */

const http = require('http');
const https = require('https');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const net = require('net');

const PORT = Number(process.env.PORT || 3000);
const BIND = process.env.BIND || '127.0.0.1';
const TB_HOST = process.env.TB_EVENTS_HOST || 'https://api.europe-west2.gcp.tinybird.co';
const DATASOURCE = process.env.TB_DATASOURCE || 'analytics_events';
const ALLOWED_ORIGIN = process.env.ALLOWED_ORIGIN || 'http://localhost:2368';
const XFF_TRUST_HOPS = Math.max(1, Number(process.env.XFF_TRUST_HOPS || 1));
const TRUST_CF_HEADER = process.env.TRUST_CF_CONNECTING_IP !== 'false';

// Default to the private ranges a container reverse proxy lives in. Anything
// outside these may not dictate its own client IP.
const TRUSTED_PROXY_CIDRS = (process.env.TRUSTED_PROXY_CIDRS ||
    '127.0.0.0/8,::1/128,10.0.0.0/8,172.16.0.0/12,192.168.0.0/16').split(',').map(s => s.trim()).filter(Boolean);

function readSecret(envName, fileName, generate) {
    if (process.env[envName]) {
        return process.env[envName].trim();
    }
    const file = path.join(__dirname, fileName);
    if (fs.existsSync(file)) {
        return fs.readFileSync(file, 'utf8').trim();
    }
    if (!generate) {
        throw new Error(`Missing ${envName} and ${fileName}`);
    }
    // Regenerating on every boot would reset sessions on each deploy, so persist
    // it. In a container without a volume, set SESSION_SECRET instead.
    const value = crypto.randomBytes(32).toString('hex');
    fs.writeFileSync(file, value, {mode: 0o600});
    return value;
}

const TRACKER_TOKEN = readSecret('TB_TRACKER_TOKEN', '.tb-tracker-token', false);
const SESSION_SECRET = readSecret('SESSION_SECRET', '.session-secret', true);

function ipToBytes(ip) {
    if (net.isIPv4(ip)) {
        return ip.split('.').map(Number);
    }
    if (!net.isIPv6(ip)) {
        return null;
    }
    // ::ffff:1.2.3.4 — an IPv4 client arriving on a dual-stack socket.
    const mapped = /^::ffff:(\d+\.\d+\.\d+\.\d+)$/i.exec(ip);
    if (mapped) {
        return ipToBytes(mapped[1]);
    }
    const halves = ip.split('::');
    const head = halves[0] ? halves[0].split(':') : [];
    const tail = halves.length > 1 && halves[1] ? halves[1].split(':') : [];
    const groups = halves.length > 1
        ? head.concat(Array(8 - head.length - tail.length).fill('0'), tail)
        : head;
    if (groups.length !== 8) {
        return null;
    }
    const bytes = [];
    for (const g of groups) {
        const n = parseInt(g || '0', 16);
        bytes.push(n >> 8, n & 0xff);
    }
    return bytes;
}

function inCidr(ip, cidr) {
    const [range, bitsRaw] = cidr.split('/');
    const a = ipToBytes(ip);
    const b = ipToBytes(range);
    if (!a || !b || a.length !== b.length) {
        return false;
    }
    let bits = bitsRaw === undefined ? a.length * 8 : Number(bitsRaw);
    for (let i = 0; i < a.length && bits > 0; i++, bits -= 8) {
        const mask = bits >= 8 ? 0xff : (0xff << (8 - bits)) & 0xff;
        if ((a[i] & mask) !== (b[i] & mask)) {
            return false;
        }
    }
    return true;
}

const isTrustedProxy = ip => TRUSTED_PROXY_CIDRS.some(cidr => inCidr(ip, cidr));

/**
 * Only a trusted reverse proxy may speak for the client. Traefik/nginx append
 * the peer address to X-Forwarded-For, so the rightmost entries are the ones
 * they added; anything further left was supplied by the caller and is forgeable.
 */
function clientIp(req) {
    const peer = req.socket.remoteAddress || '0.0.0.0';
    if (!isTrustedProxy(peer)) {
        return peer;
    }
    // Cloudflare overwrites CF-Connecting-IP with the real client address and
    // drops any copy the caller sent, so it beats counting X-Forwarded-For hops.
    // Only honoured behind a trusted peer — and the origin should refuse traffic
    // that did not come through Cloudflare, or this header becomes forgeable.
    const cf = req.headers['cf-connecting-ip'];
    if (TRUST_CF_HEADER && typeof cf === 'string' && cf.trim()) {
        return cf.trim();
    }
    const chain = String(req.headers['x-forwarded-for'] || '').split(',').map(s => s.trim()).filter(Boolean);
    if (!chain.length) {
        return peer;
    }
    return chain[Math.max(0, chain.length - XFF_TRUST_HOPS)] || peer;
}

function sessionId(siteUuid, ip, userAgent) {
    const day = new Date().toISOString().slice(0, 10);
    const h = crypto.createHmac('sha256', `${SESSION_SECRET}:${day}`)
        .update(`${siteUuid}|${ip}|${userAgent}`)
        .digest('hex');
    // Shape it like a UUID — that is what the fixtures and mv pipes expect.
    return [h.slice(0, 8), h.slice(8, 12), h.slice(12, 16), h.slice(16, 20), h.slice(20, 32)].join('-');
}

function deviceFrom(userAgent) {
    const ua = (userAgent || '').toLowerCase();
    if (/ipad|tablet|playbook|silk|(android(?!.*mobile))/.test(ua)) {
        return 'tablet';
    }
    if (/mobi|iphone|ipod|android|blackberry|iemobile|opera mini/.test(ua)) {
        return 'mobile';
    }
    return 'desktop';
}

// mv_hits reads `referrerSource` and consolidates known hostnames (Facebook,
// Reddit, …) itself, so pass the raw hostname through rather than pre-mapping.
function referrerSourceFrom(payload) {
    const parsed = payload.parsedReferrer;
    if (!parsed) {
        return '';
    }
    if (parsed.source) {
        return String(parsed.source);
    }
    if (!parsed.url) {
        return '';
    }
    try {
        const host = new URL(parsed.url).hostname;
        // Self-referrals are internal navigation, not a traffic source.
        return host === new URL(payload.href || 'http://localhost').hostname ? '' : host;
    } catch (err) {
        return '';
    }
}

function postToTinybird(body) {
    return new Promise((resolve, reject) => {
        const url = new URL(`${TB_HOST}/v0/events`);
        url.searchParams.set('name', DATASOURCE);
        const data = Buffer.from(JSON.stringify(body));
        const req = https.request(url, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Content-Length': data.length,
                Authorization: `Bearer ${TRACKER_TOKEN}`
            },
            timeout: 10000
        }, (res) => {
            let out = '';
            res.on('data', chunk => (out += chunk));
            res.on('end', () => resolve({status: res.statusCode, body: out}));
        });
        req.on('error', reject);
        req.on('timeout', () => req.destroy(new Error('Tinybird request timed out')));
        req.end(data);
    });
}

function cors(res) {
    res.setHeader('Access-Control-Allow-Origin', ALLOWED_ORIGIN);
    res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type, x-site-uuid');
    res.setHeader('Access-Control-Max-Age', '86400');
}

const server = http.createServer((req, res) => {
    cors(res);

    if (req.method === 'OPTIONS') {
        res.writeHead(204).end();
        return;
    }
    if (req.url.startsWith('/health')) {
        res.writeHead(200, {'Content-Type': 'application/json'});
        res.end(JSON.stringify({ok: true, datasource: DATASOURCE, host: TB_HOST}));
        return;
    }
    if (req.method !== 'POST' || !req.url.startsWith('/api/v1/page_hit')) {
        res.writeHead(404).end();
        return;
    }

    let raw = '';
    req.on('data', (chunk) => {
        raw += chunk;
        if (raw.length > 64 * 1024) {
            req.destroy();
        }
    });

    req.on('end', async () => {
        let event;
        try {
            event = JSON.parse(raw);
        } catch (err) {
            res.writeHead(400).end('bad json');
            return;
        }

        const payload = event.payload && typeof event.payload === 'object' ? event.payload : {};
        const ua = payload['user-agent'] || req.headers['user-agent'] || '';
        const siteUuid = payload.site_uuid || req.headers['x-site-uuid'] || '';

        const enriched = {
            timestamp: event.timestamp || new Date().toISOString(),
            session_id: sessionId(siteUuid, clientIp(req), ua),
            action: event.action || 'page_hit',
            version: String(event.version || '1'),
            payload: Object.assign({}, payload, {
                device: payload.device || deviceFrom(ua),
                referrerSource: referrerSourceFrom(payload),
                meta: Object.assign({}, payload.meta, {
                    received_timestamp: new Date().toISOString(),
                    referrerSource: referrerSourceFrom(payload)
                })
            })
        };

        // Answer the browser immediately; a failed relay must not stall the page.
        res.writeHead(202, {'Content-Type': 'application/json'});
        res.end(JSON.stringify({ok: true}));

        try {
            const result = await postToTinybird(enriched);
            if (result.status >= 300) {
                console.error(`[relay] Tinybird ${result.status}: ${result.body.slice(0, 300)}`);
            } else {
                console.log(`[relay] ${enriched.action} ${payload.pathname || ''} -> ${result.status}`);
            }
        } catch (err) {
            console.error(`[relay] failed: ${err.message}`);
        }
    });
});

server.listen(PORT, BIND, () => {
    console.log(`analytics-proxy listening on http://${BIND}:${PORT}`);
    console.log(`  relaying to ${TB_HOST}/v0/events?name=${DATASOURCE}`);
    console.log(`  accepting origin ${ALLOWED_ORIGIN}`);
    console.log(`  trusting X-Forwarded-For from ${TRUSTED_PROXY_CIDRS.join(', ')} (${XFF_TRUST_HOPS} hop)`);
});

// Coolify/Docker stop the container with SIGTERM; drain instead of dropping hits.
for (const signal of ['SIGTERM', 'SIGINT']) {
    process.on(signal, () => server.close(() => process.exit(0)));
}
