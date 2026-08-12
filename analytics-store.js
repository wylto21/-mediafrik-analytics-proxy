'use strict';

/**
 * Local analytics store — the half of Tinybird that Ghost's Analytics tab
 * actually needs, backed by SQLite.
 *
 * Ghost talks to Tinybird over exactly two surfaces: it appends events to a
 * datasource, and it reads a fixed list of pipes at `/v0/pipes/<name>.json`.
 * Nothing else about Tinybird is load-bearing, so a service that ingests the
 * same events and answers the same pipe URLs replaces it outright — and Ghost
 * has a first-class switch for pointing at one (`tinybird.stats.local`).
 *
 * SQLite rather than MySQL or ClickHouse, for three reasons that all reduce to
 * "fewer moving parts": `node:sqlite` ships with Node 22, so the proxy keeps its
 * zero-dependency property; the proxy is the only writer, which is the one
 * workload SQLite is unambiguously good at; and dev and production run the exact
 * same engine against one file that backs up by being copied.
 *
 * The read path stays cheap because the *write* path does the work. Tinybird
 * computes `_mv_hits` (JSON extraction, referrer consolidation, UA parsing) in a
 * materialized view; here the proxy already holds the parsed payload when the
 * hit arrives, so `hits` is stored pre-flattened and the pipes become plain
 * aggregates over indexed columns.
 *
 * Sessions are NOT stored. Tinybird keeps a `mv_session_data` rollup, but a
 * derived table that can drift from its source is a bug generator, and at blog
 * volume the GROUP BY costs nothing. Sessions are recomputed per query, which
 * means they are always consistent with the hits by construction.
 */

const {DatabaseSync} = require('node:sqlite');

const SCHEMA = `
CREATE TABLE IF NOT EXISTS hits (
    id            INTEGER PRIMARY KEY,
    site_uuid     TEXT    NOT NULL,
    timestamp     INTEGER NOT NULL,
    session_id    TEXT    NOT NULL,
    action        TEXT    NOT NULL DEFAULT 'page_hit',
    version       TEXT    NOT NULL DEFAULT '1',
    member_uuid   TEXT    NOT NULL DEFAULT '',
    member_status TEXT    NOT NULL DEFAULT '',
    post_uuid     TEXT    NOT NULL DEFAULT '',
    post_type     TEXT    NOT NULL DEFAULT '',
    gift_link     TEXT    NOT NULL DEFAULT '',
    location      TEXT    NOT NULL DEFAULT '',
    source        TEXT    NOT NULL DEFAULT '',
    pathname      TEXT    NOT NULL DEFAULT '',
    href          TEXT    NOT NULL DEFAULT '',
    device        TEXT    NOT NULL DEFAULT 'unknown',
    os            TEXT    NOT NULL DEFAULT 'Unknown',
    browser       TEXT    NOT NULL DEFAULT 'Unknown',
    utm_source    TEXT    NOT NULL DEFAULT '',
    utm_medium    TEXT    NOT NULL DEFAULT '',
    utm_campaign  TEXT    NOT NULL DEFAULT '',
    utm_term      TEXT    NOT NULL DEFAULT '',
    utm_content   TEXT    NOT NULL DEFAULT ''
);
-- Every pipe filters on (site_uuid, timestamp) first; the session index serves
-- the group-by that rebuilds sessions on read.
CREATE INDEX IF NOT EXISTS hits_site_ts  ON hits (site_uuid, timestamp);
CREATE INDEX IF NOT EXISTS hits_session  ON hits (site_uuid, session_id);
`;

// Ported from mv_hits.pipe. Tinybird consolidates these host-level referrers
// into one display name so the Sources table does not split "Facebook" across
// five hostnames; the mapping has to live somewhere, and doing it at ingest
// keeps the read queries free of a 40-branch CASE.
const SOURCE_ALIASES = new Map(Object.entries({
    'facebook': 'Facebook',
    'www.facebook.com': 'Facebook',
    'l.facebook.com': 'Facebook',
    'lm.facebook.com': 'Facebook',
    'm.facebook.com': 'Facebook',
    'twitter': 'Twitter',
    'x.com': 'Twitter',
    'com.twitter.android': 'Twitter',
    'go.bsky.app': 'Bluesky',
    'bsky': 'Bluesky',
    'bsky.app': 'Bluesky',
    'instagram': 'Instagram',
    'www.instagram.com': 'Instagram',
    'linkedin': 'LinkedIn',
    'linkedin_company': 'LinkedIn',
    'l.threads.com': 'Threads',
    'www.reddit.com': 'Reddit',
    'out.reddit.com': 'Reddit',
    'old.reddit.com': 'Reddit',
    'com.reddit.frontpage': 'Reddit',
    'search.brave.com': 'Brave Search',
    'www.ecosia.org': 'Ecosia',
    'gmail': 'Gmail',
    'com.google.android.gm': 'Gmail',
    'mail.google.com': 'Gmail',
    'outlook.com': 'Outlook',
    'yahoo!': 'Yahoo!',
    'www.yahoo.com': 'Yahoo!',
    'yahoo! mail': 'Yahoo!',
    'r.search.yahoo.com': 'Yahoo!',
    'aol mail': 'AOL Mail',
    'flipboard': 'Flipboard',
    'flipboard.com': 'Flipboard',
    'flipboard.app': 'Flipboard',
    'substack': 'Substack',
    'substack.com': 'Substack',
    'ghost.org': 'Ghost',
    'buffer': 'Buffer',
    'taboola': 'Taboola',
    'appnexus': 'AppNexus',
    'en.wikipedia.org': 'Wikipedia',
    'en.m.wikipedia.org': 'Wikipedia',
    'mastodon.social': 'Mastodon',
    'mastodon.online': 'Mastodon',
    'org.joinmastodon.android': 'Mastodon',
    'phanpy.social': 'Mastodon',
    'dev.phanpy.social': 'Mastodon',
    'www.memeorandum.com': 'Memeorandum',
    'memeorandum.com': 'Memeorandum',
    'ground.news': 'Ground News',
    'apple.news': 'Apple News',
    'www.smartnews.com': 'SmartNews'
}));

function normaliseSource(referrer) {
    const raw = String(referrer || '').trim();
    if (!raw) {
        return '';
    }
    const alias = SOURCE_ALIASES.get(raw.toLowerCase());
    if (alias) {
        return alias;
    }
    // domainWithoutWWW: only strips the prefix, and only when this looks like a
    // hostname at all — named sources ("Gmail") must survive untouched.
    return raw.replace(/^www\./i, '');
}

function osFrom(userAgent) {
    const ua = String(userAgent || '').toLowerCase();
    if (/windows/.test(ua)) {
        return 'windows';
    }
    if (/mac/.test(ua)) {
        return 'macos';
    }
    if (/linux/.test(ua)) {
        return 'linux';
    }
    if (/android/.test(ua)) {
        return 'android';
    }
    if (/iphone|ipad|ipod/.test(ua)) {
        return 'ios';
    }
    return 'Unknown';
}

function browserFrom(userAgent) {
    const ua = String(userAgent || '').toLowerCase();
    if (/firefox/.test(ua)) {
        return 'firefox';
    }
    if (/chrome|crios/.test(ua)) {
        return 'chrome';
    }
    if (/opera/.test(ua)) {
        return 'opera';
    }
    if (/msie|trident/.test(ua)) {
        return 'ie';
    }
    if (/iphone|ipad|safari/.test(ua)) {
        return 'safari';
    }
    return 'Unknown';
}

const str = value => (value === undefined || value === null ? '' : String(value));

/* ------------------------------------------------------------------ *
 * Time bucketing
 *
 * ClickHouse does `toDate(toTimezone(timestamp, tz))` natively. SQLite only
 * knows UTC and the server's own local time, so the buckets are computed here
 * with Intl and handed to SQL as explicit UTC ranges. That is not a workaround
 * for a missing feature — it is the only way to stay correct across a DST
 * boundary, where a "day" is 23 or 25 hours long and a fixed offset is wrong
 * for part of the range.
 * ------------------------------------------------------------------ */

const TZ_PARTS = new Map();

function offsetSeconds(timezone, instantMs) {
    let dtf = TZ_PARTS.get(timezone);
    if (!dtf) {
        dtf = new Intl.DateTimeFormat('en-US', {
            timeZone: timezone,
            hour12: false,
            year: 'numeric',
            month: '2-digit',
            day: '2-digit',
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit'
        });
        TZ_PARTS.set(timezone, dtf);
    }
    const parts = {};
    for (const {type, value} of dtf.formatToParts(new Date(instantMs))) {
        parts[type] = value;
    }
    // hour12:false still yields "24" for midnight in some ICU versions.
    const hour = Number(parts.hour) % 24;
    const asUtc = Date.UTC(Number(parts.year), Number(parts.month) - 1, Number(parts.day), hour, Number(parts.minute), Number(parts.second));
    return (asUtc - instantMs) / 1000;
}

// The UTC instant at which the given calendar day starts in `timezone`.
function startOfDayUtc(timezone, y, m, d) {
    const naive = Date.UTC(y, m - 1, d);
    // One correction lands us in the right day; a second settles the case where
    // the first guess fell on the other side of a DST transition.
    let ts = naive - offsetSeconds(timezone, naive) * 1000;
    ts = naive - offsetSeconds(timezone, ts) * 1000;
    return ts;
}

function parseDate(value) {
    const m = /^(\d{4})-(\d{2})-(\d{2})/.exec(String(value || ''));
    if (!m) {
        return null;
    }
    return {y: Number(m[1]), m: Number(m[2]), d: Number(m[3])};
}

function pad(n) {
    return String(n).padStart(2, '0');
}

/**
 * Builds the contiguous, gap-free series the KPI chart plots against. Tinybird
 * generates it with `arrayJoin(range(...))` and left-joins the data onto it;
 * the same trick is what stops a quiet day from vanishing from the chart
 * instead of showing zero.
 *
 * Returns `{label, start, end}` with UTC second bounds, hourly when the range
 * is a single day (which is what the Admin's "Today" view asks for).
 */
function buildBuckets(dateFrom, dateTo, timezone, nowMs) {
    const from = parseDate(dateFrom);
    const to = parseDate(dateTo);
    const buckets = [];

    if (!from || !to) {
        // Same default as filtered_sessions.pipe: the last 7 days.
        const todayUtc = new Date(nowMs);
        const end = {y: todayUtc.getUTCFullYear(), m: todayUtc.getUTCMonth() + 1, d: todayUtc.getUTCDate()};
        const startMs = startOfDayUtc(timezone, end.y, end.m, end.d) - 7 * 86400000;
        const start = new Date(startMs);
        return buildBuckets(
            `${start.getUTCFullYear()}-${pad(start.getUTCMonth() + 1)}-${pad(start.getUTCDate())}`,
            `${end.y}-${pad(end.m)}-${pad(end.d)}`,
            timezone,
            nowMs
        );
    }

    const startMs = startOfDayUtc(timezone, from.y, from.m, from.d);
    const endMs = startOfDayUtc(timezone, to.y, to.m, to.d);

    if (startMs === endMs) {
        // Single day: hourly buckets, stopping at the current hour so the chart
        // does not trail a row of zeros for hours that have not happened yet.
        const dayEnd = startOfDayUtc(timezone, to.y, to.m, to.d + 1);
        const limit = Math.min(dayEnd, Math.max(startMs + 3600000, Math.ceil(nowMs / 3600000) * 3600000));
        for (let t = startMs; t < limit; t += 3600000) {
            const off = offsetSeconds(timezone, t) * 1000;
            const local = new Date(t + off);
            buckets.push({
                label: `${local.getUTCFullYear()}-${pad(local.getUTCMonth() + 1)}-${pad(local.getUTCDate())} ${pad(local.getUTCHours())}:00:00`,
                start: Math.floor(t / 1000),
                end: Math.floor((t + 3600000) / 1000)
            });
        }
        return buckets;
    }

    for (let cursor = {...from}; ;) {
        const dayStart = startOfDayUtc(timezone, cursor.y, cursor.m, cursor.d);
        if (dayStart > endMs) {
            break;
        }
        const next = new Date(Date.UTC(cursor.y, cursor.m - 1, cursor.d + 1));
        const dayEnd = startOfDayUtc(timezone, next.getUTCFullYear(), next.getUTCMonth() + 1, next.getUTCDate());
        buckets.push({
            label: `${cursor.y}-${pad(cursor.m)}-${pad(cursor.d)}`,
            start: Math.floor(dayStart / 1000),
            end: Math.floor(dayEnd / 1000)
        });
        cursor = {y: next.getUTCFullYear(), m: next.getUTCMonth() + 1, d: next.getUTCDate()};
    }
    return buckets;
}

/**
 * The half-open UTC window a pipe reads, in seconds.
 *
 * Each bound defaults independently, which is how the Tinybird pipes are
 * written: a missing `date_from` means "seven days ago", a missing `date_to`
 * means "through today". With `defaults: false` a missing bound stays open —
 * gift links and per-post totals are lifetime figures, and silently clamping
 * them to a week would quietly under-report.
 */
function dayWindow(params, timezone, nowMs, {defaults = true} = {}) {
    const from = parseDate(params.date_from);
    const to = parseDate(params.date_to);
    let start = null;
    let end = null;

    if (from) {
        start = startOfDayUtc(timezone, from.y, from.m, from.d) / 1000;
    }
    if (to) {
        const next = new Date(Date.UTC(to.y, to.m - 1, to.d + 1));
        end = startOfDayUtc(timezone, next.getUTCFullYear(), next.getUTCMonth() + 1, next.getUTCDate()) / 1000;
    }
    if (!defaults) {
        return {start, end};
    }

    const today = new Date(nowMs);
    const [y, m, d] = [today.getUTCFullYear(), today.getUTCMonth() + 1, today.getUTCDate()];
    if (start === null) {
        start = (startOfDayUtc(timezone, y, m, d) - 7 * 86400000) / 1000;
    }
    if (end === null) {
        const next = new Date(Date.UTC(y, m - 1, d + 1));
        end = startOfDayUtc(timezone, next.getUTCFullYear(), next.getUTCMonth() + 1, next.getUTCDate()) / 1000;
    }
    return {start, end};
}

/* ------------------------------------------------------------------ *
 * Filters
 * ------------------------------------------------------------------ */

// Hit-level filters vary within a session, so a session qualifies when ANY of
// its hits match — that is what filtered_sessions.pipe means by "filtered".
const HIT_FILTERS = ['location', 'pathname', 'post_uuid'];
// Session-level filters describe the session's first hit only.
const SESSION_FILTERS = ['source', 'device', 'utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content'];

function memberStatusClause(value, out) {
    const list = String(value).split(',').map(s => s.trim().replace(/^'|'$/g, '')).filter(Boolean);
    if (!list.length) {
        return '';
    }
    // 'paid' covers comped and gift subscriptions, per filtered_sessions.pipe.
    const expanded = list.includes('paid') ? list.concat(['comped', 'gift']) : list;
    out.push(...expanded);
    return ` AND member_status IN (${expanded.map(() => '?').join(',')})`;
}

// Hit-level WHERE fragment, shared by every pipe.
function hitWhere(params, values) {
    let sql = ' AND action = \'page_hit\'';
    for (const key of HIT_FILTERS) {
        if (params[key] !== undefined) {
            sql += ` AND ${key} = ?`;
            values.push(str(params[key]));
        }
    }
    if (params.member_status !== undefined) {
        sql += memberStatusClause(params.member_status, values);
    }
    if (params.gift_link !== undefined) {
        sql += (params.gift_link === 'false' || params.gift_link === '0')
            ? ' AND gift_link = \'\''
            : ' AND gift_link != \'\'';
    }
    if (params.post_type !== undefined) {
        sql += params.post_type === 'post' ? ' AND post_type = \'post\'' : ' AND post_type != \'post\'';
    }
    return sql;
}

// Appends the half-open time bounds. Either end may be absent: the all-time
// pipes (gift links, per-post totals) deliberately have no default window.
function rangeClause(window, values, column = 'timestamp') {
    let sql = '';
    if (window && window.start !== null) {
        sql += ` AND ${column} >= ?`;
        values.push(window.start);
    }
    if (window && window.end !== null) {
        sql += ` AND ${column} < ?`;
        values.push(window.end);
    }
    return sql;
}

/**
 * The set of sessions a query is allowed to see: sessions with at least one
 * qualifying hit in the window, then narrowed by first-hit attributes.
 *
 * Returned as a CTE plus its bound values. Every pipe references it, exactly as
 * the Tinybird pipes reference `filtered_sessions`.
 *
 * `withAttributes` forces the first-hit projection even when nothing filters on
 * it — the "top sources / devices / UTM" pipes group BY those attributes, so
 * they need the projection whether or not it also narrows anything.
 */
function sessionScopeCte(params, window, values, {withAttributes = false} = {}) {
    // Order matters: these push into one bound-value list, so they must be
    // built in the same order their placeholders appear in the SQL below.
    const hitValues = [];
    const rangeSql = rangeClause(window, hitValues);
    const hitSql = hitWhere(params, hitValues);

    let cte = `
    scoped_hits AS (
        SELECT * FROM hits
        WHERE site_uuid = ?${rangeSql}${hitSql}
    ),
    filtered_sessions AS (
        SELECT DISTINCT session_id FROM scoped_hits
    )`;
    values.push(params.site_uuid, ...hitValues);

    const sessionFilters = SESSION_FILTERS.filter(key => params[key] !== undefined);
    if (!sessionFilters.length && !withAttributes) {
        return cte;
    }

    // Session attributes come from the first hit, so they need the unfiltered
    // hit stream — a session whose first hit was on another page still counts.
    const attrValues = [params.site_uuid];
    const attrRange = rangeClause(window, attrValues);
    cte += `,
    session_first_hit AS (
        SELECT session_id, ${SESSION_FILTERS.join(', ')}
        FROM (
            SELECT h.*, ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY timestamp, id) AS rn
            FROM hits h
            WHERE site_uuid = ?${attrRange} AND action = 'page_hit'
        ) WHERE rn = 1
    )`;
    values.push(...attrValues);

    if (sessionFilters.length) {
        cte += `,
    session_scope AS (
        SELECT fs.session_id FROM filtered_sessions fs
        JOIN session_first_hit sfh ON sfh.session_id = fs.session_id
        WHERE ${sessionFilters.map(key => `sfh.${key} = ?`).join(' AND ')}
    )`;
        values.push(...sessionFilters.map(key => str(params[key])));
    }
    return cte;
}

function scopeName(params) {
    return SESSION_FILTERS.some(key => params[key] !== undefined) ? 'session_scope' : 'filtered_sessions';
}

const truncate2 = n => Math.trunc((Number(n) || 0) * 100) / 100;

// Tinybird writes `limit {{skip}},{{limit}}` — MySQL-style offset-first — which
// reads as the opposite of SQLite's LIMIT/OFFSET. Same defaults as the pipes.
function paging(params) {
    const limit = Number.isFinite(Number(params.limit)) ? Math.max(0, Number(params.limit)) : 50;
    const skip = Number.isFinite(Number(params.skip)) ? Math.max(0, Number(params.skip)) : 0;
    return {limit, skip};
}

// ClickHouse renders DateTime as 'YYYY-MM-DD HH:MM:SS' in UTC, with no zone
// suffix; a client parsing an ISO string instead would read it as local time.
function formatDateTime(seconds) {
    if (!seconds) {
        return null;
    }
    return new Date(seconds * 1000).toISOString().replace('T', ' ').slice(0, 19);
}

/* ------------------------------------------------------------------ *
 * Pipes
 * ------------------------------------------------------------------ */

const PIPES = {
    /**
     * api_kpis — the headline series: visits, pageviews, bounce rate and mean
     * session length per bucket.
     *
     * Faithful to api_kpis.pipe in two subtleties worth stating, because both
     * look like bugs otherwise. A session is counted in the bucket of its FIRST
     * pageview, and it contributes ALL of its pageviews there, even the ones
     * that happened after midnight. And when a pathname/post/gift-link filter is
     * active, `pageviews` switches meaning entirely: it stops being a session
     * total and becomes a count of matching hits, bucketed by the hit's own
     * timestamp — because "views of this post per day" is the question the
     * post-level view is actually asking.
     */
    api_kpis(db, params, ctx) {
        const buckets = ctx.buckets();
        if (!buckets.length) {
            return [];
        }
        const windowStart = buckets[0].start;
        const windowEnd = buckets[buckets.length - 1].end;
        const window = {start: windowStart, end: windowEnd};
        const values = [];
        const scope = sessionScopeCte(params, window, values);
        const scoped = scopeName(params);

        const bucketRows = buckets.map(() => '(?,?,?)').join(',');
        const bucketValues = buckets.flatMap(b => [b.label, b.start, b.end]);

        const perHitPageviews = params.pathname !== undefined || params.post_uuid !== undefined || params.gift_link !== undefined;

        let sql = `WITH ${scope},
    sessions AS (
        SELECT h.session_id,
               COUNT(*) AS pageviews,
               MIN(h.timestamp) AS first_pageview,
               MAX(h.timestamp) - MIN(h.timestamp) AS duration,
               CASE WHEN COUNT(*) = 1 THEN 1 ELSE 0 END AS is_bounce
        FROM hits h
        JOIN ${scoped} fs ON fs.session_id = h.session_id
        WHERE h.site_uuid = ? AND h.timestamp >= ? AND h.timestamp < ? AND h.action = 'page_hit'
        GROUP BY h.session_id
    ),
    buckets(label, start_ts, end_ts) AS (VALUES ${bucketRows})
    SELECT b.label AS date,
           COUNT(s.session_id) AS visits,
           COALESCE(SUM(s.pageviews), 0) AS pageviews,
           COALESCE(AVG(s.is_bounce), 0) AS bounce_rate,
           COALESCE(AVG(s.duration), 0) AS avg_session_sec
    FROM buckets b
    LEFT JOIN sessions s ON s.first_pageview >= b.start_ts AND s.first_pageview < b.end_ts
    GROUP BY b.label ORDER BY b.label`;

        values.push(params.site_uuid, windowStart, windowEnd, ...bucketValues);
        const rows = db.prepare(sql).all(...values);

        let hitCounts = null;
        if (perHitPageviews) {
            const hitValues = [];
            const hitScope = sessionScopeCte(params, window, hitValues);
            hitValues.push(...bucketValues);
            hitCounts = new Map(db.prepare(`WITH ${hitScope},
    buckets(label, start_ts, end_ts) AS (VALUES ${bucketRows})
    SELECT b.label AS date, COUNT(h.id) AS pageviews
    FROM buckets b
    LEFT JOIN scoped_hits h ON h.timestamp >= b.start_ts AND h.timestamp < b.end_ts
        AND h.session_id IN (SELECT session_id FROM ${scopeName(params)})
    GROUP BY b.label`).all(...hitValues).map(r => [r.date, r.pageviews]));
        }

        return rows.map(row => ({
            date: row.date,
            visits: Number(row.visits),
            pageviews: hitCounts ? Number(hitCounts.get(row.date) || 0) : Number(row.pageviews),
            bounce_rate: truncate2(row.bounce_rate),
            avg_session_sec: truncate2(row.avg_session_sec)
        }));
    },

    /**
     * api_top_pages — visits per pathname, which is also what drives the
     * per-post view counts in the post list.
     */
    api_top_pages(db, params, ctx) {
        const values = [];
        const scope = sessionScopeCte(params, ctx.window(), values);
        const {limit, skip} = paging(params);

        const rows = db.prepare(`WITH ${scope}
    SELECT CASE WHEN h.post_uuid = 'undefined' THEN '' ELSE h.post_uuid END AS post_uuid,
           h.pathname AS pathname,
           COUNT(DISTINCT h.session_id) AS visits
    FROM scoped_hits h
    JOIN ${scopeName(params)} fs ON fs.session_id = h.session_id
    GROUP BY post_uuid, h.pathname
    ORDER BY visits DESC, h.pathname
    LIMIT ? OFFSET ?`).all(...values, limit, skip);

        return rows.map(row => ({
            post_uuid: row.post_uuid,
            pathname: row.pathname,
            visits: Number(row.visits)
        }));
    },

    /**
     * api_top_locations — visitors per country. Hit-level, not session-level:
     * `location` can differ between two hits of the same session (a phone
     * changing network), and the pipe counts distinct sessions per country
     * rather than picking one country per session.
     */
    api_top_locations(db, params, ctx) {
        const values = [];
        const scope = sessionScopeCte(params, ctx.window(), values);
        const {limit, skip} = paging(params);

        const rows = db.prepare(`WITH ${scope}
    SELECT h.location AS location, COUNT(DISTINCT h.session_id) AS visits
    FROM scoped_hits h
    JOIN ${scopeName(params)} fs ON fs.session_id = h.session_id
    GROUP BY h.location
    ORDER BY visits DESC, h.location
    LIMIT ? OFFSET ?`).all(...values, limit, skip);

        return rows.map(row => ({location: row.location, visits: Number(row.visits)}));
    },

    /**
     * api_active_visitors — the "N reading now" counter. Sessions seen in the
     * last five minutes, ignoring the dashboard's date range entirely.
     *
     * This is the pipe that ate 40% of the Tinybird quota, because the Admin
     * polls it once a minute for as long as the tab is open. Here it is a
     * single indexed range scan against a local file, so the polling costs
     * nothing worth measuring.
     */
    api_active_visitors(db, params) {
        const values = [params.site_uuid, Math.floor(Date.now() / 1000) - 300];
        let sql = `SELECT COUNT(DISTINCT session_id) AS active_visitors FROM hits
    WHERE site_uuid = ? AND timestamp >= ? AND action = 'page_hit'`;

        if (params.post_uuid !== undefined) {
            sql += ' AND post_uuid = ?';
            values.push(str(params.post_uuid));
        }
        if (params.gift_link !== undefined) {
            sql += (params.gift_link === 'false' || params.gift_link === '0')
                ? ' AND gift_link = \'\'' : ' AND gift_link != \'\'';
        }

        const row = db.prepare(sql).get(...values);
        return [{active_visitors: Number(row.active_visitors || 0)}];
    },

    /**
     * api_post_visitor_counts — lifetime visitor totals for a named set of
     * posts, used by the post list. Deliberately unbounded in time: this is the
     * "how many people ever read this" number, not a windowed one.
     */
    api_post_visitor_counts(db, params) {
        const uuids = str(params.post_uuids).split(',').map(s => s.trim()).filter(Boolean);
        if (!uuids.length) {
            return [];
        }
        const rows = db.prepare(`SELECT post_uuid, COUNT(DISTINCT session_id) AS visits
    FROM hits
    WHERE site_uuid = ? AND action = 'page_hit' AND post_uuid IN (${uuids.map(() => '?').join(',')})
    GROUP BY post_uuid
    ORDER BY visits DESC`).all(params.site_uuid, ...uuids);

        return rows.map(row => ({post_uuid: row.post_uuid, visits: Number(row.visits)}));
    },

    /**
     * api_gift_link_visits — usage per individual gift link.
     *
     * Note the reversal that the upstream pipe calls out: everywhere else
     * `gift_link` is a yes/no segment (was this a gift read at all), but here it
     * is an exact match on the link's token, so the Admin card can ask about one
     * link instead of fetching them all. That is why this pipe builds its own
     * WHERE clause rather than reusing the shared hit filters.
     */
    api_gift_link_visits(db, params, ctx) {
        const values = [params.site_uuid];
        // All-time by default — a gift link's usage is its lifetime total.
        let sql = `SELECT gift_link,
           COUNT(DISTINCT session_id) AS visits,
           COUNT(*) AS views,
           MAX(timestamp) AS last_seen
    FROM hits
    WHERE site_uuid = ? AND action = 'page_hit' AND gift_link != ''`;
        sql += rangeClause(ctx.window({defaults: false}), values);

        if (params.post_uuid !== undefined) {
            sql += ' AND post_uuid = ?';
            values.push(str(params.post_uuid));
        }
        if (params.gift_link !== undefined) {
            sql += ' AND gift_link = ?';
            values.push(str(params.gift_link));
        }
        sql += ' GROUP BY gift_link ORDER BY visits DESC, gift_link';

        return db.prepare(sql).all(...values).map(row => ({
            gift_link: row.gift_link,
            visits: Number(row.visits),
            views: Number(row.views),
            last_seen: formatDateTime(row.last_seen)
        }));
    }
};

/**
 * The seven "top <attribute>" pipes are one query with the column swapped, so
 * they are generated rather than copied. Each counts SESSIONS, not hits, and
 * reads the attribute from the session's first hit — a visitor who arrives from
 * Reddit and then clicks an internal link is one Reddit visit, not two.
 *
 * The UTM ones additionally drop empty values: a table of campaigns should not
 * be topped by a blank row counting everyone who arrived without a campaign.
 */
const META = {
    api_kpis: [
        {name: 'date', type: 'Date'},
        {name: 'visits', type: 'UInt64'},
        {name: 'pageviews', type: 'UInt64'},
        {name: 'bounce_rate', type: 'Float64'},
        {name: 'avg_session_sec', type: 'Float64'}
    ],
    api_top_pages: [
        {name: 'post_uuid', type: 'String'},
        {name: 'pathname', type: 'String'},
        {name: 'visits', type: 'UInt64'}
    ],
    api_top_locations: [
        {name: 'location', type: 'String'},
        {name: 'visits', type: 'UInt64'}
    ],
    api_active_visitors: [
        {name: 'active_visitors', type: 'UInt64'}
    ],
    api_post_visitor_counts: [
        {name: 'post_uuid', type: 'String'},
        {name: 'visits', type: 'UInt64'}
    ],
    api_gift_link_visits: [
        {name: 'gift_link', type: 'String'},
        {name: 'visits', type: 'UInt64'},
        {name: 'views', type: 'UInt64'},
        {name: 'last_seen', type: 'DateTime'}
    ]
};

for (const [pipe, column, skipEmpty] of [
    ['api_top_sources', 'source', false],
    ['api_top_devices', 'device', false],
    ['api_top_utm_sources', 'utm_source', true],
    ['api_top_utm_mediums', 'utm_medium', true],
    ['api_top_utm_campaigns', 'utm_campaign', true],
    ['api_top_utm_terms', 'utm_term', true],
    ['api_top_utm_contents', 'utm_content', true]
]) {
    PIPES[pipe] = (db, params, ctx) => {
        const values = [];
        const scope = sessionScopeCte(params, ctx.window(), values, {withAttributes: true});
        const {limit, skip} = paging(params);

        const rows = db.prepare(`WITH ${scope}
    SELECT sfh.${column} AS ${column}, COUNT(*) AS visits
    FROM ${scopeName(params)} fs
    JOIN session_first_hit sfh ON sfh.session_id = fs.session_id
    ${skipEmpty ? `WHERE sfh.${column} != ''` : ''}
    GROUP BY sfh.${column}
    ORDER BY visits DESC, sfh.${column}
    LIMIT ? OFFSET ?`).all(...values, limit, skip);

        return rows.map(row => ({[column]: row[column], visits: Number(row.visits)}));
    };
    META[pipe] = [{name: column, type: 'String'}, {name: 'visits', type: 'UInt64'}];
}

// Ghost may request a versioned pipe name (api_kpis_v2) depending on
// `tinybird.stats.version`. The versions differ in how Tinybird materializes
// them upstream, not in what they return, so one implementation answers both.
function resolvePipe(name) {
    const base = String(name).replace(/_v\d+$/, '');
    return PIPES[base] ? {handler: PIPES[base], base} : null;
}

function openStore(filePath) {
    const db = new DatabaseSync(filePath);
    // WAL lets the Admin's reads run while a page view is being written; without
    // it a concurrent reader and writer contend for the same lock.
    db.exec('PRAGMA journal_mode = WAL');
    db.exec('PRAGMA synchronous = NORMAL');
    db.exec('PRAGMA busy_timeout = 5000');
    db.exec(SCHEMA);

    const insert = db.prepare(`INSERT INTO hits (
        site_uuid, timestamp, session_id, action, version,
        member_uuid, member_status, post_uuid, post_type, gift_link,
        location, source, pathname, href, device, os, browser,
        utm_source, utm_medium, utm_campaign, utm_term, utm_content
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`);

    return {
        /**
         * Flattens one enriched event into a row. This is the `mv_hits`
         * transformation, done once at write time instead of on every read.
         */
        record(event) {
            const payload = event.payload && typeof event.payload === 'object' ? event.payload : {};
            const ua = payload['user-agent'] || '';
            const seconds = Math.floor(new Date(event.timestamp).getTime() / 1000);
            if (!Number.isFinite(seconds)) {
                return;
            }
            // mv_hits falls back to payload.meta.referrerSource when the
            // top-level one is empty; the proxy sets both, but a hit relayed
            // from elsewhere may only carry the nested copy.
            const referrer = str(payload.referrerSource) || str(payload.meta && payload.meta.referrerSource);
            insert.run(
                str(payload.site_uuid),
                seconds,
                str(event.session_id) || '0',
                str(event.action) || 'page_hit',
                str(event.version) || '1',
                str(payload.member_uuid),
                str(payload.member_status),
                str(payload.post_uuid),
                str(payload.post_type),
                str(payload.gift_link),
                str(payload.location),
                normaliseSource(referrer),
                str(payload.pathname),
                str(payload.href),
                str(payload.device) || 'unknown',
                osFrom(ua),
                browserFrom(ua),
                str(payload.utm_source),
                str(payload.utm_medium),
                str(payload.utm_campaign),
                str(payload.utm_term),
                str(payload.utm_content)
            );
        },

        /**
         * Answers a pipe in Tinybird's response shape. Ghost reads `.data` and
         * ignores the rest, but the envelope is what makes this a drop-in: a
         * client that checks `rows` or `meta` keeps working.
         */
        query(pipeName, params) {
            const resolved = resolvePipe(pipeName);
            if (!resolved) {
                return null;
            }
            const started = process.hrtime.bigint();
            const timezone = params.timezone || 'Etc/UTC';
            const now = Date.now();
            // Lazily, because the pipes disagree about time: most want the
            // default seven-day window, the KPI chart wants it split into
            // buckets, and the lifetime pipes want no window at all.
            const ctx = {
                timezone,
                buckets: () => buildBuckets(params.date_from, params.date_to, timezone, now),
                window: opts => dayWindow(params, timezone, now, opts)
            };
            const data = resolved.handler(db, params, ctx);
            const elapsed = Number(process.hrtime.bigint() - started) / 1e9;
            return {
                meta: META[resolved.base] || [],
                data,
                rows: data.length,
                statistics: {elapsed, rows_read: data.length, bytes_read: 0}
            };
        },

        stats() {
            const row = db.prepare('SELECT COUNT(*) AS hits, MAX(timestamp) AS latest FROM hits').get();
            return {hits: Number(row.hits || 0), latest: row.latest ? new Date(row.latest * 1000).toISOString() : null};
        },

        close() {
            db.close();
        }
    };
}

module.exports = {openStore, buildBuckets, normaliseSource, osFrom, browserFrom};
