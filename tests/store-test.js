'use strict';

/**
 * Exercises the local store against hand-built hits whose expected KPIs can be
 * worked out on paper. The point is not coverage — it is pinning the two
 * semantics that are easy to get wrong when porting the pipes: a session is
 * counted in the bucket of its FIRST pageview (with all of its pageviews), and
 * a pathname filter switches `pageviews` to a per-hit count instead.
 */

const assert = require('node:assert');
const fs = require('node:fs');
const path = require('node:path');
const os = require('node:os');

const {openStore, buildBuckets, normaliseSource} = require('../analytics-store');

const SITE = 'b6c830e8-0d85-480e-b473-5fb21380084b';
const dbPath = path.join(fs.mkdtempSync(path.join(os.tmpdir(), 'store-test-')), 'analytics.db');
const store = openStore(dbPath);

let failures = 0;
function check(name, fn) {
    try {
        fn();
        console.log(`  ok  ${name}`);
    } catch (err) {
        failures++;
        console.error(`FAIL  ${name}\n      ${err.message}`);
    }
}

const hit = (session, iso, extra = {}) => store.record({
    timestamp: iso,
    session_id: session,
    action: 'page_hit',
    version: '1',
    payload: Object.assign({
        site_uuid: SITE,
        pathname: '/',
        href: `https://example.com${extra.pathname || '/'}`,
        'user-agent': 'Mozilla/5.0 (Macintosh) AppleWebKit/537.36 Chrome/120',
        device: 'desktop',
        member_status: 'undefined',
        referrerSource: ''
    }, extra)
});

// 2026-08-05: one bouncing session, one two-page session lasting 120s.
hit('s1', '2026-08-05T10:00:00Z');
hit('s2', '2026-08-05T11:00:00Z', {pathname: '/post-a', post_uuid: 'uuid-a'});
hit('s2', '2026-08-05T11:02:00Z', {pathname: '/post-b', post_uuid: 'uuid-b'});
// 2026-08-06: a session whose second hit lands the following day. Tinybird
// attributes the whole session to the 6th, the day it started.
hit('s3', '2026-08-06T23:50:00Z', {pathname: '/post-a', post_uuid: 'uuid-a'});
hit('s3', '2026-08-07T00:10:00Z', {pathname: '/post-a', post_uuid: 'uuid-a'});
// 2026-08-07 is otherwise empty — it must still appear, as a zero row.

const range = {site_uuid: SITE, date_from: '2026-08-05', date_to: '2026-08-08', timezone: 'Etc/UTC'};

check('api_kpis fills empty days instead of dropping them', () => {
    const {data} = store.query('api_kpis', range);
    assert.deepStrictEqual(data.map(r => r.date), ['2026-08-05', '2026-08-06', '2026-08-07', '2026-08-08']);
});

check('api_kpis counts visits, pageviews, bounce rate and duration', () => {
    const {data} = store.query('api_kpis', range);
    const day5 = data.find(r => r.date === '2026-08-05');
    assert.strictEqual(day5.visits, 2, 'two sessions started on the 5th');
    assert.strictEqual(day5.pageviews, 3, 's1 one view + s2 two views');
    assert.strictEqual(day5.bounce_rate, 0.5, 'one of two sessions bounced');
    assert.strictEqual(day5.avg_session_sec, 60, 'mean of 0s and 120s');
});

check('a session belongs to the day it started, with all of its pageviews', () => {
    const {data} = store.query('api_kpis', range);
    const day6 = data.find(r => r.date === '2026-08-06');
    const day7 = data.find(r => r.date === '2026-08-07');
    assert.strictEqual(day6.visits, 1);
    assert.strictEqual(day6.pageviews, 2, 'both hits count on the 6th, not one each day');
    assert.strictEqual(day7.visits, 0, 'the 7th started no session of its own');
    assert.strictEqual(day7.pageviews, 0);
});

check('a pathname filter switches pageviews to a per-hit count', () => {
    const {data} = store.query('api_kpis', Object.assign({pathname: '/post-a'}, range));
    const day6 = data.find(r => r.date === '2026-08-06');
    const day7 = data.find(r => r.date === '2026-08-07');
    // s3 viewed /post-a once on each day: bucketed by the hit, not the session.
    assert.strictEqual(day6.pageviews, 1);
    assert.strictEqual(day7.pageviews, 1);
});

check('api_top_pages ranks by unique sessions', () => {
    const {data} = store.query('api_top_pages', range);
    assert.deepStrictEqual(data, [
        {post_uuid: '', pathname: '/', visits: 1},
        {post_uuid: 'uuid-a', pathname: '/post-a', visits: 2},
        {post_uuid: 'uuid-b', pathname: '/post-b', visits: 1}
    ].sort((a, b) => b.visits - a.visits || a.pathname.localeCompare(b.pathname)));
});

check('api_top_pages honours limit and skip', () => {
    const {data, rows} = store.query('api_top_pages', Object.assign({limit: 1}, range));
    assert.strictEqual(rows, 1);
    assert.strictEqual(data[0].pathname, '/post-a', 'the most visited page comes first');
});

check('the response carries Tinybird\'s envelope', () => {
    const result = store.query('api_kpis', range);
    assert.ok(Array.isArray(result.meta) && result.meta[0].name === 'date');
    assert.strictEqual(result.rows, result.data.length);
    assert.ok(typeof result.statistics.elapsed === 'number');
});

check('a versioned pipe name resolves to the same implementation', () => {
    assert.deepStrictEqual(store.query('api_kpis_v2', range).data, store.query('api_kpis', range).data);
});

check('an unknown pipe is reported, not guessed at', () => {
    assert.strictEqual(store.query('api_nonsense', range), null);
});

check('a single-day range buckets by hour', () => {
    const buckets = buildBuckets('2026-08-05', '2026-08-05', 'Etc/UTC', Date.UTC(2026, 7, 5, 23, 59));
    assert.strictEqual(buckets.length, 24);
    assert.strictEqual(buckets[0].label, '2026-08-05 00:00:00');
    assert.strictEqual(buckets[1].start - buckets[0].start, 3600);
});

check('a spring-forward day is 23 hours long in its own timezone', () => {
    // Europe/Paris jumps 02:00 -> 03:00 on 2026-03-29.
    const [day] = buildBuckets('2026-03-29', '2026-03-29', 'Europe/Paris', Date.UTC(2026, 2, 30));
    const buckets = buildBuckets('2026-03-28', '2026-03-30', 'Europe/Paris', Date.UTC(2026, 2, 31));
    const shortDay = buckets.find(b => b.label === '2026-03-29');
    assert.strictEqual(shortDay.end - shortDay.start, 23 * 3600, 'DST must not be modelled as a fixed offset');
    assert.ok(day.label.startsWith('2026-03-29'));
});

check('timezone shifts a hit into the neighbouring day', () => {
    // 23:50 UTC on the 6th is 01:50 on the 7th in Paris (UTC+2 in August).
    const {data} = store.query('api_kpis', Object.assign({}, range, {timezone: 'Europe/Paris'}));
    const day7 = data.find(r => r.date === '2026-08-07');
    assert.strictEqual(day7.visits, 1, 's3 started on the 7th in Paris time');
});

check('referrer hostnames consolidate to one display name', () => {
    assert.strictEqual(normaliseSource('lm.facebook.com'), 'Facebook');
    assert.strictEqual(normaliseSource('www.example.org'), 'example.org');
    assert.strictEqual(normaliseSource(''), '');
});

check('one site cannot read another site\'s hits', () => {
    const {data} = store.query('api_top_pages', Object.assign({}, range, {site_uuid: 'someone-else'}));
    assert.deepStrictEqual(data, []);
});

/* --- the remaining pipes ------------------------------------------------ */

// s4 arrives from Reddit on mobile with a campaign, then navigates internally.
// The second hit has no referrer and no UTM: session attributes must still come
// from the first hit, or every visitor would look like direct traffic.
hit('s4', '2026-08-05T09:00:00Z', {
    pathname: '/post-a', post_uuid: 'uuid-a', location: 'FR', device: 'mobile',
    referrerSource: 'www.reddit.com', utm_source: 'newsletter', utm_medium: 'email',
    utm_campaign: 'launch', utm_term: 'ghost', utm_content: 'header'
});
hit('s4', '2026-08-05T09:05:00Z', {pathname: '/', location: 'FR', device: 'mobile'});
// s5 is direct traffic from another country, on the same page.
hit('s5', '2026-08-05T09:30:00Z', {pathname: '/post-a', post_uuid: 'uuid-a', location: 'CM', device: 'desktop'});

check('api_top_sources attributes the session to its first referrer', () => {
    const {data} = store.query('api_top_sources', range);
    const reddit = data.find(r => r.source === 'Reddit');
    assert.strictEqual(reddit.visits, 1, 's4 counts once, not once per hit');
    // s1/s2/s3/s5 arrived with no referrer — direct traffic keeps an empty label.
    assert.strictEqual(data.find(r => r.source === '').visits, 4);
});

check('api_top_devices counts sessions, not pageviews', () => {
    const {data} = store.query('api_top_devices', range);
    const byDevice = Object.fromEntries(data.map(r => [r.device, r.visits]));
    assert.strictEqual(byDevice.mobile, 1, 's4 is one mobile session across two hits');
    assert.strictEqual(byDevice.desktop, 4);
});

check('api_top_locations counts distinct sessions per country', () => {
    const {data} = store.query('api_top_locations', range);
    const byLocation = Object.fromEntries(data.map(r => [r.location, r.visits]));
    assert.strictEqual(byLocation.FR, 1);
    assert.strictEqual(byLocation.CM, 1);
});

check('the UTM pipes drop empty values instead of topping the table with them', () => {
    for (const [pipe, column, value] of [
        ['api_top_utm_sources', 'utm_source', 'newsletter'],
        ['api_top_utm_mediums', 'utm_medium', 'email'],
        ['api_top_utm_campaigns', 'utm_campaign', 'launch'],
        ['api_top_utm_terms', 'utm_term', 'ghost'],
        ['api_top_utm_contents', 'utm_content', 'header']
    ]) {
        const {data} = store.query(pipe, range);
        assert.deepStrictEqual(data, [{[column]: value, visits: 1}], `${pipe} returned ${JSON.stringify(data)}`);
    }
});

check('api_post_visitor_counts is a lifetime total, ignoring the date range', () => {
    const {data} = store.query('api_post_visitor_counts', {
        site_uuid: SITE,
        post_uuids: 'uuid-a,uuid-b',
        // A window that excludes every hit — the pipe must ignore it.
        date_from: '2020-01-01', date_to: '2020-01-02'
    });
    const byPost = Object.fromEntries(data.map(r => [r.post_uuid, r.visits]));
    assert.strictEqual(byPost['uuid-a'], 4, 's2, s3, s4 and s5 each read post-a');
    assert.strictEqual(byPost['uuid-b'], 1, 'only s2 read post-b');
});

check('api_post_visitor_counts asks for nothing when given no uuids', () => {
    assert.deepStrictEqual(store.query('api_post_visitor_counts', {site_uuid: SITE}).data, []);
});

check('api_active_visitors only sees the last five minutes', () => {
    const before = store.query('api_active_visitors', {site_uuid: SITE}).data;
    assert.deepStrictEqual(before, [{active_visitors: 0}], 'the seeded hits are historical');

    hit('live-1', new Date(Date.now() - 60 * 1000).toISOString());
    hit('live-2', new Date(Date.now() - 60 * 1000).toISOString());
    hit('stale', new Date(Date.now() - 10 * 60 * 1000).toISOString());
    assert.deepStrictEqual(
        store.query('api_active_visitors', {site_uuid: SITE}).data,
        [{active_visitors: 2}],
        'the ten-minute-old session has aged out'
    );
});

check('api_gift_link_visits reports per-link usage over all time', () => {
    hit('g1', '2026-08-05T12:00:00Z', {pathname: '/post-a', post_uuid: 'uuid-a', gift_link: 'tok-1'});
    hit('g1', '2026-08-05T12:01:00Z', {pathname: '/post-a', post_uuid: 'uuid-a', gift_link: 'tok-1'});
    hit('g2', '2026-08-06T12:00:00Z', {pathname: '/post-b', post_uuid: 'uuid-b', gift_link: 'tok-2'});

    const {data} = store.query('api_gift_link_visits', {site_uuid: SITE});
    const tok1 = data.find(r => r.gift_link === 'tok-1');
    assert.strictEqual(tok1.visits, 1, 'one session');
    assert.strictEqual(tok1.views, 2, 'two views by that session');
    assert.strictEqual(tok1.last_seen, '2026-08-05 12:01:00', 'ClickHouse DateTime formatting, not ISO');
    assert.strictEqual(data.length, 2, 'non-gift hits are excluded');
});

check('api_gift_link_visits treats gift_link as an exact token, not a segment', () => {
    // Everywhere else `gift_link` means "was this a gift read at all"; here it
    // narrows to one link. Getting this backwards would return every link.
    const {data} = store.query('api_gift_link_visits', {site_uuid: SITE, gift_link: 'tok-2'});
    assert.deepStrictEqual(data.map(r => r.gift_link), ['tok-2']);
});

check('a session-level filter narrows every pipe that shares the scope', () => {
    const {data} = store.query('api_top_pages', Object.assign({device: 'mobile'}, range));
    // Only s4 is mobile; it saw /post-a and /.
    assert.deepStrictEqual(data.map(r => r.pathname).sort(), ['/', '/post-a']);
    assert.ok(data.every(r => r.visits === 1));
});

check('a store created before event_id existed still opens, keeping its hits', () => {
    // Production had a database in exactly this shape. Declaring the index
    // alongside the table is not enough: CREATE TABLE IF NOT EXISTS is a no-op
    // against an existing database, so the index would be built on a column
    // that only the migration is about to add — and the process would die at
    // startup rather than degrade.
    const oldDir = fs.mkdtempSync(path.join(os.tmpdir(), 'store-old-'));
    const oldPath = path.join(oldDir, 'legacy.db');
    const {DatabaseSync} = require('node:sqlite');
    const legacy = new DatabaseSync(oldPath);
    legacy.exec(`CREATE TABLE hits (
        id INTEGER PRIMARY KEY, site_uuid TEXT NOT NULL, timestamp INTEGER NOT NULL,
        session_id TEXT NOT NULL, action TEXT NOT NULL DEFAULT 'page_hit',
        version TEXT NOT NULL DEFAULT '1', member_uuid TEXT NOT NULL DEFAULT '',
        member_status TEXT NOT NULL DEFAULT '', post_uuid TEXT NOT NULL DEFAULT '',
        post_type TEXT NOT NULL DEFAULT '', gift_link TEXT NOT NULL DEFAULT '',
        location TEXT NOT NULL DEFAULT '', source TEXT NOT NULL DEFAULT '',
        pathname TEXT NOT NULL DEFAULT '', href TEXT NOT NULL DEFAULT '',
        device TEXT NOT NULL DEFAULT 'unknown', os TEXT NOT NULL DEFAULT 'Unknown',
        browser TEXT NOT NULL DEFAULT 'Unknown', utm_source TEXT NOT NULL DEFAULT '',
        utm_medium TEXT NOT NULL DEFAULT '', utm_campaign TEXT NOT NULL DEFAULT '',
        utm_term TEXT NOT NULL DEFAULT '', utm_content TEXT NOT NULL DEFAULT ''
    )`);
    legacy.prepare(`INSERT INTO hits (site_uuid, timestamp, session_id, pathname)
        VALUES (?,?,?,?)`).run(SITE, Math.floor(Date.parse('2026-08-01T10:00:00Z') / 1000), 'legacy-1', '/ancien/');
    legacy.close();

    const migrated = openStore(oldPath);
    const {data} = migrated.query('api_top_pages', {
        site_uuid: SITE, date_from: '2026-08-01', date_to: '2026-08-01', timezone: 'Etc/UTC'
    });
    assert.deepStrictEqual(data, [{post_uuid: '', pathname: '/ancien/', visits: 1}], 'the existing hit survives');
    // And the migrated store must accept new events with an event_id.
    assert.strictEqual(migrated.record({
        timestamp: '2026-08-01T11:00:00Z', session_id: 'legacy-2', action: 'page_hit', version: '1',
        payload: {site_uuid: SITE, pathname: '/apres/', event_id: 'evt-after-migration'}
    }), true);
    migrated.close();
    fs.rmSync(oldDir, {recursive: true, force: true});
});

/* --- importing history from a Tinybird export ---------------------------- */

// One row exactly as `analytics_events` exports it: payload as a JSON string,
// and a timestamp with no zone marker even though it is UTC.
const exported = {
    timestamp: '2026-08-04 09:00:23',
    session_id: 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee',
    action: 'page_hit',
    version: '1',
    payload: JSON.stringify({
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0) Chrome/145',
        location: 'IS', pathname: '/imported/', href: 'https://x/imported/',
        site_uuid: SITE, post_uuid: 'undefined', member_status: 'undefined',
        gift_link: '', device: 'desktop', referrerSource: '',
        event_id: 'evt-0001'
    })
};

check('an exported row imports with its payload still a string', () => {
    assert.strictEqual(store.record(exported), true);
    const {data} = store.query('api_top_pages', {
        site_uuid: SITE, date_from: '2026-08-04', date_to: '2026-08-04', timezone: 'Etc/UTC'
    });
    assert.deepStrictEqual(data, [{post_uuid: '', pathname: '/imported/', visits: 1}]);
});

check('re-importing the same event is a no-op, not a duplicate', () => {
    assert.strictEqual(store.record(exported), false, 'event_id already present');
    assert.strictEqual(store.record(Object.assign({}, exported)), false);
    const {data} = store.query('api_top_pages', {
        site_uuid: SITE, date_from: '2026-08-04', date_to: '2026-08-04', timezone: 'Etc/UTC'
    });
    assert.strictEqual(data[0].visits, 1, 'still one visit after three imports');
});

check('a zone-less timestamp is read as UTC, not as server-local time', () => {
    // The whole point: on a server at UTC+2 the naive reading would file this
    // 09:00 UTC hit at 07:00, and a late-evening hit on the previous day.
    const {data} = store.query('api_kpis', {
        site_uuid: SITE, date_from: '2026-08-04', date_to: '2026-08-04', timezone: 'Etc/UTC'
    });
    const active = data.filter(r => r.visits > 0);
    assert.deepStrictEqual(active.map(r => r.date), ['2026-08-04 09:00:00']);
});

check('an event with no event_id still records', () => {
    // Only the import path relies on event_id; a hit from an older tracker must
    // not be silently dropped for lacking one.
    const before = store.stats().hits;
    hit('no-evt', '2026-08-04T10:00:00Z', {pathname: '/sans-id/'});
    assert.strictEqual(store.stats().hits, before + 1);
});

check('every pipe Ghost is scoped to read is implemented', () => {
    // The list Ghost signs into its JWT — anything missing here would 404 in
    // the Admin rather than fail loudly at deploy time.
    const required = [
        'api_kpis', 'api_active_visitors', 'api_post_visitor_counts', 'api_top_locations',
        'api_top_pages', 'api_top_sources', 'api_top_utm_sources', 'api_top_utm_mediums',
        'api_top_utm_campaigns', 'api_top_utm_contents', 'api_top_utm_terms',
        'api_top_devices', 'api_gift_link_visits'
    ];
    const missing = required.filter(pipe => store.query(pipe, {site_uuid: SITE, post_uuids: 'x'}) === null);
    assert.deepStrictEqual(missing, [], `not implemented: ${missing.join(', ')}`);
    // And the _v2 aliases Ghost may ask for instead.
    const missingV2 = required.filter(pipe => store.query(`${pipe}_v2`, {site_uuid: SITE, post_uuids: 'x'}) === null);
    assert.deepStrictEqual(missingV2, []);
});

store.close();
fs.rmSync(path.dirname(dbPath), {recursive: true, force: true});

console.log(failures ? `\n${failures} failure(s)` : '\nall checks passed');
process.exit(failures ? 1 : 0);
