#!/usr/bin/env node
'use strict';

/**
 * Pushes an exported Tinybird history into the proxy's local store.
 *
 * Usage:
 *   STATS_JWT_SECRET=… node import-history.js historique-prod.ndjson \
 *       https://mediafrik.ikwat.tech/_analytics
 *
 * The secret is read from the environment and never appears in the command
 * line, where it would land in the shell history and in `ps` output.
 *
 * Safe to re-run: the store rejects an event_id it already holds, so a partial
 * import can simply be repeated. Sending in batches keeps each request small
 * enough to retry cheaply and gives progress on a slow link.
 */

const fs = require('fs');
const https = require('https');
const http = require('http');
const {URL} = require('url');

const [file, endpoint] = process.argv.slice(2);
const secret = process.env.STATS_JWT_SECRET;
const BATCH = Number(process.env.BATCH || 500);

if (!file || !endpoint || !secret) {
    console.error('usage: STATS_JWT_SECRET=… node import-history.js <file.ndjson> <endpoint>');
    console.error('  endpoint example: https://mediafrik.ikwat.tech/_analytics');
    process.exit(1);
}

const lines = fs.readFileSync(file, 'utf8').split('\n').filter(l => l.trim());
const url = new URL(`${endpoint.replace(/\/$/, '')}/v0/import`);
const transport = url.protocol === 'https:' ? https : http;

function send(body) {
    return new Promise((resolve, reject) => {
        const data = Buffer.from(body);
        const req = transport.request(url, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/x-ndjson',
                'Content-Length': data.length,
                Authorization: `Bearer ${secret}`
            },
            timeout: 120000
        }, (res) => {
            let out = '';
            res.on('data', chunk => (out += chunk));
            res.on('end', () => {
                if (res.statusCode !== 200) {
                    reject(new Error(`HTTP ${res.statusCode}: ${out.slice(0, 300)}`));
                    return;
                }
                try {
                    resolve(JSON.parse(out));
                } catch (err) {
                    reject(new Error(`unparseable response: ${out.slice(0, 200)}`));
                }
            });
        });
        req.on('error', reject);
        req.on('timeout', () => req.destroy(new Error('request timed out')));
        req.end(data);
    });
}

(async () => {
    console.log(`${lines.length} event(s) to import -> ${url.href}`);
    const total = {imported: 0, duplicates: 0, rejected: 0};
    let last = null;

    for (let i = 0; i < lines.length; i += BATCH) {
        const batch = lines.slice(i, i + BATCH);
        let result;
        try {
            result = await send(batch.join('\n'));
        } catch (err) {
            console.error(`\nbatch at line ${i + 1} failed: ${err.message}`);
            console.error('Nothing is lost — re-run the same command, already-imported events are skipped.');
            process.exit(1);
        }
        total.imported += result.imported;
        total.duplicates += result.duplicates;
        total.rejected += result.rejected;
        last = result.store;
        process.stdout.write(`\r  ${Math.min(i + BATCH, lines.length)}/${lines.length}` +
            ` — ${total.imported} imported, ${total.duplicates} already present, ${total.rejected} rejected`);
    }

    console.log(`\n\nstore now holds ${last.hits} hit(s), latest ${last.latest}`);
    if (total.rejected) {
        console.log(`${total.rejected} line(s) could not be parsed — inspect them before assuming the import is complete.`);
    }
})();
