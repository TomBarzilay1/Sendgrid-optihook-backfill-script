/**
 * SendGrid → Optihook retro-backfill
 * ==================================
 * Publishes any missing OPEN / CLICK events so that Optihook’s event rows match
 * the *_COUNT columns stored in the SendGrid metrics table.
 *
 * Incrementality strategy
 * -----------------------
 * We checkpoint on the pair **(processed_time, sg_message_id)**, guaranteeing a
 * strict total order even when several rows share the same millisecond:
 *
 *   WHERE  PROCESSED_TIME  >  @ckptTs
 *      OR (PROCESSED_TIME  =  @ckptTs AND MSGID > @ckptId)
 *
 * After every successful slice, we persist the greatest pair we’ve published.
 *
 * Other guard-rails
 * -----------------
 * • Gap-aware SQL: duplicates only missing OPEN/CLICK rows.                  \
 * • Robust JSON parse for UNIQUE_ARGS.                                       \
 * • Payload validation: events lacking required fields are skipped.          \
 * • Pub/Sub publish retried 3× on transient errors.                          \
 * • Local JSON checkpoint file (`CHECKPOINT_FILE`).                          \
 * • Idempotent – safe to run repeatedly over the same date range.
 *
 * ---------------------------------------------------------------------------
 * Usage
 *   ts-node backfill.ts --start 2025-01-01 --end 2025-06-01
 *
 * Required env vars
 *   BQ_PROJECT, BQ_TABLE, OPTIHOOK_TABLE, PUBSUB_TOPIC
 *
 * Optional env vars
 *   CHUNK_DAYS       (default 3)
 *   BATCH_SIZE       (default 1000)
 *   CHECKPOINT_FILE  (default ./backfill.ckpt.json)
 *   RUN_ID           (default epoch-ms)
 * ---------------------------------------------------------------------------
 */

import {BigQuery, BigQueryTimestamp} from '@google-cloud/bigquery';
import {PubSub}                      from '@google-cloud/pubsub';
import yargs                         from 'yargs';
import {hideBin}                     from 'yargs/helpers';
import fs                            from 'fs/promises';
import path                          from 'path';

// ─────────────────── CLI & ENV ───────────────────────────────────────────────

const args = yargs(hideBin(process.argv))
    .option('start', {demandOption: true, type: 'string'})
    .option('end',   {demandOption: true, type: 'string'})
    .parseSync();

function env(name: string, def?: string): string {
    const v = process.env[name] ?? def;
    if (v == null) throw new Error(`${name} env missing`);
    return v;
}

const projectId       = env('BQ_PROJECT');
const metricsFromSendgridTable    = env('BQ_TABLE');
const optihookTable   = env('OPTIHOOK_TABLE');
const topicPath       = env('PUBSUB_TOPIC');

const chunkDays       = +env('CHUNK_DAYS',      '3');
const batchSize       = +env('BATCH_SIZE',      '1000');
const ckptFile        = path.resolve(env('CHECKPOINT_FILE', './backfill.ckpt.json'));
const runId           = env('RUN_ID', `${Date.now()}`);

// ─────────────────── GCP CLIENTS ─────────────────────────────────────────────

const bigQuery = new BigQuery({projectId});
const pubsub   = new PubSub();
const topic    = pubsub.topic(topicPath);

// ─────────────────── HELPERS ─────────────────────────────────────────────────

const DAY_MS = 86_400_000;
const isoDay = (d: Date) => d.toISOString().slice(0, 10);

function safeJson(txt: string | null){
    if (!txt) return {};
    try { return JSON.parse(txt); } catch { return {} as Record<string, unknown>; }
}

// ─────────────────── CHECKPOINT I/O ──────────────────────────────────────────

type Checkpoint = { ts: string; id: string };

async function loadCheckpoint(): Promise<Checkpoint> {
    try {
        return JSON.parse(await fs.readFile(ckptFile, 'utf8'));
    } catch (err: any) {
        if (err.code !== 'ENOENT') throw err;           // real failure → bubble up

        const init: Checkpoint = { ts: '1970-01-01 00:00:00', id: '' };
        await saveCheckpoint(init);                     // create file + parent dirs
        return init;
    }
}

async function saveCheckpoint(cp: Checkpoint): Promise<void> {
    await fs.mkdir(path.dirname(ckptFile), { recursive: true });
    await fs.writeFile(ckptFile, JSON.stringify(cp));
}

// ─────────────────── GAP-AWARE QUERY ────────────────────────────────────────

/** Build the gap-aware SQL.
 *  @param start  YYYY-MM-DD (inclusive)
 *  @param end    YYYY-MM-DD (exclusive)
 *  @param ckpt   current checkpoint tuple { ts: ISO string, id: msg-id }
 */
function buildQuery(start: string, end: string, ckpt: Checkpoint): string {
    return `
  /* ── slice from SendGrid metrics that is newer than the checkpoint ───────── */
  WITH src AS (
    SELECT
      MSGID                            AS sg_message_id,
      EMAIL,
      UNIQUE_ARGS,
      SAFE_CAST(OPEN_COUNT  AS INT64)  AS open_cnt,
      SAFE_CAST(CLICK_COUNT AS INT64)  AS click_cnt,
      PROCESSED_TIME,

      DELIVERED,
      DELIVERED_TIME,
      DELIVERED_IP,

      OPEN_FIRST_TIME,
      OPEN_IP,
      SG_MACHINE_OPEN,
      OPEN_USER_AGENT,

      CLICK_FIRST_TIME,
      CLICK_IP,
      CLICK_USER_AGENT,

      UNSUBSCRIBED,
      UNSUBSCRIBED_TIME,
      UNSUBSCRIBED_IP,
      UNSUBSCRIBED_USER_AGENT
    FROM \`${metricsFromSendgridTable}\`
    WHERE PROCESSED_TIME >= TIMESTAMP('${start}')
      AND PROCESSED_TIME <  TIMESTAMP('${end}')
      AND (
            PROCESSED_TIME  > TIMESTAMP('${ckpt.ts}')
         OR (PROCESSED_TIME = TIMESTAMP('${ckpt.ts}') AND MSGID > '${ckpt.id}')
      )
  ),

  /* ── how many rows we have already pushed per (message,event) ─────────────── */
  existing AS (
    SELECT sg_message_id, event, COUNT(*) AS pushed
    FROM   \`${optihookTable}\`
    WHERE  sg_message_id IN (SELECT sg_message_id FROM src) AND (TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) BETWEEN TIMESTAMP("${start}") AND TIMESTAMP("${end}") OR _PARTITIONTIME IS NULL)
    GROUP  BY sg_message_id, event
  ),

  /* ── events that should exist at most once per message ────────────────────── */
  singles AS (
    /* processed */
    SELECT
      s.sg_message_id,
      s.EMAIL,
      s.UNIQUE_ARGS,
      'processed'                AS event,
      s.PROCESSED_TIME           AS evt_ts,
      NULL                       AS ip,
      s.SG_MACHINE_OPEN,
      s.OPEN_USER_AGENT,
      NULL                       AS CLICK_USER_AGENT,
      s.PROCESSED_TIME           AS PROCESSED_TIME
    FROM src s
    LEFT JOIN existing e
      ON e.sg_message_id = s.sg_message_id AND e.event = 'processed'
    WHERE e.sg_message_id IS NULL

    UNION ALL

    /* delivered */
    SELECT
      s.sg_message_id,
      s.EMAIL,
      s.UNIQUE_ARGS,
      'delivered',
      s.DELIVERED_TIME,
      s.DELIVERED_IP,
      NULL,
      NULL,
      NULL,
      s.PROCESSED_TIME
    FROM src s
    LEFT JOIN existing e
      ON e.sg_message_id = s.sg_message_id AND e.event = 'delivered'
    WHERE s.DELIVERED = 'true' AND e.sg_message_id IS NULL

    UNION ALL

    /* unsubscribed */
    SELECT
      s.sg_message_id,
      s.EMAIL,
      s.UNIQUE_ARGS,
      'unsubscribed',
      s.UNSUBSCRIBED_TIME,
      s.UNSUBSCRIBED_IP,
      NULL,
      NULL,
      s.UNSUBSCRIBED_USER_AGENT,
      s.PROCESSED_TIME
    FROM src s
    LEFT JOIN existing e
      ON e.sg_message_id = s.sg_message_id AND e.event = 'unsubscribed'
    WHERE s.UNSUBSCRIBED IS NOT NULL AND e.sg_message_id IS NULL
  ),

  /* ── events that may repeat (open / click) and need gap fan-out ───────────── */
  gaps AS (
    /* open */
    SELECT
      s.sg_message_id,
      s.EMAIL,
      s.UNIQUE_ARGS,
      'open'                     AS event,
      s.OPEN_FIRST_TIME          AS evt_ts,
      s.OPEN_IP                  AS ip,
      s.SG_MACHINE_OPEN,
      s.OPEN_USER_AGENT,
      NULL                       AS CLICK_USER_AGENT,
      s.PROCESSED_TIME           AS PROCESSED_TIME,
      GREATEST(s.open_cnt - COALESCE(e.pushed, 0), 0) AS gap
    FROM src s
    LEFT JOIN existing e
      ON e.sg_message_id = s.sg_message_id AND e.event = 'open'
    WHERE s.open_cnt > COALESCE(e.pushed, 0)

    UNION ALL

    /* click */
    SELECT
      s.sg_message_id,
      s.EMAIL,
      s.UNIQUE_ARGS,
      'click',
      s.CLICK_FIRST_TIME,
      s.CLICK_IP,
      NULL,
      NULL,
      s.CLICK_USER_AGENT,
      s.PROCESSED_TIME,
      GREATEST(s.click_cnt - COALESCE(e.pushed, 0), 0) AS gap
    FROM src s
    LEFT JOIN existing e
      ON e.sg_message_id = s.sg_message_id AND e.event = 'click'
    WHERE s.click_cnt > COALESCE(e.pushed, 0)
  )

  /* ── final union + fan-out ────────────────────────────────────────────────── */
  SELECT
    sg_message_id,
    EMAIL,
    UNIQUE_ARGS,
    event,
    evt_ts,
    ip,
    SG_MACHINE_OPEN,
    OPEN_USER_AGENT,
    CLICK_USER_AGENT,
    PROCESSED_TIME
  FROM singles

  UNION ALL

  SELECT
    sg_message_id,
    EMAIL,
    UNIQUE_ARGS,
    event,
    evt_ts,
    ip,
    SG_MACHINE_OPEN,
    OPEN_USER_AGENT,
    CLICK_USER_AGENT,
    PROCESSED_TIME
  FROM gaps, UNNEST(GENERATE_ARRAY(1, gap)) AS _

  ORDER BY PROCESSED_TIME, sg_message_id;
  `;
}


// ─────────────────── ROW → PAYLOAD + VALIDATION ─────────────────────────────

interface MetricsRow {
    sg_message_id: string;
    EMAIL: string;
    UNIQUE_ARGS: string | null;
    event: string;
    evt_ts: BigQueryTimestamp | null;
    ip: string | null;
    SG_MACHINE_OPEN: boolean | null;
    OPEN_USER_AGENT: string | null;
    CLICK_USER_AGENT: string | null;
    PROCESSED_TIME: BigQueryTimestamp;
}

const requiredPaths: (string | string[])[] = [
    'event',
    'category',
    'origin',
    'tenant_id',
    'customer',
    ['context', 'execution_gateway'],
    'insertion_time',
    'send_id',
    'execution_date',
];

function has(obj: any, p: string | string[]): boolean {
    if (Array.isArray(p)) {
        let cur = obj;
        for (const k of p) { if (cur == null || !(k in cur)) return false; cur = cur[k]; }
        return cur != null;
    }
    return obj[p] != null;
}

function isValid(p: Record<string, unknown>): boolean {
    return requiredPaths.every(r => has(p, r));
}

function toPayload(row: MetricsRow): Record<string, unknown> {
    const ua = safeJson(row.UNIQUE_ARGS);

    return {
        run_id:            runId,
        brand:             ua['brand'],
        cancel_id:         ua['cancel_id'],
        customer:          ua['customer'],
        email:             row.EMAIL,
        event:             row.event,
        execution_date:    ua['execution_date'],
        execution_datetime:ua['execution_datetime'],
        ip:                row.ip,
        is_hashed:         ua['is_hashed'],
        optimove_mail_name:ua['optimove_mail_name'],
        region:            ua['region'],
        sendAt:            ua['sendAt'],
        send_id:           ua['send_id'],
        sg_machine_open:   row.event === 'open' ? !!row.SG_MACHINE_OPEN : false,
        sg_message_id:     row.sg_message_id,
        tenant_id:         ua['tenant_id'] ? +ua['tenant_id'] : null,
        timestamp:         row.evt_ts?.value ?? new Date().toISOString(),
        useragent:         row.OPEN_USER_AGENT ?? row.CLICK_USER_AGENT,
        insertion_time:    new Date().toISOString(),

        origin:            'optimail',
        category:          'metric',
        context:           {execution_gateway: 'sendgrid'},
    };
}

// ─────────────────── BATCH PUBLISH WITH RETRY ────────────────────────────────

async function publish(messages: Record<string, unknown>[]): Promise<void> {
    for (let attempt = 1; attempt <= 3; attempt++) {
        try {
            await topic.publishMessage({data: Buffer.from(JSON.stringify(messages))});
            return;
        } catch (e) {
            console.error(`Publish failed (attempt ${attempt})`, (e as Error).message);
            if (attempt === 3) throw e;
            await new Promise(r => setTimeout(r, attempt * 1_000));
        }
    }
}

// ─────────────────── MAIN LOOP ───────────────────────────────────────────────

async function backfill(): Promise<void> {
    let ckpt = await loadCheckpoint();
    let sliceStart = new Date(args.start);
    const endDate  = new Date(args.end);
    let total      = 0;

    while (sliceStart < endDate) {
        const sliceEnd = new Date(Math.min(sliceStart.getTime() + chunkDays * DAY_MS, endDate.getTime()));

        const sql = buildQuery(isoDay(sliceStart), isoDay(sliceEnd), ckpt);
        const [job] = await bigQuery.createQueryJob({query: sql, useLegacySql: false});
        const stream = job.getQueryResultsStream({maxResults: 10_000});

        const batch: Record<string, unknown>[] = [];
        for await (const row of stream) {
            const payload = toPayload(row as MetricsRow);
            if (!isValid(payload)) { console.debug('skip invalid', payload.sg_message_id); continue; }

            batch.push(payload);


            if (batch.length >= batchSize) { await publish(batch); total += batch.length; batch.length = 0; }
            ckpt = ckpt = {
                ts: (row as MetricsRow).PROCESSED_TIME.value
                    .replace('T',' ').replace('Z','')
                    .replace(/\.(\d{6})\d+$/, '.$1'),   // keep max 6 decimals
                id: (row as MetricsRow).sg_message_id
            };

        }
        if (batch.length) { await publish(batch); total += batch.length; }

        await saveCheckpoint(ckpt);
        console.log(`✔ ${isoDay(sliceStart)} → ${isoDay(sliceEnd)} (${total} rows)`);
        sliceStart = sliceEnd;
    }

    console.log(`✔ Run ${runId} finished – published ${total} events.`);
}

// kick off
backfill().catch(err => { console.error(err); process.exitCode = 1; });
