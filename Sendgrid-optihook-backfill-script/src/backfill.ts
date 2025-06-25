/**
 * Retro-backfill SendGrid metrics → Optihook Pub/Sub topic
 *
 *   ts-node src/backfill.ts \
 *     --start 2024-01-01 \
 *     --end   2024-12-31
 *
 * Environment variables:
 *   BQ_PROJECT      – GCP project that owns the table
 *   BQ_TABLE        – full table name e.g. cloudcore-prod-eu.chris_w_eu.dazn_events
 *   PUBSUB_TOPIC    – full Pub/Sub topic path
 *   CHUNK_DAYS      – days per query window      (default 3)
 *   BATCH_SIZE      – msgs per Pub/Sub publish   (default 1 000)
 */

import {BigQuery, BigQueryTimestamp} from '@google-cloud/bigquery';
import {PubSub}   from '@google-cloud/pubsub';
import yargs from 'yargs';
import {hideBin} from 'yargs/helpers';

// ---------- CLI & env --------------------------------------------------------------------------

const argv = yargs(hideBin(process.argv))
    .option('start', {demandOption: true, type: 'string', desc: 'ISO start date (inclusive)'})
    .option('end',   {demandOption: true, type: 'string', desc: 'ISO end date   (exclusive)'})
    .parseSync();

const BQ_PROJECT   = process.env.BQ_PROJECT   ?? (() => {throw new Error('BQ_PROJECT env missing');})();
const BQ_TABLE     = process.env.BQ_TABLE     ?? (() => {throw new Error('BQ_TABLE env missing');})();
const PUBSUB_TOPIC = process.env.PUBSUB_TOPIC ?? (() => {throw new Error('PUBSUB_TOPIC env missing');})() ;
const OPTIHOOK_TABLE = process.env.OPTIHOOK_TABLE
    ?? (() => { throw new Error('OPTIHOOK_TABLE env missing'); })();

const CHUNK_DAYS = +(process.env.CHUNK_DAYS ?? 1);
const BATCH_SIZE = +(process.env.BATCH_SIZE ?? 1_000);

// ---------- Clients ---------------------------------------------------------------------------

const bigquery = new BigQuery({projectId: BQ_PROJECT});
const pubsub   = new PubSub();
const topic    = pubsub.topic(PUBSUB_TOPIC);

// ---------- Helpers ----------------------------------------------------------------------------

function fmt(date: Date): string {
    // YYYY-MM-DD
    return date.toISOString().slice(0, 10);
}

const EVENT_RULES = [
    {
        name: 'processed',
        filter: 'TRUE',
        ts:     'PROCESSED_TIME',
        ip:     'NULL',
        extras: 'SG_MACHINE_OPEN, OPEN_USER_AGENT',
    },
    {
        name: 'delivered',
        filter: "DELIVERED = 'true'",
        ts:     'DELIVERED_TIME',
        ip:     'DELIVERED_IP',
        extras: 'NULL, NULL',
    },
    {
        name: 'open',
        filter: 'SAFE_CAST(OPEN_COUNT AS INT64) > 0',
        ts:     'OPEN_FIRST_TIME',
        ip:     'OPEN_IP',
        extras: 'SG_MACHINE_OPEN, OPEN_USER_AGENT',
    },
    {
        name: 'click',
        filter: 'SAFE_CAST(CLICK_COUNT AS INT64) > 0',
        ts:     'CLICK_FIRST_TIME',
        ip:     'CLICK_IP',
        extras: 'NULL, CLICK_USER_AGENT',
    },
    {
        name: 'unsubscribed',
        filter: 'UNSUBSCRIBED IS NOT NULL',
        ts:     'UNSUBSCRIBED_TIME',
        ip:     'UNSUBSCRIBED_IP',
        extras: 'NULL, UNSUBSCRIBED_USER_AGENT',
    },
];

function buildQuery(start: string, end: string): string {
    return `
  /* -------- source rows from the SendGrid metrics table -------- */
  WITH src AS (
    SELECT
      MSGID                            AS sg_message_id,
      EMAIL,
      UNIQUE_ARGS,
      SAFE_CAST(OPEN_COUNT  AS INT64)  AS open_cnt,
      SAFE_CAST(CLICK_COUNT AS INT64)  AS click_cnt,

      -- per-event columns
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
    FROM \`${BQ_TABLE}\`
    WHERE PROCESSED_TIME >= TIMESTAMP('${start}')
      AND PROCESSED_TIME <  TIMESTAMP('${end}')
  ),

  /* -------- what we've already pushed to Optihook -------- */
  existing AS (
    SELECT
      sg_message_id,
      event,
      COUNT(*) AS pushed
    FROM \`${OPTIHOOK_TABLE}\`
    WHERE sg_message_id IN (SELECT sg_message_id FROM src)   -- small scan
    GROUP BY sg_message_id, event
  ),

  /* -------- events that can occur only once per message -------- */
  singles AS (
    /* processed */
    SELECT
      s.sg_message_id, s.EMAIL, s.UNIQUE_ARGS,
      'processed'          AS event,
      s.PROCESSED_TIME     AS evt_ts,
      NULL                 AS ip,
      s.SG_MACHINE_OPEN,
      s.OPEN_USER_AGENT,
      NULL                 AS CLICK_USER_AGENT
    FROM src s
    LEFT JOIN existing e
      ON  e.sg_message_id = s.sg_message_id
      AND e.event        = 'processed'
    WHERE e.sg_message_id IS NULL

    UNION ALL

    /* delivered */
    SELECT
      s.sg_message_id, s.EMAIL, s.UNIQUE_ARGS,
      'delivered',
      s.DELIVERED_TIME,
      s.DELIVERED_IP,
      NULL, NULL, NULL
    FROM src s
    LEFT JOIN existing e
      ON  e.sg_message_id = s.sg_message_id
      AND e.event        = 'delivered'
    WHERE s.DELIVERED = 'true'
      AND e.sg_message_id IS NULL

    UNION ALL

    /* unsubscribed */
    SELECT
      s.sg_message_id, s.EMAIL, s.UNIQUE_ARGS,
      'unsubscribed',
      s.UNSUBSCRIBED_TIME,
      s.UNSUBSCRIBED_IP,
      NULL, NULL, s.UNSUBSCRIBED_USER_AGENT
    FROM src s
    LEFT JOIN existing e
      ON  e.sg_message_id = s.sg_message_id
      AND e.event        = 'unsubscribed'
    WHERE s.UNSUBSCRIBED IS NOT NULL
      AND e.sg_message_id IS NULL
  ),

  /* -------- events that can repeat (open / click) -------- */
  gaps AS (
    /* OPEN */
    SELECT
      s.sg_message_id, s.EMAIL, s.UNIQUE_ARGS,
      'open'                 AS event,
      s.OPEN_FIRST_TIME      AS evt_ts,
      s.OPEN_IP              AS ip,
      s.SG_MACHINE_OPEN,
      s.OPEN_USER_AGENT,
      NULL                   AS CLICK_USER_AGENT,
      GREATEST(s.open_cnt - COALESCE(e.pushed, 0), 0) AS gap
    FROM src s
    LEFT JOIN existing e
      ON  e.sg_message_id = s.sg_message_id
      AND e.event        = 'open'
    WHERE s.open_cnt > COALESCE(e.pushed, 0)

    UNION ALL

    /* CLICK */
    SELECT
      s.sg_message_id, s.EMAIL, s.UNIQUE_ARGS,
      'click',
      s.CLICK_FIRST_TIME,
      s.CLICK_IP,
      NULL                   AS SG_MACHINE_OPEN,
      NULL                   AS OPEN_USER_AGENT,
      s.CLICK_USER_AGENT,
      GREATEST(s.click_cnt - COALESCE(e.pushed, 0), 0) AS gap
    FROM src s
    LEFT JOIN existing e
      ON  e.sg_message_id = s.sg_message_id
      AND e.event        = 'click'
    WHERE s.click_cnt > COALESCE(e.pushed, 0)
  )

  /* -------- final result set -------- */
  SELECT
    sg_message_id, EMAIL, UNIQUE_ARGS, event, evt_ts, ip,
    SG_MACHINE_OPEN, OPEN_USER_AGENT, CLICK_USER_AGENT
  FROM singles         -- single-occurrence events

  UNION ALL

  /* for opens and clicks: repeat the row 'gap' times */
  SELECT
    sg_message_id, EMAIL, UNIQUE_ARGS, event, evt_ts, ip,
    SG_MACHINE_OPEN, OPEN_USER_AGENT, CLICK_USER_AGENT
  FROM gaps, UNNEST(GENERATE_ARRAY(1, gap)) AS _fanout;
  `;
}



type Row = {
    sg_message_id: string;
    EMAIL: string;
    UNIQUE_ARGS: string;
    event: string;
    evt_ts: BigQueryTimestamp | null;
    ip: string | null;
    SG_MACHINE_OPEN: boolean | null;
    OPEN_USER_AGENT: string | null;
    CLICK_USER_AGENT: string | null;
};

function rowToPayload(row: Row): Record<string, unknown> {
    const ua = row.UNIQUE_ARGS ? JSON.parse(row.UNIQUE_ARGS) : {};
    return {
        brand:               ua.brand,
        cancel_id:           ua.cancel_id,
        customer:            ua.customer,
        email:               row.EMAIL,
        event:               row.event,
        execution_date:      ua.execution_date,
        execution_datetime:  ua.execution_datetime,
        ip:                  row.ip,
        is_hashed:           ua.is_hashed,
        optimove_mail_name:  ua.optimove_mail_name,
        region:              ua.region,
        sendAt:              ua.sendAt,
        send_id:             ua.send_id,
        sg_machine_open:     row.event === 'open' ? !!row.SG_MACHINE_OPEN : false,
        sg_message_id:       row.sg_message_id,
        tenant_id:           ua.tenant_id ? +ua.tenant_id : undefined,
        timestamp:           row.evt_ts?.value ?? new Date().toISOString(),
        useragent:           row.OPEN_USER_AGENT ?? row.CLICK_USER_AGENT,
        insertion_time:      new Date().toISOString(),
        origin:              'optimail',
        category:            'metric',
        context:             {execution_gateway: 'sendgrid'},
    };
}

// ---------- Main driver ------------------------------------------------------------------------

async function publishBatch(msgs: Buffer[]) {
    await topic.publishMessage({
        data: Buffer.from(JSON.stringify(msgs.map(b => JSON.parse(b.toString()))))
    });
}

async function backfill(startISO: string, endISO: string) {
    let cursor = new Date(startISO);
    const end   = new Date(endISO);
    let total   = 0;

    while (cursor < end) {
        const chunkEnd = new Date(cursor);
        chunkEnd.setDate(chunkEnd.getDate() + CHUNK_DAYS);
        if (chunkEnd > end) chunkEnd.setTime(end.getTime());

        const query = buildQuery(fmt(cursor), fmt(chunkEnd));

        const [job] = await bigquery.createQueryJob({query, useLegacySql: false});
        const stream = job.getQueryResultsStream({maxResults: 10_000});

        let batch: Buffer[] = [];

        for await (const rowAny of stream) {
            const row = rowAny as Row;
            batch.push(Buffer.from(JSON.stringify(rowToPayload(row))));
            if (batch.length >= BATCH_SIZE) {
                await publishBatch(batch);
                total += batch.length;
                batch = [];
            }
        }
        if (batch.length) {
            await publishBatch(batch);
            total += batch.length;
        }

        console.log(
            `Processed slice ${fmt(cursor)} → ${fmt(chunkEnd)}  (${total} msgs so far)`,
        );
        cursor = chunkEnd;
    }

    console.log(`✔ Finished. Published ${total} messages.`);
}

// ---------- Fire ------------------------------------------------------------------------------

backfill(argv.start, argv.end).catch(err => {
    console.error(err);
    process.exitCode = 1;
});
