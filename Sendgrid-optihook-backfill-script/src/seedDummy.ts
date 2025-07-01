/**
 * RESET the dummy SendGrid table (truncate) and INSERT 5 rich test rows
 *
 *   npx ts-node src/resetAndSeedFull.ts \
 *        --project optihook-stg \
 *        --dataset optihook \
 *        --table   sendgrid_dummy
 */

import { BigQuery } from '@google-cloud/bigquery';
import yargs from 'yargs';
import { hideBin } from 'yargs/helpers';

const argv = yargs(hideBin(process.argv))
    .option('project', { type: 'string', demandOption: true })
    .option('dataset', { type: 'string', demandOption: true })
    .option('table',   { type: 'string', demandOption: true })
    .parseSync();

const bq        = new BigQuery({ projectId: argv.project });
const fullTable = `\`${argv.project}.${argv.dataset}.${argv.table}\``;

async function main() {
    /* -------------------------------------------------------------- *
     * 1. Create table with full prod-like schema (if not exists)     *
     * -------------------------------------------------------------- */
    await bq.query(`
    CREATE TABLE IF NOT EXISTS ${fullTable} (
      MSGID                     STRING,
      EMAIL                     STRING,
      EMAIL_DOMAIN              STRING,
      USER_ID                   STRING,
      RESELLER_ID               STRING,
      SUBJECT                   STRING,
      CATEGORY                  STRING,
      UNIQUE_ARGS               STRING,
      FROM_ADDRESS              STRING,
      ENVELOPE_FROM             STRING,
      APP_ID                    STRING,
      LINKS                     STRING,

      PROCESSED_TIME            TIMESTAMP,
      RECV_HOST                 STRING,
      DELIVERED                 STRING,
      DELIVERED_TIME            TIMESTAMP,
      EE_TIME                   TIMESTAMP,
      DELIVERED_IP              STRING,
      MX                        STRING,
      POOL                      STRING,

      OPEN_COUNT                STRING,
      OPEN_FIRST_TIME           TIMESTAMP,
      OPEN_LAST_TIME            TIMESTAMP,
      OPEN_IP                   STRING,
      OPEN_USER_AGENT           STRING,

      CLICK_COUNT               STRING,
      CLICK_FIRST_TIME          TIMESTAMP,
      CLICK_LAST_TIME           TIMESTAMP,
      CLICK_IP                  STRING,
      CLICK_USER_AGENT          STRING,

      BOUNCED                   STRING,
      BOUNCED_TYPE              STRING,
      BOUNCED_REASON            STRING,
      BOUNCED_TIME              TIMESTAMP,
      BOUNCED_IP                STRING,

      DEFERRED_COUNT            STRING,
      DEFERRED_FIRST_TIME       TIMESTAMP,
      DEFERRED_LAST_TIME        TIMESTAMP,
      DEFERRED_REASON           STRING,

      DROPPED                   STRING,
      DROPPED_TYPE              STRING,
      DROPPED_TIME              TIMESTAMP,

      UNSUBSCRIBED              STRING,
      UNSUBSCRIBED_TIME         TIMESTAMP,
      UNSUBSCRIBED_TYPE         STRING,
      UNSUBSCRIBED_IP           STRING,
      UNSUBSCRIBED_USER_AGENT   STRING,

      SPAMREPORT                STRING,
      SPAMREPORT_TIME           TIMESTAMP,
      SPAMREPORT_IP             STRING,

      CREATED                   TIMESTAMP,
      UPDATED                   TIMESTAMP,

      RECV_CLIENT_IP            STRING,
      RECV_PROTO                STRING,
      RECV_MSGID                STRING,
      RECV_DC                   STRING,
      RECV_RECIPIENT_COUNT      STRING,
      RECV_API_KEY_ID           STRING,
      RECV_CREDENTIAL_ID        STRING,

      IP_POOL_ID                STRING,
      CLICK_UNIQUE_COUNT        STRING,
      SG_MACHINE_OPEN           BOOL,
      BOUNCE_RULE               STRING,
      BOUNCE_CLASSIFICATION     STRING,
      DATA_RESIDENCY            STRING
    )
    PARTITION BY DATE(PROCESSED_TIME)
  `);
    console.log(`✔ Table schema ensured: ${fullTable}`);

    /* -------------------------- *
     * 2. Truncate previous rows  *
     * -------------------------- */
    await bq.query(`TRUNCATE TABLE ${fullTable}`);
    console.log('✔ Previous dummy data removed');

    /* ---------------------------------------------------- *
     * 3. Insert 5 exhaustive dummy rows (cover all events) *
     * ---------------------------------------------------- */
    await bq.query(`
    INSERT INTO ${fullTable}
      (MSGID, EMAIL, EMAIL_DOMAIN, USER_ID, RESELLER_ID, SUBJECT, CATEGORY,
       UNIQUE_ARGS, FROM_ADDRESS, LINKS, PROCESSED_TIME, DELIVERED, DELIVERED_TIME,
       DELIVERED_IP, OPEN_COUNT, OPEN_FIRST_TIME, OPEN_IP, OPEN_USER_AGENT,
       CLICK_COUNT, CLICK_FIRST_TIME, CLICK_IP, CLICK_USER_AGENT,
       UNSUBSCRIBED, UNSUBSCRIBED_TIME, UNSUBSCRIBED_IP, UNSUBSCRIBED_USER_AGENT,
       SG_MACHINE_OPEN, DATA_RESIDENCY)
    VALUES
    --processed + delivered +  2 open - 4
    ('msg_001','alice@example.com','example.com','u1','r1','Dummy subj 1',NULL,
     '{"brand":"AA01","cancel_id":"c1","customer":"cust1","execution_date":"2025-06-14","execution_datetime":"1749918600","is_hashed":"false","optimove_mail_name":"Mail1","region":"eu","sendAt":"1749918600","send_id":"0.1","tenant_id":"3014"}',
     '"BrandX" <noreply@brandx.com>',
     '{"html":"1"}', CURRENT_TIMESTAMP(),'true',CURRENT_TIMESTAMP(),'1.1.1.1',
     '2', CURRENT_TIMESTAMP(),'2.2.2.2','Mozilla/5.0',
     '0', NULL, NULL, NULL,
     NULL, NULL, NULL, NULL,
     TRUE,'global'),

    -- processed only - 1
    ('msg_002','bob@example.com','example.com','u2','r2','Dummy subj 2',NULL,
     '{"brand":"BB02","cancel_id":"c2","customer":"cust2","execution_date":"2025-06-14","execution_datetime":"1749918601","is_hashed":"false","optimove_mail_name":"Mail2","region":"us","sendAt":"1749918601","send_id":"0.2","tenant_id":"3014"}',
     '"BrandX" <noreply@brandx.com>',
     '{"html":"1"}', CURRENT_TIMESTAMP(),'false',NULL,NULL,
     '0', NULL,NULL,NULL,
     '0', NULL,NULL,NULL,
     NULL,NULL,NULL,NULL,
     NULL,'global'),

    --processed + delivered + 4 click - 6 
    ('msg_003','carol@example.com','example.com','u3','r3','Dummy subj 3',NULL,
     '{"brand":"CC03","cancel_id":"c3","customer":"cust3","execution_date":"2025-06-14","execution_datetime":"1749918602","is_hashed":"true","optimove_mail_name":"Mail3","region":"eu","sendAt":"1749918602","send_id":"0.3","tenant_id":"3014"}',
     '"BrandX" <noreply@brandx.com>',
     '{"html":"1"}', CURRENT_TIMESTAMP(),'true',CURRENT_TIMESTAMP(),'3.3.3.3',
     '0', NULL,NULL,NULL,
     '4', CURRENT_TIMESTAMP(),'4.4.4.4','Mozilla/4.0',
     NULL,NULL,NULL,NULL,
     NULL,'global'),

    --processed + unsubscribed + delivered - 3
    ('msg_004','dave@example.com','example.com','u4','r4','Dummy subj 4',NULL,
     '{"brand":"DD04","cancel_id":"c4","customer":"cust4","execution_date":"2025-06-14","execution_datetime":"1749918603","is_hashed":"false","optimove_mail_name":"Mail4","region":"us","sendAt":"1749918603","send_id":"0.4","tenant_id":"3014"}',
     '"BrandX" <noreply@brandx.com>',
     '{"html":"1"}', CURRENT_TIMESTAMP(),'true',CURRENT_TIMESTAMP(),'5.5.5.5',
     '0', NULL,NULL,NULL,
     '0', NULL,NULL,NULL,
     'yes', CURRENT_TIMESTAMP(),'6.6.6.6','Mozilla/5.0',
     NULL,'global'),

    -- delivered + processed - 2
    ('msg_005','eve@example.com','example.com','u5','r5','Dummy subj 5',NULL,
     '{"brand":"EE05","cancel_id":"c5","customer":"cust5","execution_date":"2025-06-14","execution_datetime":"1749918604","is_hashed":"true","optimove_mail_name":"Mail5","region":"eu","sendAt":"1749918604","send_id":"0.5","tenant_id":"3014"}',
     '"BrandX" <noreply@brandx.com>',
     '{"html":"1"}', CURRENT_TIMESTAMP(),'true',CURRENT_TIMESTAMP(),'7.7.7.7',
     '0', NULL,NULL,NULL,
     '0', NULL,NULL,NULL,
     NULL,NULL,NULL,NULL,
     NULL,'global')
  `);

    console.log('✔ Inserted 5 rich dummy rows should be total of 16 events');
}

main().catch(err => {
    console.error(err);
    process.exitCode = 1;
});
