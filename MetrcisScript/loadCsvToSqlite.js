/* STREAM-SIZED CSV → SQLite loader
   ▸ Handles huge files (row-by-row)
   ▸ Creates the table on first row
   ▸ Wraps inserts in a single transaction for speed
*/

const fs = require('fs');
const path = require('path');
const sqlite3 = require('sqlite3').verbose();
const csv = require('csv-parser');

const CSV_FILE   = 'Delivered and Open.csv';   // <- your CSV
const DB_FILE    = 'mydatabase.db';
const TABLE_NAME = 'dazn_metrics';

const db = new sqlite3.Database(DB_FILE, (err) => {
    if (err) throw err;
    console.log(`✅ Connected to ${DB_FILE}`);
});

let initialized = false;
let stmt;

// BEGIN a transaction for faster bulk insert
db.serialize(() => db.run('BEGIN TRANSACTION'));

fs.createReadStream(CSV_FILE)
    .pipe(csv({ mapHeaders: ({ header }) => header.trim() }))   // trim header whitespace
    .on('data', (row) => {
        if (!initialized) {
            const columns      = Object.keys(row);
            const colQuoted    = columns.map(c => `"${c}"`).join(', ');
            const colDefs      = columns.map(c => `"${c}" TEXT`).join(', ');
            const placeholders = columns.map(() => '?').join(', ');
            const insertSQL    = `INSERT INTO "${TABLE_NAME}" (${colQuoted}) VALUES (${placeholders})`;

            db.run(`CREATE TABLE IF NOT EXISTS "${TABLE_NAME}" (${colDefs})`, (err) => {
                if (err) throw err;
            });

            stmt        = db.prepare(insertSQL);
            initialized = true;
        }

        stmt.run(Object.values(row));        // insert the current row
    })
    .on('end', () => {
        if (stmt) stmt.finalize();
        db.run('COMMIT', () => {
            db.close();
            console.log('✅ CSV data loaded & committed successfully.');
        });
    })
    .on('error', (err) => {
        console.error('❌ Error while importing CSV:', err);
    });
