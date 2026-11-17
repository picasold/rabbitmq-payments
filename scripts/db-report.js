// Ringkasan isi database: hasil kerja Consumer A dan Consumer B.
// Jalankan: npm run db:report
import { pool, waitForDb } from '../src/db.js';

await waitForDb();

const { rows: payments } = await pool.query(
  `SELECT status, COUNT(*)::int AS jumlah FROM payments GROUP BY status ORDER BY status`
);
console.log('\n=== Tabel payments ===');
console.table(payments);

const { rows: detail } = await pool.query(
  `SELECT order_id, status, amount, routing_key,
          to_char(processed_at, 'HH24:MI:SS') AS waktu
     FROM payments ORDER BY processed_at DESC LIMIT 20`
);
console.table(detail);

const { rows: attempts } = await pool.query(
  `SELECT f.payment_id, p.order_id, f.attempt, f.outcome, f.reason
     FROM payment_failures f
     LEFT JOIN payments p ON p.payment_id = f.payment_id
    ORDER BY f.payment_id, f.attempt`
);
console.log('=== Riwayat percobaan (bukti retry) ===');
console.table(attempts.map(({ payment_id, ...r }) => r));

await pool.end();
process.exit(0);
