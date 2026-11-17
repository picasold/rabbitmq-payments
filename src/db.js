// ---------------------------------------------------------------------------
// Lapisan database (PostgreSQL).
// Consumer A memakai ini untuk "update DB" sesuai permintaan tugas.
// Consumer B memakainya untuk mencatat tiap percobaan yang gagal.
// ---------------------------------------------------------------------------

import pg from 'pg';
import { DATABASE_URL } from './config.js';
import { createLogger } from './logger.js';

const log = createLogger('db', 'blue');
const { Pool } = pg;

export const pool = new Pool({ connectionString: DATABASE_URL });

/** Tunggu sampai Postgres benar-benar siap menerima koneksi. */
export async function waitForDb(attempts = 30, delayMs = 2000) {
  for (let i = 1; i <= attempts; i++) {
    try {
      await pool.query('SELECT 1');
      log.ok(`terhubung ke PostgreSQL (percobaan ke-${i})`);
      return;
    } catch (err) {
      log.warn(`Postgres belum siap (${i}/${attempts}): ${err.message}`);
      if (i === attempts) throw err;
      await new Promise((r) => setTimeout(r, delayMs));
    }
  }
}

/**
 * Dipakai Consumer A: catat/perbarui pembayaran yang sukses.
 * ON CONFLICT membuat operasi ini idempoten -- kalau pesan yang sama
 * terkirim dua kali, baris di DB tidak ganda.
 */
export async function markPaymentPaid(payment) {
  const sql = `
    INSERT INTO payments (payment_id, order_id, amount, currency, status, routing_key, payload, processed_at)
    VALUES ($1, $2, $3, $4, 'PAID', $5, $6, NOW())
    ON CONFLICT (payment_id) DO UPDATE
      SET status       = 'PAID',
          amount       = EXCLUDED.amount,
          routing_key  = EXCLUDED.routing_key,
          payload      = EXCLUDED.payload,
          processed_at = NOW()
    RETURNING payment_id, status, processed_at;
  `;
  const values = [
    payment.payment_id,
    payment.order_id,
    payment.amount,
    payment.currency,
    payment.routing_key,
    JSON.stringify(payment),
  ];
  const { rows } = await pool.query(sql, values);
  return rows[0];
}

/** Dipakai Consumer B: simpan status akhir pembayaran yang gagal. */
export async function markPaymentFailed(payment, status) {
  const sql = `
    INSERT INTO payments (payment_id, order_id, amount, currency, status, routing_key, payload, processed_at)
    VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
    ON CONFLICT (payment_id) DO UPDATE
      SET status       = EXCLUDED.status,
          payload      = EXCLUDED.payload,
          processed_at = NOW()
    RETURNING payment_id, status;
  `;
  const values = [
    payment.payment_id,
    payment.order_id,
    payment.amount,
    payment.currency,
    status,
    payment.routing_key,
    JSON.stringify(payment),
  ];
  const { rows } = await pool.query(sql, values);
  return rows[0];
}

/** Dipakai Consumer B: catat setiap percobaan penanganan (untuk audit retry). */
export async function logAttempt({ paymentId, attempt, outcome, reason }) {
  const sql = `
    INSERT INTO payment_failures (payment_id, attempt, outcome, reason, created_at)
    VALUES ($1, $2, $3, $4, NOW())
    RETURNING id;
  `;
  const { rows } = await pool.query(sql, [paymentId, attempt, outcome, reason]);
  return rows[0];
}

export async function closeDb() {
  await pool.end();
}
