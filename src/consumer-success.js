// ---------------------------------------------------------------------------
// CONSUMER A -- success worker
//
// Binding : payment.success  (dari topic exchange payments.event)
// Queue   : payments.success.q   -> DURABLE
// ACK     : MANUAL (noAck: false)
//
// Tugasnya: menulis log dan meng-update database.
// Pesan baru dihapus dari queue setelah DB benar-benar berhasil di-update.
// ---------------------------------------------------------------------------

import { QUEUES, PREFETCH } from './config.js';
import { connectWithRetry, assertTopology } from './topology.js';
import { waitForDb, markPaymentPaid, closeDb } from './db.js';
import { createLogger } from './logger.js';

const log = createLogger('consumer-A', 'green');

async function main() {
  await waitForDb();

  const connection = await connectWithRetry();
  const channel = await connection.createChannel();
  await assertTopology(channel);

  // Ambil maksimal 1 pesan sebelum ACK -> beban terbagi rata antar worker
  await channel.prefetch(PREFETCH);

  log.info(`menunggu pesan di queue "${QUEUES.SUCCESS}" ... (Ctrl+C untuk berhenti)`);

  await channel.consume(
    QUEUES.SUCCESS,
    async (msg) => {
      if (!msg) return;

      const routingKey = msg.fields.routingKey;
      let payment;

      try {
        payment = JSON.parse(msg.content.toString());
      } catch (err) {
        // Pesan rusak tidak akan pernah bisa diproses -> jangan requeue,
        // langsung buang supaya tidak memacetkan queue.
        log.error('payload bukan JSON yang valid, pesan dibuang:', err.message);
        return channel.nack(msg, false, false);
      }

      try {
        log.info(
          `terima ${routingKey}  ${payment.order_id}  ` +
            `Rp${Number(payment.amount).toLocaleString('id-ID')}  via ${payment.method}`
        );

        // --- update DB ---
        const row = await markPaymentPaid({ ...payment, routing_key: routingKey });

        log.ok(
          `DB updated -> payment_id=${row.payment_id} status=${row.status}`
        );

        // --- MANUAL ACK: baru di sini pesan dihapus dari queue ---
        channel.ack(msg);
      } catch (err) {
        // Gagal update DB (misal Postgres sedang down).
        // requeue=true supaya pesan dicoba lagi nanti, bukan hilang.
        log.error(`gagal update DB: ${err.message} -> pesan dikembalikan ke queue`);
        channel.nack(msg, false, true);
        await new Promise((r) => setTimeout(r, 1000));
      }
    },
    { noAck: false } // <-- MANUAL ACK
  );

  // Berhenti dengan rapi saat container di-stop (docker compose down / Ctrl+C)
  const shutdown = async () => {
    log.info('menutup koneksi ...');
    try {
      await channel.close();
      await connection.close();
      await closeDb();
    } catch {}
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

main().catch((err) => {
  log.error(err);
  process.exit(1);
});
