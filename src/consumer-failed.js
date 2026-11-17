// ---------------------------------------------------------------------------
// CONSUMER B -- failed worker + Dead Letter Exchange
//
// Binding : payment.failed  (dari topic exchange payments.event)
// Queue   : payments.failed.q  -> DURABLE, manual ACK
//
// Alur retry:
//   1. Pesan gagal diproses  -> channel.nack(msg, false, false)
//   2. RabbitMQ otomatis melempar pesan ke exchange payments.retry
//      (karena queue punya argumen x-dead-letter-exchange)
//   3. Pesan mengendap di payments.failed.retry.q selama TTL 5 detik
//   4. TTL habis -> pesan dilempar balik ke payments.event -> masuk lagi ke sini
//   5. Setelah MAX_RETRY (2) kali, pesan dikirim ke payments.dlx (DLQ)
//
// Hitungan retry diambil dari header `x-death` yang ditambahkan RabbitMQ
// secara otomatis setiap kali pesan mati di sebuah queue.
// ---------------------------------------------------------------------------

import {
  QUEUES,
  EXCHANGES,
  ROUTING_KEYS,
  MAX_RETRY,
  RETRY_TTL_MS,
  PREFETCH,
} from './config.js';
import { connectWithRetry, assertTopology } from './topology.js';
import { waitForDb, markPaymentFailed, logAttempt, closeDb } from './db.js';
import { createLogger } from './logger.js';

const log = createLogger('consumer-B', 'yellow');

/**
 * Berapa kali pesan ini sudah melewati antrean retry.
 * RabbitMQ menyimpan jejaknya di header x-death:
 *   [{ queue: 'payments.failed.retry.q', reason: 'expired', count: 2, ... }]
 */
function getRetryCount(msg) {
  const deaths = msg.properties?.headers?.['x-death'] || [];
  const fromRetryQueue = deaths.find(
    (d) => d.queue === QUEUES.RETRY && d.reason === 'expired'
  );
  return fromRetryQueue ? Number(fromRetryQueue.count) : 0;
}

/**
 * Simulasi pekerjaan nyata menangani pembayaran gagal
 * (misalnya: memberi tahu gateway, membatalkan stok, mengirim email).
 *
 * Hasilnya ditentukan field `scenario` dari producer supaya demo bisa
 * menunjukkan kedua jalur:
 *   - transient : gagal di percobaan pertama, pulih saat retry
 *   - permanent : gagal terus -> berakhir di DLQ
 */
async function handleFailedPayment(payment, attempt) {
  await new Promise((r) => setTimeout(r, 150)); // pura-pura ada proses I/O

  if (payment.scenario === 'transient' && attempt >= 2) return;
  if (payment.scenario === 'ok') return;

  throw new Error(
    payment.failure_reason
      ? `tidak bisa memulihkan pembayaran: ${payment.failure_reason}`
      : 'penanganan pembayaran gagal'
  );
}

async function main() {
  await waitForDb();

  const connection = await connectWithRetry();
  const channel = await connection.createChannel();
  await assertTopology(channel);
  await channel.prefetch(PREFETCH);

  log.info(
    `menunggu pesan di queue "${QUEUES.FAILED}"  ` +
      `(max retry = ${MAX_RETRY}, jeda = ${RETRY_TTL_MS / 1000}s)`
  );

  await channel.consume(
    QUEUES.FAILED,
    async (msg) => {
      if (!msg) return;

      let payment;
      try {
        payment = JSON.parse(msg.content.toString());
      } catch (err) {
        log.error('payload bukan JSON yang valid, langsung ke DLQ');
        channel.publish(EXCHANGES.DLX, ROUTING_KEYS.FAILED, msg.content, {
          persistent: true,
          headers: { 'x-dead-reason': 'INVALID_JSON' },
        });
        return channel.ack(msg);
      }

      const retryCount = getRetryCount(msg);   // 0, 1, 2, ...
      const attempt = retryCount + 1;          // percobaan ke-berapa

      log.info(
        `terima ${payment.order_id}  percobaan ke-${attempt}` +
          `  (retry terpakai ${retryCount}/${MAX_RETRY})` +
          `  reason=${payment.failure_reason}`
      );

      try {
        // ---------- coba tangani ----------
        await handleFailedPayment(payment, attempt);

        await markPaymentFailed(
          { ...payment, routing_key: msg.fields.routingKey },
          'FAILED_HANDLED'
        );
        await logAttempt({
          paymentId: payment.payment_id,
          attempt,
          outcome: 'RECOVERED',
          reason: payment.failure_reason,
        });

        log.ok(
          `${payment.order_id} berhasil ditangani di percobaan ke-${attempt} -> ACK`
        );
        channel.ack(msg);
      } catch (err) {
        // ---------- gagal ----------
        if (retryCount >= MAX_RETRY) {
          // Sudah kehabisan jatah retry -> kirim ke Dead Letter Exchange.
          const headers = {
            'x-dead-reason': err.message,
            'x-retry-attempts': attempt,
            'x-original-routing-key': msg.fields.routingKey,
            'x-dead-lettered-at': new Date().toISOString(),
          };

          channel.publish(EXCHANGES.DLX, ROUTING_KEYS.FAILED, msg.content, {
            persistent: true,
            contentType: 'application/json',
            messageId: payment.payment_id,
            headers,
          });

          await markPaymentFailed(
            { ...payment, routing_key: msg.fields.routingKey },
            'DEAD_LETTERED'
          );
          await logAttempt({
            paymentId: payment.payment_id,
            attempt,
            outcome: 'DEAD_LETTERED',
            reason: err.message,
          });

          log.dead(
            `${payment.order_id} gagal ${attempt}x -> dikirim ke "${QUEUES.DLQ}"`
          );

          // ACK pesan asli: tanggung jawabnya sudah pindah ke DLQ.
          channel.ack(msg);
        } else {
          // Masih ada jatah retry.
          await logAttempt({
            paymentId: payment.payment_id,
            attempt,
            outcome: 'RETRY_SCHEDULED',
            reason: err.message,
          });

          log.warn(
            `${payment.order_id} gagal (${err.message}) -> ` +
              `NACK, dicoba lagi dalam ${RETRY_TTL_MS / 1000}s ` +
              `(sisa retry ${MAX_RETRY - retryCount})`
          );

          // requeue = false -> pesan TIDAK kembali ke antrean ini secara
          // langsung, melainkan dilempar ke payments.retry oleh RabbitMQ.
          channel.nack(msg, false, false);
        }
      }
    },
    { noAck: false } // <-- MANUAL ACK
  );

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
