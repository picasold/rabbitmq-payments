// ---------------------------------------------------------------------------
// TOPOLOGY
// File ini yang membuat seluruh "pipa" di RabbitMQ: exchange, queue, binding.
// Dipanggil oleh producer maupun consumer, jadi siapa pun yang start duluan
// akan membentuk topologi yang sama persis (operasi declare bersifat idempoten).
//
//   Producer
//      |  routing key: payment.success / payment.failed
//      v
//  [payments.event]  (TOPIC EXCHANGE)
//      |                         |
//      | payment.success         | payment.failed
//      v                         v
//  payments.success.q        payments.failed.q  <-------------------+
//   (Consumer A)              (Consumer B)                          |
//                                 | pesan di-NACK (gagal diproses)  |
//                                 v                                 |
//                           [payments.retry]                        |
//                                 |                                 |
//                                 v                                 |
//                      payments.failed.retry.q                      |
//                      (TTL 5 detik, lalu dilempar balik) ----------+
//
//   Sudah retry 2x dan masih gagal -> Consumer B mengirim ke:
//                           [payments.dlx]  ->  queue payments.dlx  (DLQ)
// ---------------------------------------------------------------------------

import amqp from 'amqplib';
import {
  AMQP_URL,
  EXCHANGES,
  QUEUES,
  ROUTING_KEYS,
  RETRY_TTL_MS,
} from './config.js';
import { createLogger } from './logger.js';

const log = createLogger('topology', 'blue');

/**
 * Membuka koneksi ke RabbitMQ dengan retry.
 * Saat `docker compose up`, aplikasi sering menyala sebelum RabbitMQ siap,
 * jadi kita coba berulang kali, bukan langsung crash.
 */
export async function connectWithRetry(attempts = 30, delayMs = 2000) {
  for (let i = 1; i <= attempts; i++) {
    try {
      const connection = await amqp.connect(AMQP_URL);
      log.ok(`terhubung ke RabbitMQ (percobaan ke-${i})`);

      // Kalau broker mati atau koneksi putus, proses sengaja dihentikan.
      // Di docker-compose service-nya memakai `restart: unless-stopped`,
      // jadi container akan dinyalakan ulang dan menyambung lagi sendiri.
      connection.on('error', (err) => {
        log.error(`koneksi RabbitMQ bermasalah: ${err.message}`);
      });
      connection.on('close', () => {
        if (process.env.ALLOW_CLOSE === '1') return; // penutupan normal
        log.warn('koneksi RabbitMQ tertutup -> proses berhenti agar di-restart');
        process.exit(1);
      });

      return connection;
    } catch (err) {
      log.warn(
        `RabbitMQ belum siap (percobaan ${i}/${attempts}): ${err.message}`
      );
      if (i === attempts) throw err;
      await new Promise((r) => setTimeout(r, delayMs));
    }
  }
}

/**
 * Membuat semua exchange, queue, dan binding.
 * Aman dipanggil berkali-kali.
 */
export async function assertTopology(channel) {
  // ------------------------------------------------------------------
  // 1. EXCHANGE
  // ------------------------------------------------------------------
  // durable: true  -> exchange tetap ada walau broker di-restart
  await channel.assertExchange(EXCHANGES.MAIN, 'topic', { durable: true });
  await channel.assertExchange(EXCHANGES.RETRY, 'topic', { durable: true });
  await channel.assertExchange(EXCHANGES.DLX, 'topic', { durable: true });

  // ------------------------------------------------------------------
  // 2. QUEUE CONSUMER A  -- jalur sukses
  // ------------------------------------------------------------------
  await channel.assertQueue(QUEUES.SUCCESS, { durable: true });
  await channel.bindQueue(QUEUES.SUCCESS, EXCHANGES.MAIN, ROUTING_KEYS.SUCCESS);

  // ------------------------------------------------------------------
  // 3. QUEUE CONSUMER B  -- jalur gagal
  // ------------------------------------------------------------------
  // Kalau pesan di sini di-NACK (requeue=false), RabbitMQ otomatis
  // melemparnya ke exchange payments.retry.
  await channel.assertQueue(QUEUES.FAILED, {
    durable: true,
    arguments: {
      'x-dead-letter-exchange': EXCHANGES.RETRY,
      'x-dead-letter-routing-key': ROUTING_KEYS.FAILED,
    },
  });
  await channel.bindQueue(QUEUES.FAILED, EXCHANGES.MAIN, ROUTING_KEYS.FAILED);

  // ------------------------------------------------------------------
  // 4. QUEUE RETRY  -- "ruang tunggu" berdurasi TTL
  // ------------------------------------------------------------------
  // Queue ini sengaja TIDAK punya consumer. Pesan hanya mengendap di sini
  // selama x-message-ttl. Saat TTL habis, pesan dianggap "expired" dan
  // RabbitMQ melemparnya kembali ke payments.event dengan routing key
  // payment.failed -> otomatis masuk lagi ke payments.failed.q.
  await channel.assertQueue(QUEUES.RETRY, {
    durable: true,
    arguments: {
      'x-message-ttl': RETRY_TTL_MS,
      'x-dead-letter-exchange': EXCHANGES.MAIN,
      'x-dead-letter-routing-key': ROUTING_KEYS.FAILED,
    },
  });
  await channel.bindQueue(QUEUES.RETRY, EXCHANGES.RETRY, ROUTING_KEYS.FAILED);

  // ------------------------------------------------------------------
  // 5. DEAD LETTER QUEUE  -- tujuan akhir
  // ------------------------------------------------------------------
  // Tidak ada TTL dan tidak ada dead-letter lagi: pesan berhenti di sini
  // sampai diperiksa manusia.
  await channel.assertQueue(QUEUES.DLQ, { durable: true });
  await channel.bindQueue(QUEUES.DLQ, EXCHANGES.DLX, '#');

  log.ok('exchange, queue, dan binding siap');
}

/** Helper: buka koneksi + channel sekaligus deklarasi topologi. */
export async function setup() {
  const connection = await connectWithRetry();
  const channel = await connection.createChannel();
  await assertTopology(channel);
  return { connection, channel };
}

// Kalau file ini dijalankan langsung (`npm run topology`), cukup buat
// topologinya lalu keluar. Berguna untuk menyiapkan Management UI.
if (import.meta.url === `file://${process.argv[1]}`) {
  const { connection } = await setup();
  await connection.close();
  log.ok('topology selesai dibuat, koneksi ditutup');
  process.exit(0);
}
