// ---------------------------------------------------------------------------
// PRODUCER
// Mengirim event pembayaran ke TOPIC EXCHANGE `payments.event`
// dengan dua routing key berbeda:
//
//    payment.success  -> ditangkap Consumer A
//    payment.failed   -> ditangkap Consumer B
//
// Cara pakai:
//    node src/producer.js                      (paket demo lengkap)
//    node src/producer.js --success 3
//    node src/producer.js --failed 2 --scenario permanent
// ---------------------------------------------------------------------------

process.env.ALLOW_CLOSE = '1';
import { randomUUID } from 'node:crypto';
import { EXCHANGES, ROUTING_KEYS } from './config.js';
import { connectWithRetry, assertTopology } from './topology.js';
import { createLogger } from './logger.js';

const log = createLogger('producer', 'cyan');

// --- baca argumen baris perintah -------------------------------------------
function parseArgs(argv) {
  const args = { success: null, failed: null, scenario: null };
  for (let i = 2; i < argv.length; i++) {
    const key = argv[i].replace(/^--/, '');
    if (key in args) args[key] = argv[i + 1];
  }
  return args;
}

const BANKS = ['BCA', 'Mandiri', 'BNI', 'BRI', 'QRIS', 'GoPay'];
const FAIL_REASONS = [
  'INSUFFICIENT_FUNDS',
  'CARD_DECLINED',
  'GATEWAY_TIMEOUT',
  'INVALID_CVV',
];

const rand = (arr) => arr[Math.floor(Math.random() * arr.length)];

function buildPayment(kind, scenario) {
  const id = randomUUID();
  return {
    payment_id: id,
    order_id: 'ORD-' + id.slice(0, 8).toUpperCase(),
    amount: Number((Math.random() * 900_000 + 25_000).toFixed(2)),
    currency: 'IDR',
    method: rand(BANKS),
    created_at: new Date().toISOString(),
    ...(kind === 'failed'
      ? {
          failure_reason: rand(FAIL_REASONS),
          // Dipakai Consumer B untuk mensimulasikan hasil penanganan:
          //   transient -> sempat gagal, lalu pulih saat retry
          //   permanent -> gagal terus sampai masuk DLQ
          scenario: scenario || 'permanent',
        }
      : {}),
  };
}

async function publish(channel, routingKey, payment) {
  const body = Buffer.from(JSON.stringify(payment));

  // deliveryMode persistent + queue durable = pesan selamat walau broker restart
  const ok = channel.publish(EXCHANGES.MAIN, routingKey, body, {
    persistent: true,
    contentType: 'application/json',
    messageId: payment.payment_id,
    timestamp: Date.now(),
    headers: { 'x-origin': 'producer', 'x-routing-key': routingKey },
  });

  if (!ok) await new Promise((r) => channel.once('drain', r));

  // Publisher confirm: menunggu broker benar-benar menerima pesan.
  await channel.waitForConfirms();

  const label = routingKey === ROUTING_KEYS.SUCCESS ? 'SUCCESS' : 'FAILED ';
  log.info(
    `-> ${routingKey.padEnd(16)} ${label} ${payment.order_id}  ` +
      `Rp${payment.amount.toLocaleString('id-ID')}` +
      (payment.scenario ? `  [scenario=${payment.scenario}]` : '')
  );
}

async function main() {
  const args = parseArgs(process.argv);
  const connection = await connectWithRetry();

  // createConfirmChannel: channel yang mendukung publisher confirm
  const channel = await connection.createConfirmChannel();
  await assertTopology(channel);

  // Mode manual: jumlah ditentukan lewat argumen
  if (args.success !== null || args.failed !== null) {
    const nSuccess = Number(args.success || 0);
    const nFailed = Number(args.failed || 0);

    for (let i = 0; i < nSuccess; i++) {
      await publish(channel, ROUTING_KEYS.SUCCESS, buildPayment('success'));
    }
    for (let i = 0; i < nFailed; i++) {
      await publish(
        channel,
        ROUTING_KEYS.FAILED,
        buildPayment('failed', args.scenario)
      );
    }
  } else {
    // Mode demo bawaan: menunjukkan ketiga jalur sekaligus
    log.info('mode demo: 3 sukses, 1 gagal-sementara, 1 gagal-permanen');
    for (let i = 0; i < 3; i++) {
      await publish(channel, ROUTING_KEYS.SUCCESS, buildPayment('success'));
    }
    await publish(
      channel,
      ROUTING_KEYS.FAILED,
      buildPayment('failed', 'transient')
    );
    await publish(
      channel,
      ROUTING_KEYS.FAILED,
      buildPayment('failed', 'permanent')
    );
  }

  log.ok('semua pesan terkirim dan dikonfirmasi broker');
  await channel.close();
  await connection.close();
  process.exit(0);
}

main().catch((err) => {
  log.error(err);
  process.exit(1);
});
