// ---------------------------------------------------------------------------
// Konfigurasi terpusat: nama exchange, queue, routing key, dan parameter retry.
// Semua file lain mengimpor dari sini supaya tidak ada nama yang salah ketik.
// ---------------------------------------------------------------------------

export const AMQP_URL =
  process.env.AMQP_URL || 'amqp://admin:admin123@localhost:5672';

export const DATABASE_URL =
  process.env.DATABASE_URL ||
  'postgres://payments:payments123@localhost:5432/payments';

// --- Exchange --------------------------------------------------------------
export const EXCHANGES = {
  // Exchange utama. Tipe TOPIC sesuai tugas.
  MAIN: 'payments.event',
  // Exchange penampung pesan yang sedang menunggu jeda retry.
  RETRY: 'payments.retry',
  // Dead Letter Exchange: tujuan akhir pesan yang gagal terus.
  DLX: 'payments.dlx',
};

// --- Routing key -----------------------------------------------------------
export const ROUTING_KEYS = {
  SUCCESS: 'payment.success',
  FAILED: 'payment.failed',
};

// --- Queue -----------------------------------------------------------------
export const QUEUES = {
  // Consumer A
  SUCCESS: 'payments.success.q',
  // Consumer B
  FAILED: 'payments.failed.q',
  // Ruang tunggu retry (pesan "diparkir" selama TTL lalu dilempar balik)
  RETRY: 'payments.failed.retry.q',
  // Dead Letter Queue. Namanya sengaja 'payments.dlx' sesuai permintaan tugas.
  DLQ: 'payments.dlx',
};

// --- Parameter retry -------------------------------------------------------
// Pesan gagal dicoba ulang MAX_RETRY kali dengan jeda RETRY_TTL_MS tiap kali.
// Setelah itu baru masuk DLQ.
export const RETRY_TTL_MS = Number(process.env.RETRY_TTL_MS || 5000);
export const MAX_RETRY = Number(process.env.MAX_RETRY || 2);

// Berapa pesan yang boleh dipegang consumer sebelum ACK (1 = adil & aman).
export const PREFETCH = Number(process.env.PREFETCH || 1);
