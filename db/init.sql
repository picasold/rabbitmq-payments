-- ---------------------------------------------------------------------------
-- Skema database. Dijalankan otomatis oleh container postgres saat pertama
-- kali dibuat (lihat volume /docker-entrypoint-initdb.d di docker-compose.yml)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS payments (
  payment_id   TEXT PRIMARY KEY,
  order_id     TEXT        NOT NULL,
  amount       NUMERIC(14,2) NOT NULL,
  currency     TEXT        NOT NULL DEFAULT 'IDR',
  -- PAID            : diproses Consumer A
  -- FAILED_HANDLED  : gagal, tapi berhasil ditangani Consumer B (mungkin setelah retry)
  -- DEAD_LETTERED   : gagal terus sampai masuk DLQ
  status       TEXT        NOT NULL,
  routing_key  TEXT        NOT NULL,
  payload      JSONB,
  processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Riwayat tiap percobaan penanganan pembayaran gagal (bukti retry bekerja).
CREATE TABLE IF NOT EXISTS payment_failures (
  id         BIGSERIAL PRIMARY KEY,
  payment_id TEXT        NOT NULL,
  attempt    INT         NOT NULL,
  outcome    TEXT        NOT NULL,  -- RETRY_SCHEDULED | RECOVERED | DEAD_LETTERED
  reason     TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_payments_status  ON payments (status);
CREATE INDEX IF NOT EXISTS idx_failures_payment ON payment_failures (payment_id);
