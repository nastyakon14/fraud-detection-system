-- проскроенные транзации для хранения в базу данных
CREATE TABLE IF NOT EXISTS transaction_scores (
    id BIGSERIAL PRIMARY KEY,
    transaction_id TEXT NOT NULL,
    score DOUBLE PRECISION NOT NULL,
    fraud_flag INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);