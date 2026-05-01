-- =============================================================
--  StockIQ – TimescaleDB + PostgreSQL Schema
--  Run once on a fresh database.
--  Requires: TimescaleDB extension installed.
-- =============================================================

CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS pg_trgm;    -- trigram index for full-text search

-- ─────────────────────────────────────────────────────────
--  Reference / metadata tables
-- ─────────────────────────────────────────────────────────
CREATE TABLE instruments (
    symbol      TEXT        NOT NULL,
    exchange    TEXT        NOT NULL CHECK (exchange IN ('NSE','BSE')),
    name        TEXT        NOT NULL DEFAULT '',
    sector      TEXT        NOT NULL DEFAULT '',
    industry    TEXT        NOT NULL DEFAULT '',
    isin        TEXT,
    lot_size    INT         DEFAULT 1,
    tick_size   NUMERIC     DEFAULT 0.05,
    is_active   BOOLEAN     DEFAULT TRUE,
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (symbol, exchange)
);

CREATE INDEX idx_instruments_name_trgm
    ON instruments USING gin (name gin_trgm_ops);
CREATE INDEX idx_instruments_symbol_trgm
    ON instruments USING gin (symbol gin_trgm_ops);


-- ─────────────────────────────────────────────────────────
--  Tick hypertable  (raw ticks from Kite / Upstox)
-- ─────────────────────────────────────────────────────────
CREATE TABLE ticks (
    time            TIMESTAMPTZ NOT NULL,
    symbol          TEXT        NOT NULL,
    exchange        TEXT        NOT NULL,
    ltp             NUMERIC     NOT NULL,
    open            NUMERIC,
    high            NUMERIC,
    low             NUMERIC,
    close           NUMERIC,
    volume          BIGINT,
    change_pct_day  NUMERIC
);

SELECT create_hypertable('ticks', 'time', chunk_time_interval => INTERVAL '1 day');

CREATE INDEX ON ticks (symbol, exchange, time DESC);

-- Retention: keep raw ticks for 7 days; aggregates persist longer
SELECT add_retention_policy('ticks', INTERVAL '7 days');


-- ─────────────────────────────────────────────────────────
--  Continuous aggregates (OHLCV rollups)
-- ─────────────────────────────────────────────────────────

-- 1-minute bars
CREATE MATERIALIZED VIEW ohlcv_1m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', time)  AS time_bucket,
    symbol, exchange,
    FIRST(ltp, time)               AS open,
    MAX(ltp)                       AS high,
    MIN(ltp)                       AS low,
    LAST(ltp, time)                AS close,
    SUM(volume)                    AS volume
FROM ticks
GROUP BY time_bucket, symbol, exchange
WITH NO DATA;

SELECT add_continuous_aggregate_policy('ohlcv_1m',
    start_offset => INTERVAL '2 hours',
    end_offset   => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 minute');

SELECT add_retention_policy('ohlcv_1m', INTERVAL '30 days');


-- 5-minute bars
CREATE MATERIALIZED VIEW ohlcv_5m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('5 minutes', time) AS time_bucket,
    symbol, exchange,
    FIRST(ltp, time)               AS open,
    MAX(ltp)                       AS high,
    MIN(ltp)                       AS low,
    LAST(ltp, time)                AS close,
    SUM(volume)                    AS volume
FROM ticks
GROUP BY time_bucket, symbol, exchange
WITH NO DATA;

SELECT add_continuous_aggregate_policy('ohlcv_5m',
    start_offset => INTERVAL '1 day',
    end_offset   => INTERVAL '5 minutes',
    schedule_interval => INTERVAL '5 minutes');

SELECT add_retention_policy('ohlcv_5m', INTERVAL '90 days');


-- 15-minute bars
CREATE MATERIALIZED VIEW ohlcv_15m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('15 minutes', time) AS time_bucket,
    symbol, exchange,
    FIRST(ltp, time)                AS open,
    MAX(ltp)                        AS high,
    MIN(ltp)                        AS low,
    LAST(ltp, time)                 AS close,
    SUM(volume)                     AS volume
FROM ticks
GROUP BY time_bucket, symbol, exchange
WITH NO DATA;

SELECT add_continuous_aggregate_policy('ohlcv_15m',
    start_offset => INTERVAL '3 days',
    end_offset   => INTERVAL '15 minutes',
    schedule_interval => INTERVAL '15 minutes');

SELECT add_retention_policy('ohlcv_15m', INTERVAL '180 days');


-- 1-hour bars
CREATE MATERIALIZED VIEW ohlcv_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time) AS time_bucket,
    symbol, exchange,
    FIRST(ltp, time)            AS open,
    MAX(ltp)                    AS high,
    MIN(ltp)                    AS low,
    LAST(ltp, time)             AS close,
    SUM(volume)                 AS volume
FROM ticks
GROUP BY time_bucket, symbol, exchange
WITH NO DATA;

SELECT add_continuous_aggregate_policy('ohlcv_1h',
    start_offset => INTERVAL '7 days',
    end_offset   => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');

SELECT add_retention_policy('ohlcv_1h', INTERVAL '2 years');


-- Daily bars
CREATE MATERIALIZED VIEW ohlcv_1d
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 day', time) AS time_bucket,
    symbol, exchange,
    FIRST(ltp, time)           AS open,
    MAX(ltp)                   AS high,
    MIN(ltp)                   AS low,
    LAST(ltp, time)            AS close,
    SUM(volume)                AS volume
FROM ticks
GROUP BY time_bucket, symbol, exchange
WITH NO DATA;

SELECT add_continuous_aggregate_policy('ohlcv_1d',
    start_offset => INTERVAL '30 days',
    end_offset   => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 day');

-- Keep daily bars forever (effectively)
SELECT add_retention_policy('ohlcv_1d', INTERVAL '10 years');


-- ─────────────────────────────────────────────────────────
--  Latest quotes materialised view
--  Refreshed every 5 seconds by a background job.
--  Used by the screener for fast filtered reads.
-- ─────────────────────────────────────────────────────────
CREATE MATERIALIZED VIEW latest_quotes AS
SELECT DISTINCT ON (symbol, exchange)
    t.symbol,
    t.exchange,
    i.name,
    i.sector,
    t.ltp,
    t.open,
    t.high,
    t.low,
    t.close,
    t.ltp  AS prev_close,    -- overridden by app layer with actual prev close
    t.volume,
    t.change_pct_day,
    0.0    AS change_pct_1m,
    0.0    AS change_pct_5m,
    0.0    AS change_pct_15m,
    -- Indicator columns (populated by background indicator job)
    NULL::NUMERIC AS rsi_14,
    NULL::NUMERIC AS macd,
    NULL::NUMERIC AS macd_signal,
    NULL::NUMERIC AS macd_hist,
    NULL::NUMERIC AS sma_20,
    NULL::NUMERIC AS sma_50,
    NULL::NUMERIC AS ema_20,
    NULL::NUMERIC AS atr_14,
    NULL::NUMERIC AS bb_upper,
    NULL::NUMERIC AS bb_lower,
    NULL::NUMERIC AS vol_stddev,
    NULL::NUMERIC AS rel_volume,
    NULL::NUMERIC AS slope_1d,
    NULL::NUMERIC AS slope_1w,
    t.time AS updated_at
FROM ticks t
LEFT JOIN instruments i USING (symbol, exchange)
ORDER BY symbol, exchange, time DESC
WITH NO DATA;

-- Partial indexes for common screener filters
CREATE INDEX idx_lq_exchange    ON latest_quotes (exchange);
CREATE INDEX idx_lq_ltp         ON latest_quotes (ltp);
CREATE INDEX idx_lq_change_day  ON latest_quotes (change_pct_day DESC NULLS LAST);
CREATE INDEX idx_lq_volume      ON latest_quotes (volume DESC NULLS LAST);
CREATE INDEX idx_lq_rsi         ON latest_quotes (rsi_14 NULLS LAST);


-- ─────────────────────────────────────────────────────────
--  News table
-- ─────────────────────────────────────────────────────────
CREATE TABLE news_articles (
    id          TEXT        PRIMARY KEY,
    headline    TEXT        NOT NULL,
    source      TEXT        NOT NULL,
    source_type TEXT        NOT NULL,
    url         TEXT        NOT NULL,
    published_at TIMESTAMPTZ NOT NULL,
    sentiment   TEXT        NOT NULL DEFAULT 'neutral',
    symbols     TEXT[]      DEFAULT '{}',
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_news_published ON news_articles (published_at DESC);
CREATE INDEX idx_news_source    ON news_articles (source_type);


-- ─────────────────────────────────────────────────────────
--  User-facing tables (auth, watchlists, saved screens)
-- ─────────────────────────────────────────────────────────
CREATE TABLE users (
    id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    email       TEXT        UNIQUE NOT NULL,
    hashed_pw   TEXT        NOT NULL,
    display_name TEXT       DEFAULT '',
    is_active   BOOLEAN     DEFAULT TRUE,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE watchlists (
    id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id     UUID        REFERENCES users(id) ON DELETE CASCADE,
    name        TEXT        NOT NULL DEFAULT 'Default',
    symbols     JSONB       DEFAULT '[]',
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE saved_screens (
    id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id     UUID        REFERENCES users(id) ON DELETE CASCADE,
    name        TEXT        NOT NULL,
    filters     JSONB       NOT NULL,     -- serialised ScreenerFilter
    created_at  TIMESTAMPTZ DEFAULT NOW()
);


-- ─────────────────────────────────────────────────────────
--  Price snapshots table (for rolling % change computation)
-- ─────────────────────────────────────────────────────────
CREATE TABLE price_snapshots (
    time        TIMESTAMPTZ NOT NULL,
    symbol      TEXT        NOT NULL,
    exchange    TEXT        NOT NULL,
    ltp         NUMERIC     NOT NULL,
    window_min  INT         NOT NULL,     -- 1, 5, or 15
    PRIMARY KEY (symbol, exchange, window_min, time)
);

SELECT create_hypertable(
    'price_snapshots', 'time',
    chunk_time_interval => INTERVAL '1 hour'
);

SELECT add_retention_policy('price_snapshots', INTERVAL '1 day');