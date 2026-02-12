-- ========================================================================
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ========================================================================
-- for final bars created by bar_builder and saved by bar_store
CREATE TABLE public.bars (
    instrument_id           BIGINT        NOT NULL,
    timeframe               TEXT          NOT NULL,
    bar_ts                  TIMESTAMPTZ   NOT NULL,

    open                    NUMERIC(12,2) NOT NULL,
    high                    NUMERIC(12,2) NOT NULL,
    low                     NUMERIC(12,2) NOT NULL,
    close                   NUMERIC(12,2) NOT NULL,

    volume                  BIGINT        NOT NULL,

    oi                      BIGINT        NULL,
    oi_change               BIGINT        NULL,

    PRIMARY KEY (instrument_id, timeframe, bar_ts)
);

-- Promote to hypertable on bar_ts
SELECT create_hypertable(
    relation                => 'public.bars',
    time_column_name        => 'bar_ts',
    partitioning_column     => 'instrument_id',
    number_partitions       => 4,    -- tune for CPU cores
    chunk_time_interval     => INTERVAL '1 day',
    if_not_exists           => TRUE,
    create_default_indexes  => FALSE
);

-- Covering index for your loader (instrument_id + timeframe filter, bar_ts DESC)
CREATE INDEX idx_bars_cover_instrument_tf_ts
  ON public.bars (instrument_id, timeframe, bar_ts DESC)
  INCLUDE (open, high, low, close, volume, oi, oi_change);
  
-- ========================================================================
-- for bars acquired from external sources - Kite
CREATE TABLE public.bars_external (
    instrument_id           BIGINT        NOT NULL,
    timeframe               TEXT          NOT NULL,
    bar_ts                  TIMESTAMPTZ   NOT NULL,

    open                    NUMERIC(12,2) NOT NULL,
    high                    NUMERIC(12,2) NOT NULL,
    low                     NUMERIC(12,2) NOT NULL,
    close                   NUMERIC(12,2) NOT NULL,

    volume                  BIGINT        NOT NULL,

    oi                      BIGINT        NULL,
    oi_change               BIGINT        NULL,

    PRIMARY KEY (instrument_id, timeframe, bar_ts)
);

-- Promote to hypertable on bar_ts
SELECT create_hypertable(
    relation            => 'public.bars_external',
    time_column_name    => 'bar_ts',
    partitioning_column => 'instrument_id',
    number_partitions   => 4,    -- tune for CPU cores
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists       => TRUE,
    create_default_indexes => FALSE
);


-- Covering index for your loader (instrument_id + timeframe filter, bar_ts DESC)
CREATE INDEX idx_bars_external_cover_instrument_tf_ts
  ON public.bars_external (instrument_id, timeframe, bar_ts DESC)
  INCLUDE (open, high, low, close, volume, oi, oi_change);

-- ========================================================================

-- docker exec -i artham_00_postgres \
--      psql -U postgres -d artham < ./postgres-init/003_create_bars.sql

-- ========================================================================
