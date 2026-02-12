-- ============================================================
-- Verbatim market ticks (full Kite payload, depth included)
-- ============================================================

CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS public.ticks (
    -- Identity
    instrument_id          BIGINT NOT NULL,
    instrument_type        TEXT NOT NULL,

    -- Time (absolute instants)
    exchange_ts            TIMESTAMPTZ NOT NULL,
    ingest_ts              TIMESTAMPTZ NOT NULL,
    db_ts                  TIMESTAMPTZ NOT NULL,
    last_trade_time        TIMESTAMPTZ NOT NULL,

    -- Market state
    tradable               BOOLEAN NOT NULL,

    -- Price/volume data
    last_price             NUMERIC(12,2) NOT NULL,
    last_traded_quantity   INTEGER,
    average_traded_price   NUMERIC(12,2),
    volume_traded          BIGINT,
    total_buy_quantity     BIGINT,
    total_sell_quantity    BIGINT,

    -- OHLC snapshot from vendor (kept verbatim)
    ohlc_open              NUMERIC(10,2),
    ohlc_high              NUMERIC(10,2),
    ohlc_low               NUMERIC(10,2),
    ohlc_close             NUMERIC(10,2),
    change                 NUMERIC(8,4),

    -- Derivatives
    oi                     BIGINT,
    oi_day_high            BIGINT,
    oi_day_low             BIGINT,

    -- ========================================================
    -- Depth - Buy side (5 levels)
    -- ========================================================
    depth_buy_0_quantity   NUMERIC(12,0),
    depth_buy_0_price      NUMERIC(12,2),
    depth_buy_0_orders     NUMERIC(12,0),

    depth_buy_1_quantity   NUMERIC(12,0),
    depth_buy_1_price      NUMERIC(12,2),
    depth_buy_1_orders     NUMERIC(12,0),

    depth_buy_2_quantity   NUMERIC(12,0),
    depth_buy_2_price      NUMERIC(12,2),
    depth_buy_2_orders     NUMERIC(12,0),

    depth_buy_3_quantity   NUMERIC(12,0),
    depth_buy_3_price      NUMERIC(12,2),
    depth_buy_3_orders     NUMERIC(12,0),

    depth_buy_4_quantity   NUMERIC(12,0),
    depth_buy_4_price      NUMERIC(12,2),
    depth_buy_4_orders     NUMERIC(12,0),

    -- ========================================================
    -- Depth - Sell side (5 levels)
    -- ========================================================
    depth_sell_0_quantity  NUMERIC(12,0),
    depth_sell_0_price     NUMERIC(12,2),
    depth_sell_0_orders    NUMERIC(12,0),

    depth_sell_1_quantity  NUMERIC(12,0),
    depth_sell_1_price     NUMERIC(12,2),
    depth_sell_1_orders    NUMERIC(12,0),

    depth_sell_2_quantity  NUMERIC(12,0),
    depth_sell_2_price     NUMERIC(12,2),
    depth_sell_2_orders    NUMERIC(12,0),

    depth_sell_3_quantity  NUMERIC(12,0),
    depth_sell_3_price     NUMERIC(12,2),
    depth_sell_3_orders    NUMERIC(12,0),
    
    depth_sell_4_quantity  NUMERIC(12,0),
    depth_sell_4_price     NUMERIC(12,2),
    depth_sell_4_orders    NUMERIC(12,0)
);

-- Promote to hypertable on exchange_ts
SELECT create_hypertable(
    relation            => 'public.ticks',
    time_column_name    => 'exchange_ts',
    chunk_time_interval => INTERVAL '1 hour'
);

-- Helpful index for lookups by instrument and time
CREATE INDEX IF NOT EXISTS idx_ticks_instrument_ts ON public.ticks (instrument_id, exchange_ts DESC);


-- ========================================================================

-- docker exec -i artham_00_postgres \
--      psql -U postgres -d artham < ./postgres-init/002_create_ticks.sql

-- ========================================================================

