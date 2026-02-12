-- ========================================================================
-- Instruments table for storing instrument/security data from Zerodha Kite
-- Includes equity, derivatives, options, and other security types

CREATE TABLE IF NOT EXISTS public.instruments (
    instrument_id BIGINT,
    exchange TEXT NOT NULL,
    segment TEXT NOT NULL,
    trading_symbol TEXT NOT NULL,
    underlying_instrument_id BIGINT,
    underlying_trading_symbol TEXT,
    timezone TEXT NOT NULL,
    instrument_type TEXT NOT NULL,
    description TEXT,
    isin TEXT,
    strike NUMERIC(10,2),
    expiry DATE,
    lot_size NUMERIC(10,0),
    tick_size NUMERIC(10,4),
    display_order INTEGER NOT NULL DEFAULT 0,
    expired BOOLEAN NOT NULL,

    PRIMARY KEY (instrument_id)
);

-- Enable extension for trigram indexes (for ILIKE searches)
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Equality/uniqueness and targeted lookups
CREATE INDEX IF NOT EXISTS idx_instruments_exchange_trading_symbol_unique
    ON public.instruments(exchange, trading_symbol);

CREATE INDEX IF NOT EXISTS idx_instruments_trading_symbol
    ON public.instruments(trading_symbol);

CREATE INDEX IF NOT EXISTS idx_instruments_underlying_id
    ON public.instruments(underlying_instrument_id);

CREATE INDEX IF NOT EXISTS idx_instruments_isin
    ON public.instruments(isin);

-- Ordering support (may assist certain queries that filter + order)
CREATE INDEX IF NOT EXISTS idx_instruments_display_order
    ON public.instruments(display_order);

-- Composite indexes retained for derivatives and ordering by expiry/strike
CREATE INDEX IF NOT EXISTS idx_instruments_deriv_lookup
    ON public.instruments(underlying_trading_symbol, exchange);

CREATE INDEX IF NOT EXISTS idx_instruments_expiry_strike
    ON public.instruments(expiry, strike);

-- Case-insensitive contains search support (ILIKE '%...%')
CREATE INDEX IF NOT EXISTS idx_instruments_trading_symbol_trgm
    ON public.instruments USING gin (lower(trading_symbol) gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_instruments_underlying_symbol_trgm
    ON public.instruments USING gin (lower(underlying_trading_symbol) gin_trgm_ops)
    WHERE underlying_trading_symbol IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_instruments_exchange_trgm
    ON public.instruments USING gin (lower(exchange) gin_trgm_ops);

-- ========================================================================

-- docker exec -i artham_00_postgres \
--      psql -U postgres -d artham < ./postgres-init/001_create_instruments.sql

-- ========================================================================

