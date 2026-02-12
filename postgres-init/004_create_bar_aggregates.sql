-- -- ============================================================
-- -- DROP ALL MATERIALIZED VIEWS (for idempotency)
-- -- ============================================================
-- DROP MATERIALIZED VIEW IF EXISTS bars_3m CASCADE;
-- DROP MATERIALIZED VIEW IF EXISTS bars_5m CASCADE;
-- DROP MATERIALIZED VIEW IF EXISTS bars_15m CASCADE;
-- DROP MATERIALIZED VIEW IF EXISTS bars_25m CASCADE;
-- DROP MATERIALIZED VIEW IF EXISTS bars_75m CASCADE;
-- DROP MATERIALIZED VIEW IF EXISTS bars_125m CASCADE;
-- DROP MATERIALIZED VIEW IF EXISTS bars_1w CASCADE;
-- DROP MATERIALIZED VIEW IF EXISTS bars_1mth CASCADE;
-- DROP MATERIALIZED VIEW IF EXISTS bars_3mth CASCADE;
-- DROP MATERIALIZED VIEW IF EXISTS bars_6mth CASCADE;
-- DROP MATERIALIZED VIEW IF EXISTS bars_1y CASCADE;
-- DROP MATERIALIZED VIEW IF EXISTS bars_external_3m CASCADE;
-- DROP MATERIALIZED VIEW IF EXISTS bars_external_5m CASCADE;
-- DROP MATERIALIZED VIEW IF EXISTS bars_external_15m CASCADE;
-- DROP MATERIALIZED VIEW IF EXISTS bars_external_25m CASCADE;
-- DROP MATERIALIZED VIEW IF EXISTS bars_external_75m CASCADE;
-- DROP MATERIALIZED VIEW IF EXISTS bars_external_125m CASCADE;
-- DROP MATERIALIZED VIEW IF EXISTS bars_external_1w CASCADE;
-- DROP MATERIALIZED VIEW IF EXISTS bars_external_1mth CASCADE;
-- DROP MATERIALIZED VIEW IF EXISTS bars_external_3mth CASCADE;
-- DROP MATERIALIZED VIEW IF EXISTS bars_external_6mth CASCADE;
-- DROP MATERIALIZED VIEW IF EXISTS bars_external_1y CASCADE;
-- -- ============================================================
-- -- ========================================================================
-- -- CREATE EXTENSION IF NOT EXISTS timescaledb;

-- -- ============================================================
-- -- Continuous aggregates for resampled STANDARD_BAR timeframes
-- -- ============================================================

-- -- ============================================================
-- -- For bars (contains 1m and 1D) resampling
-- -- ============================================================

-- -- 3m
-- CREATE MATERIALIZED VIEW bars_3m
-- WITH (timescaledb.continuous) AS
-- SELECT
--     instrument_id,
--     '3m' AS timeframe,
--     time_bucket('3 minutes', bar_ts) AS bar_ts,

--     first(open, bar_ts) AS open,
--     max(high)           AS high,
--     min(low)            AS low,
--     last(close, bar_ts) AS close,
--     sum(volume)         AS volume,
--     last(oi, bar_ts)    AS oi,
--     sum(oi_change)      AS oi_change
-- FROM bars
-- WHERE timeframe = '1m'
-- GROUP BY instrument_id, time_bucket('3 minutes', bar_ts)
-- WITH NO DATA;

-- -- 5m
-- CREATE MATERIALIZED VIEW bars_5m
-- WITH (timescaledb.continuous) AS
-- SELECT
--     instrument_id,
--     '5m' AS timeframe,
--     time_bucket('5 minutes', bar_ts) AS bar_ts,

--     first(open, bar_ts) AS open,
--     max(high)           AS high,
--     min(low)            AS low,
--     last(close, bar_ts) AS close,
--     sum(volume)         AS volume,
--     last(oi, bar_ts)    AS oi,
--     sum(oi_change)      AS oi_change
-- FROM bars
-- WHERE timeframe = '1m'
-- GROUP BY instrument_id, time_bucket('5 minutes', bar_ts)
-- WITH NO DATA;

-- -- 15m
-- CREATE MATERIALIZED VIEW bars_15m
-- WITH (timescaledb.continuous) AS
-- SELECT
--     instrument_id,
--     '15m' AS timeframe,
--     time_bucket('15 minutes', bar_ts) AS bar_ts,

--     first(open, bar_ts) AS open,
--     max(high)           AS high,
--     min(low)            AS low,
--     last(close, bar_ts) AS close,
--     sum(volume)         AS volume,
--     last(oi, bar_ts)    AS oi,
--     sum(oi_change)      AS oi_change
-- FROM bars
-- WHERE timeframe = '1m'
-- GROUP BY instrument_id, time_bucket('15 minutes', bar_ts)
-- WITH NO DATA;

-- -- 25m
-- CREATE MATERIALIZED VIEW bars_25m
-- WITH (timescaledb.continuous) AS
-- SELECT
--     instrument_id,
--     '25m' AS timeframe,
--     time_bucket('25 minutes', bar_ts) AS bar_ts,

--     first(open, bar_ts) AS open,
--     max(high)           AS high,
--     min(low)            AS low,
--     last(close, bar_ts) AS close,
--     sum(volume)         AS volume,
--     last(oi, bar_ts)    AS oi,
--     sum(oi_change)      AS oi_change
-- FROM bars
-- WHERE timeframe = '1m'
-- GROUP BY instrument_id, time_bucket('25 minutes', bar_ts)
-- WITH NO DATA;

-- -- 75m
-- CREATE MATERIALIZED VIEW bars_75m
-- WITH (timescaledb.continuous) AS
-- SELECT
--     instrument_id,
--     '75m' AS timeframe,
--     time_bucket('75 minutes', bar_ts) AS bar_ts,

--     first(open, bar_ts) AS open,
--     max(high)           AS high,
--     min(low)            AS low,
--     last(close, bar_ts) AS close,
--     sum(volume)         AS volume,
--     last(oi, bar_ts)    AS oi,
--     sum(oi_change)      AS oi_change
-- FROM bars
-- WHERE timeframe = '1m'
-- GROUP BY instrument_id, time_bucket('75 minutes', bar_ts)
-- WITH NO DATA;

-- -- 125m
-- CREATE MATERIALIZED VIEW bars_125m
-- WITH (timescaledb.continuous) AS
-- SELECT
--     instrument_id,
--     '125m' AS timeframe,
--     time_bucket('125 minutes', bar_ts) AS bar_ts,

--     first(open, bar_ts) AS open,
--     max(high)           AS high,
--     min(low)            AS low,
--     last(close, bar_ts) AS close,
--     sum(volume)         AS volume,
--     last(oi, bar_ts)    AS oi,
--     sum(oi_change)      AS oi_change
-- FROM bars
-- WHERE timeframe = '1m'
-- GROUP BY instrument_id, time_bucket('125 minutes', bar_ts)
-- WITH NO DATA;

-- -- ============================================================
-- -- DAILY → HIGHER TIMEFRAMES
-- -- ============================================================

-- -- 1W (ISO week, Monday-aligned)
-- CREATE MATERIALIZED VIEW bars_1w
-- WITH (timescaledb.continuous) AS
-- SELECT
--     instrument_id,
--     '1W' AS timeframe,
--     (time_bucket('1 week', bar_ts) + INTERVAL '1 week' - INTERVAL '1 day') AS bar_ts,

--     first(open, bar_ts) AS open,
--     max(high)           AS high,
--     min(low)            AS low,
--     last(close, bar_ts) AS close,
--     sum(volume)         AS volume,
--     last(oi, bar_ts)    AS oi,
--     sum(oi_change)      AS oi_change
-- FROM bars
-- WHERE timeframe = '1D'
-- GROUP BY instrument_id, time_bucket('1 week', bar_ts)
-- WITH NO DATA;

-- -- 1M (calendar month)
-- CREATE MATERIALIZED VIEW bars_1mth
-- WITH (timescaledb.continuous) AS
-- SELECT
--     instrument_id,
--     '1M' AS timeframe,
--     (time_bucket('1 month', bar_ts) + INTERVAL '1 month' - INTERVAL '1 day') AS bar_ts,

--     first(open, bar_ts) AS open,
--     max(high)           AS high,
--     min(low)            AS low,
--     last(close, bar_ts) AS close,
--     sum(volume)         AS volume,
--     last(oi, bar_ts)    AS oi,
--     sum(oi_change)      AS oi_change
-- FROM bars
-- WHERE timeframe = '1D'
-- GROUP BY instrument_id, time_bucket('1 month', bar_ts)
-- WITH NO DATA;

-- -- 3M (calendar quarter)
-- CREATE MATERIALIZED VIEW bars_3mth
-- WITH (timescaledb.continuous) AS
-- SELECT
--     instrument_id,
--     '3M' AS timeframe,
--     (time_bucket('3 months', bar_ts) + INTERVAL '3 months' - INTERVAL '1 day') AS bar_ts,

--     first(open, bar_ts) AS open,
--     max(high)           AS high,
--     min(low)            AS low,
--     last(close, bar_ts) AS close,
--     sum(volume)         AS volume,
--     last(oi, bar_ts)    AS oi,
--     sum(oi_change)      AS oi_change
-- FROM bars
-- WHERE timeframe = '1D'
-- GROUP BY instrument_id, time_bucket('3 months', bar_ts)
-- WITH NO DATA;

-- -- 6M (half-year: Jan–Jun, Jul–Dec)
-- CREATE MATERIALIZED VIEW bars_6mth
-- WITH (timescaledb.continuous) AS
-- SELECT
--     instrument_id,
--     '6M' AS timeframe,
--     (time_bucket('6 months', bar_ts) + INTERVAL '6 months' - INTERVAL '1 day') AS bar_ts,

--     first(open, bar_ts) AS open,
--     max(high)           AS high,
--     min(low)            AS low,
--     last(close, bar_ts) AS close,
--     sum(volume)         AS volume,
--     last(oi, bar_ts)    AS oi,
--     sum(oi_change)      AS oi_change
-- FROM bars
-- WHERE timeframe = '1D'
-- GROUP BY instrument_id, time_bucket('6 months', bar_ts)
-- WITH NO DATA;

-- -- 1Y (calendar year)
-- CREATE MATERIALIZED VIEW bars_1y
-- WITH (timescaledb.continuous) AS
-- SELECT
--     instrument_id,
--     '1Y' AS timeframe,
--     (time_bucket('1 year', bar_ts) + INTERVAL '1 year' - INTERVAL '1 day') AS bar_ts,

--     first(open, bar_ts) AS open,
--     max(high)           AS high,
--     min(low)            AS low,
--     last(close, bar_ts) AS close,
--     sum(volume)         AS volume,
--     last(oi, bar_ts)    AS oi,
--     sum(oi_change)      AS oi_change
-- FROM bars
-- WHERE timeframe = '1D'
-- GROUP BY instrument_id, time_bucket('1 year', bar_ts)
-- WITH NO DATA;


-- -- ============================================================
-- -- REFRESH POLICIES (separated by timeframe group)
-- -- ============================================================

-- -- Minute-based bars
-- DO $$
-- DECLARE view_name TEXT;
-- BEGIN
--     FOREACH view_name IN ARRAY ARRAY['bars_3m','bars_5m','bars_15m','bars_25m','bars_75m','bars_125m']
--     LOOP
--         PERFORM add_continuous_aggregate_policy(
--             view_name,
--             start_offset => INTERVAL '30 years',
--             end_offset   => INTERVAL '1 minute',
--             schedule_interval => INTERVAL '1 minute'
--         );
--     END LOOP;
-- END $$;

-- -- Day-based bars
-- DO $$
-- DECLARE view_name TEXT;
-- BEGIN
--     FOREACH view_name IN ARRAY ARRAY['bars_1w','bars_1mth','bars_3mth','bars_6mth','bars_1y']
--     LOOP
--         PERFORM add_continuous_aggregate_policy(
--             view_name,
--             start_offset => INTERVAL '30 years',
--             end_offset   => INTERVAL '1 hour',
--             schedule_interval => INTERVAL '1 hour'
--         );
--     END LOOP;
-- END $$;

-- -- ============================================================
-- -- For bars_external (contains 1m and 1D) resampling
-- -- ============================================================

-- -- 3m
-- CREATE MATERIALIZED VIEW bars_external_3m
-- WITH (timescaledb.continuous) AS
-- SELECT
--     instrument_id,
--     '3m' AS timeframe,
--     time_bucket('3 minutes', bar_ts) AS bar_ts,

--     first(open, bar_ts) AS open,
--     max(high)           AS high,
--     min(low)            AS low,
--     last(close, bar_ts) AS close,
--     sum(volume)         AS volume,
--     last(oi, bar_ts)    AS oi,
--     sum(oi_change)      AS oi_change
-- FROM bars_external
-- WHERE timeframe = '1m'
-- GROUP BY instrument_id, time_bucket('3 minutes', bar_ts)
-- WITH NO DATA;

-- -- 5m
-- CREATE MATERIALIZED VIEW bars_external_5m
-- WITH (timescaledb.continuous) AS
-- SELECT
--     instrument_id,
--     '5m' AS timeframe,
--     time_bucket('5 minutes', bar_ts) AS bar_ts,

--     first(open, bar_ts) AS open,
--     max(high)           AS high,
--     min(low)            AS low,
--     last(close, bar_ts) AS close,
--     sum(volume)         AS volume,
--     last(oi, bar_ts)    AS oi,
--     sum(oi_change)      AS oi_change
-- FROM bars_external
-- WHERE timeframe = '1m'
-- GROUP BY instrument_id, time_bucket('5 minutes', bar_ts)
-- WITH NO DATA;

-- -- 15m
-- CREATE MATERIALIZED VIEW bars_external_15m
-- WITH (timescaledb.continuous) AS
-- SELECT
--     instrument_id,
--     '15m' AS timeframe,
--     time_bucket('15 minutes', bar_ts) AS bar_ts,

--     first(open, bar_ts) AS open,
--     max(high)           AS high,
--     min(low)            AS low,
--     last(close, bar_ts) AS close,
--     sum(volume)         AS volume,
--     last(oi, bar_ts)    AS oi,
--     sum(oi_change)      AS oi_change
-- FROM bars_external
-- WHERE timeframe = '1m'
-- GROUP BY instrument_id, time_bucket('15 minutes', bar_ts)
-- WITH NO DATA;

-- -- 25m
-- CREATE MATERIALIZED VIEW bars_external_25m
-- WITH (timescaledb.continuous) AS
-- SELECT
--     instrument_id,
--     '25m' AS timeframe,
--     time_bucket('25 minutes', bar_ts) AS bar_ts,

--     first(open, bar_ts) AS open,
--     max(high)           AS high,
--     min(low)            AS low,
--     last(close, bar_ts) AS close,
--     sum(volume)         AS volume,
--     last(oi, bar_ts)    AS oi,
--     sum(oi_change)      AS oi_change
-- FROM bars_external
-- WHERE timeframe = '1m'
-- GROUP BY instrument_id, time_bucket('25 minutes', bar_ts)
-- WITH NO DATA;

-- -- 75m
-- CREATE MATERIALIZED VIEW bars_external_75m
-- WITH (timescaledb.continuous) AS
-- SELECT
--     instrument_id,
--     '75m' AS timeframe,
--     time_bucket('75 minutes', bar_ts) AS bar_ts,

--     first(open, bar_ts) AS open,
--     max(high)           AS high,
--     min(low)            AS low,
--     last(close, bar_ts) AS close,
--     sum(volume)         AS volume,
--     last(oi, bar_ts)    AS oi,
--     sum(oi_change)      AS oi_change
-- FROM bars_external
-- WHERE timeframe = '1m'
-- GROUP BY instrument_id, time_bucket('75 minutes', bar_ts)
-- WITH NO DATA;

-- -- 125m
-- CREATE MATERIALIZED VIEW bars_external_125m
-- WITH (timescaledb.continuous) AS
-- SELECT
--     instrument_id,
--     '125m' AS timeframe,
--     time_bucket('125 minutes', bar_ts) AS bar_ts,

--     first(open, bar_ts) AS open,
--     max(high)           AS high,
--     min(low)            AS low,
--     last(close, bar_ts) AS close,
--     sum(volume)         AS volume,
--     last(oi, bar_ts)    AS oi,
--     sum(oi_change)      AS oi_change
-- FROM bars_external
-- WHERE timeframe = '1m'
-- GROUP BY instrument_id, time_bucket('125 minutes', bar_ts)
-- WITH NO DATA;

-- -- ============================================================
-- -- DAILY → HIGHER TIMEFRAMES
-- -- ============================================================

-- -- 1W (ISO week, Monday-aligned)
-- CREATE MATERIALIZED VIEW bars_external_1w
-- WITH (timescaledb.continuous) AS
-- SELECT
--     instrument_id,
--     '1W' AS timeframe,
--     (time_bucket('1 week', bar_ts) + INTERVAL '1 week' - INTERVAL '1 day') AS bar_ts,

--     first(open, bar_ts) AS open,
--     max(high)           AS high,
--     min(low)            AS low,
--     last(close, bar_ts) AS close,
--     sum(volume)         AS volume,
--     last(oi, bar_ts)    AS oi,
--     sum(oi_change)      AS oi_change
-- FROM bars_external
-- WHERE timeframe = '1D'
-- GROUP BY instrument_id, time_bucket('1 week', bar_ts)
-- WITH NO DATA;

-- -- 1M (calendar month)
-- CREATE MATERIALIZED VIEW bars_external_1mth
-- WITH (timescaledb.continuous) AS
-- SELECT
--     instrument_id,
--     '1M' AS timeframe,
--     (time_bucket('1 month', bar_ts) + INTERVAL '1 month' - INTERVAL '1 day') AS bar_ts,

--     first(open, bar_ts) AS open,
--     max(high)           AS high,
--     min(low)            AS low,
--     last(close, bar_ts) AS close,
--     sum(volume)         AS volume,
--     last(oi, bar_ts)    AS oi,
--     sum(oi_change)      AS oi_change
-- FROM bars_external
-- WHERE timeframe = '1D'
-- GROUP BY instrument_id, time_bucket('1 month', bar_ts)
-- WITH NO DATA;

-- -- 3M (calendar quarter)
-- CREATE MATERIALIZED VIEW bars_external_3mth
-- WITH (timescaledb.continuous) AS
-- SELECT
--     instrument_id,
--     '3M' AS timeframe,
--     (time_bucket('3 months', bar_ts) + INTERVAL '3 months' - INTERVAL '1 day') AS bar_ts,

--     first(open, bar_ts) AS open,
--     max(high)           AS high,
--     min(low)            AS low,
--     last(close, bar_ts) AS close,
--     sum(volume)         AS volume,
--     last(oi, bar_ts)    AS oi,
--     sum(oi_change)      AS oi_change
-- FROM bars_external
-- WHERE timeframe = '1D'
-- GROUP BY instrument_id, time_bucket('3 months', bar_ts)
-- WITH NO DATA;

-- -- 6M (half-year: Jan–Jun, Jul–Dec)
-- CREATE MATERIALIZED VIEW bars_external_6mth
-- WITH (timescaledb.continuous) AS
-- SELECT
--     instrument_id,
--     '6M' AS timeframe,
--     (time_bucket('6 months', bar_ts) + INTERVAL '6 months' - INTERVAL '1 day') AS bar_ts,

--     first(open, bar_ts) AS open,
--     max(high)           AS high,
--     min(low)            AS low,
--     last(close, bar_ts) AS close,
--     sum(volume)         AS volume,
--     last(oi, bar_ts)    AS oi,
--     sum(oi_change)      AS oi_change
-- FROM bars_external
-- WHERE timeframe = '1D'
-- GROUP BY instrument_id, time_bucket('6 months', bar_ts)
-- WITH NO DATA;

-- -- 1Y (calendar year)
-- CREATE MATERIALIZED VIEW bars_external_1y
-- WITH (timescaledb.continuous) AS
-- SELECT
--     instrument_id,
--     '1Y' AS timeframe,
--     (time_bucket('1 year', bar_ts) + INTERVAL '1 year' - INTERVAL '1 day') AS bar_ts,

--     first(open, bar_ts) AS open,
--     max(high)           AS high,
--     min(low)            AS low,
--     last(close, bar_ts) AS close,
--     sum(volume)         AS volume,
--     last(oi, bar_ts)    AS oi,
--     sum(oi_change)      AS oi_change
-- FROM bars_external
-- WHERE timeframe = '1D'
-- GROUP BY instrument_id, time_bucket('1 year', bar_ts)
-- WITH NO DATA;

-- -- ============================================================
-- -- REFRESH POLICY (separated by timeframe group)
-- -- ============================================================

-- -- Minute-based bars_external
-- DO $$
-- DECLARE view_name TEXT;
-- BEGIN
--     FOREACH view_name IN ARRAY ARRAY['bars_external_3m','bars_external_5m','bars_external_15m','bars_external_25m','bars_external_75m','bars_external_125m']
--     LOOP
--         PERFORM add_continuous_aggregate_policy(
--             view_name,
--             start_offset => INTERVAL '30 years',
--             end_offset   => INTERVAL '1 minute',
--             schedule_interval => INTERVAL '1 minute'
--         );
--     END LOOP;
-- END $$;

-- -- Day-based bars_external
-- DO $$
-- DECLARE view_name TEXT;
-- BEGIN
--     FOREACH view_name IN ARRAY ARRAY['bars_external_1w','bars_external_1mth','bars_external_3mth','bars_external_6mth','bars_external_1y']
--     LOOP
--         PERFORM add_continuous_aggregate_policy(
--             view_name,
--             start_offset => INTERVAL '30 years',
--             end_offset   => INTERVAL '1 hour',
--             schedule_interval => INTERVAL '1 hour'
--         );
--     END LOOP;
-- END $$;


-- -- ========================================================================

-- -- docker exec -i artham_00_postgres \
-- --      psql -U postgres -d artham < ./postgres-init/004_create_bar_aggregates.sql

-- -- ========================================================================
