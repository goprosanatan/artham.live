

-- -- Materialized view for daily bars report

-- CREATE MATERIALIZED VIEW IF NOT EXISTS bars_daily_report AS
-- SELECT
--     DATE(bar_ts AT TIME ZONE 'Asia/Kolkata') AS report_date,
--     instrument_id,
--     timeframe,
--     COUNT(*) AS bars_downloaded
-- FROM
--     bars
-- GROUP BY
--     report_date,
--     instrument_id,
--     timeframe
-- WITH NO DATA;

-- -- Index for fast refresh and query
-- CREATE INDEX IF NOT EXISTS idx_bars_daily_report_date
--     ON bars_daily_report (report_date);


-- -- Materialized view for daily bars_external report

-- CREATE MATERIALIZED VIEW IF NOT EXISTS bars_external_daily_report AS
-- SELECT
--     DATE(bar_ts AT TIME ZONE 'Asia/Kolkata') AS report_date,
--     instrument_id,
--     timeframe,
--     COUNT(*) AS bars_downloaded
-- FROM
--     bars_external
-- GROUP BY
--     report_date,
--     instrument_id,
--     timeframe
-- WITH NO DATA;

-- -- Index for fast refresh and query
-- CREATE INDEX IF NOT EXISTS idx_bars_external_daily_report_date
--     ON bars_external_daily_report (report_date);

-- -- Schedule a refresh at 4:30 PM IST every day using pg_cron (requires pg_cron extension)
-- -- This assumes your database timezone is set to Asia/Kolkata
-- -- If not, adjust the time accordingly

-- -- Enable pg_cron if not already enabled
-- -- CREATE EXTENSION IF NOT EXISTS pg_cron;

-- -- Schedule the refresh (run as superuser):
-- -- SELECT cron.schedule('refresh_bars_external_daily_report',
-- --     '30 16 * * *',
-- --     $$REFRESH MATERIALIZED VIEW CONCURRENTLY bars_external_daily_report;$$);

-- -- To manually refresh:
-- -- REFRESH MATERIALIZED VIEW CONCURRENTLY bars_external_daily_report;


-- ========================================================================

-- docker exec -i artham_00_postgres \
--      psql -U postgres -d artham < ./postgres-init/005_report_bars.sql

-- ========================================================================
