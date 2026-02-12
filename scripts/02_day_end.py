# ===================================================================
# Day Start Script
# ===================================================================

import os
import logging
from decouple import config
import psycopg
import redis
import time
import sys

logger = logging.getLogger(__name__)
logging.basicConfig(
    filename=(os.path.join(config("DIR_LOGS", cast=str), "artham_00_scripts.log")),
    encoding="utf-8",
    level=logging.DEBUG,
    datefmt="%Y-%m-%d %H:%M:%S %p %Z",
    format="%(asctime)s __DAY_END %(levelname)-8s %(message)s",
)

from library.core.bar import BAR_DOWNLOADER

# ===================================================================================================
# CONNECTIONS


pg_conn = psycopg.connect(
    dbname="artham",
    user=config("POSTGRES_USER", cast=str),
    password=config("POSTGRES_PASSWORD", cast=str),
    host=config("POSTGRES_HOST", cast=str),
    port=config("POSTGRES_PORT", cast=int),
    options=f"-c timezone=Asia/Kolkata",
)

# Test Postgres connection
try:
    with pg_conn.cursor() as cur:
        cur.execute("SELECT 1;")
        result = cur.fetchone()
        print("Postgres connection test result:", result)
        logger.info(f"Postgres connection test result: {result}")
except Exception as e:
    print(f"Postgres connection failed: {e}")
    logger.error(f"Postgres connection failed: {e}")


redis_conn = redis.Redis(
    host=config("REDIS_HOST", cast=str),
    port=config("REDIS_PORT", cast=int),
    decode_responses=True,
)

# Test Redis connection
try:
    pong = redis_conn.ping()
    print(
        "Redis connection test result: Pong" if pong else "Redis connection test failed"
    )
    logger.info(
        "Redis connection test result: Pong" if pong else "Redis connection test failed"
    )
except Exception as e:
    print(f"Redis connection failed: {e}")
    logger.error(f"Redis connection failed: {e}")


# ===================================================================================================
# Redis Flush

print("Flushing Redis database...")
logger.info("Flushing Redis database...")

# ===================================================================================================
# Download External Bars

# time the download process
start_time = time.time()

# read all instruments (Reliance EQ/OPT/FUT) from the new universe sets
instruments_eq = redis_conn.sinter("instruments:type:eq", "instruments:symbol:reliance")
instruments_opt = redis_conn.sinter("instruments:segment:nfo-opt", "instruments:underlying_trading_symbol:reliance")
instruments_fut = redis_conn.sinter("instruments:segment:nfo-fut", "instruments:underlying_trading_symbol:reliance")
instruments_all = [int(i) for i in set().union(instruments_eq, instruments_opt, instruments_fut)]
instruments_all_errors = []

downloader = BAR_DOWNLOADER(
    access_token=redis_conn.get("kite:access_token"),
    pg_conn=pg_conn,
    table_name="bars_external",
)

timeframes = ["1m", "1D"]

for instrument_id in instruments_all:
    for timeframe in timeframes:
        print(
            f"Downloading external bars for instrument_id: {instrument_id}, timeframe: {timeframe}"
        )
        logger.info(
            f"Downloading external bars for instrument_id: {instrument_id}, timeframe: {timeframe}"
        )

        try:
            downloader.download_bars(
                instrument_id=instrument_id,
                timeframe=timeframe,
            )
        except Exception as e:
            print(f"Error downloading bars for instrument_id {instrument_id}: {e}")
            logger.error(
                f"Error downloading bars for instrument_id {instrument_id}: {e}"
            )
            instruments_all_errors.append((instrument_id, timeframe))

# try again for errors
if instruments_all_errors:
    for instrument_id, timeframe in instruments_all_errors:
        print(
            f"Retrying downloading external bars for instrument_id: {instrument_id}, timeframe: {timeframe}"
        )
        logger.info(
            f"Retrying downloading external bars for instrument_id: {instrument_id}, timeframe: {timeframe}"
        )

        try:
            downloader.download_bars(
                instrument_id=instrument_id,
                timeframe=timeframe,
            )
        except Exception as e:
            print(f"Error downloading bars for instrument_id {instrument_id}: {e}")
            logger.error(
                f"Error downloading bars for instrument_id {instrument_id}: {e}"
            )

# ===================================================================================================
# A REPORT OF ALL THE EXTERNAL BARS DOWNLOADED TODAY

# Print total time required to download bars

total_time = time.time() - start_time
total_minutes = total_time / 60
print(f"\n\nTotal time required to download bars: {total_minutes:.2f} minutes\n\n")
logger.info(f"Total time required to download bars: {total_minutes:.2f} minutes")

# ===================================================================================================
# DONE

# close connections
pg_conn.close()
redis_conn.close()

sys.exit(1)
