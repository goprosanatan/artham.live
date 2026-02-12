# ===================================================================================================
# ===================================================================================================

import logging
import numpy as np
import pandas as pd
import pytz
from decimal import Decimal
from typing import List, Literal, Union, Optional, Sequence
from datetime import datetime, timedelta, timezone, date
import asyncio
import psycopg
from decouple import config
from psycopg.rows import dict_row
from psycopg.sql import SQL, Identifier

from library import models
from library.adapters.kite_api import KITE_API
from library.modules import pg_crud

logger = logging.getLogger(__name__)

# ===================================================================================================
#


class BAR_RESAMPLER:
    """
    Resamples STANDARD_BAR candlestick data into higher timeframes.

    Supports both intraday and daily resampling with proper OHLCV aggregation.
    Intraday resampling respects day boundaries and dynamically detects market open times.
    Daily resampling supports weekly, monthly, quarterly, half-yearly, and yearly intervals.

    Attributes:
        df (pd.DataFrame): Internal DataFrame with OHLCV data indexed by bar_ts.
        instrument_id (int): Instrument identifier from input bars.
        dict_aggregation (dict): OHLCV aggregation rules for pandas resample.

    Example:
        >>> bars_1m = kite_api.get_historical_max(738561, "1m")
        >>> resampler = BAR_RESAMPLER(bars_1m)
        >>> bars_5m = resampler.resample_minute(5)
        >>> bars_weekly = resampler.resample_day("1W")
    """

    def __init__(
        self,
        bars: List[models.STANDARD_BAR],
    ):
        """
        Initialize the resampler with a list of STANDARD_BAR instances.

        Args:
            bars: List of STANDARD_BAR model instances with bar_ts, OHLCV, and OI data.
                Must contain at least one bar.

        Raises:
            ValueError: If bars list is empty.
        """
        if not bars:
            raise ValueError("Bar list cannot be empty")

        self.df = pd.DataFrame([c.model_dump() for c in bars])

        # get first element for instrument_id
        self.instrument_id = self.df["instrument_id"].iloc[0]

        # drop all columns except open, high, low, close, volume, oi
        self.df = self.df[
            [
                col
                for col in self.df.columns
                if col in ["bar_ts", "open", "high", "low", "close", "volume", "oi"]
            ]
        ]

        self.df["bar_ts"] = pd.to_datetime(self.df["bar_ts"])
        self.df.set_index("bar_ts", inplace=True)
        self.df.sort_index(inplace=True)

        self.dict_aggregation = {
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
            "oi": "last",
        }

        print(f"[BAR_RESAMPLER] Initialized for instrument_id {self.instrument_id} with {len(bars)} bars.")
        logger.info(f"[BAR_RESAMPLER] Initialized for instrument_id {self.instrument_id} with {len(bars)} bars.")

    # ------------------------------------------------------------------
    def resample_minute(self, minutes: int):
        """
        Resample 1-minute intraday bars into higher-minute intervals.

        Automatically detects each trading day's actual start time and ensures
        resampled bars never cross midnight boundaries. Each day is processed
        independently with dynamic offset alignment to the first bar of each day.

        Args:
            minutes: Target interval in minutes (e.g., 5, 15, 30, 60).
                Values <= 1 return bars as-is without resampling.

        Returns:
            List of STANDARD_BAR instances with:
                - bar_ts: Opening timestamp of each interval
                - open: First price in the interval
                - high: Maximum price in the interval
                - low: Minimum price in the interval
                - close: Last price in the interval
                - volume: Sum of volume in the interval
                - oi: Last OI value in the interval
                - oi_change: Difference from previous bar's OI (None for first bar)
                - timeframe: String format, e.g., "5m", "15m"
                - instrument_id: Preserved from input bars

        Example:
            >>> bars_1m = kite_api.get_historical_max(738561, "1m")
            >>> resampler = BAR_RESAMPLER(bars_1m)
            >>> bars_5m = resampler.resample_minute(5)
            >>> bars_15m = resampler.resample_minute(15)
        """

        if minutes <= 1:
            # No resampling needed — return as-is
            return [self._row_to_model(ts, row) for ts, row in self.df.iterrows()]

        print(f"[BAR_RESAMPLER] Resampling to {minutes} minute bars.")
        logger.info(f"[BAR_RESAMPLER] Resampling to {minutes} minute bars.")

        rule = f"{minutes}min"  # ✅ 'min' is the correct frequency alias
        resampled_chunks = []

        # Get all unique calendar days in the data
        unique_days = self.df.index.normalize().unique()

        for day in unique_days:
            # Mask to isolate 1 trading day (e.g., 2024-06-01 00:00 → 2024-06-02 00:00)
            mask = (self.df.index >= day) & (self.df.index < day + pd.Timedelta(days=1))
            df_day = self.df.loc[mask]

            if df_day.empty:
                continue

            # 🕒 Compute that day's first actual tick (may vary per day)
            first_tick_time = df_day.index[0].time()
            offset_str = f"{first_tick_time.hour:02d}:{first_tick_time.minute:02d}:{first_tick_time.second:02d}"

            # 📊 Resample within this day's window, respecting its true open time
            resampled = (
                df_day.resample(
                    rule=rule,
                    origin="start_day",  # anchor bins to each day separately
                    offset=offset_str,  # 🧠 dynamically aligned to that day’s open
                    closed="left",
                    label="left",
                )
                .agg(self.dict_aggregation)
                .dropna(subset=["open", "close"])  # remove empty bins
            )

            resampled_chunks.append(resampled)

        # 🔗 Combine all day-level resampled frames
        if not resampled_chunks:
            return []

        df_final = pd.concat(resampled_chunks).sort_index()

        df_final["oi"] = df_final["oi"].astype(int)
        df_final["oi_change"] = df_final["oi"].diff()
        df_final["instrument_id"] = self.instrument_id
        df_final["timeframe"] = f"{minutes}m"

        bars_out = [
            models.STANDARD_BAR(
                bar_ts=ts.to_pydatetime(),
                open=row["open"],
                high=row["high"],
                low=row["low"],
                close=row["close"],
                volume=row["volume"],
                oi=row["oi"] if pd.notna(row["oi"]) else None,
                oi_change=row["oi_change"] if pd.notna(row["oi_change"]) else None,
                timeframe=row["timeframe"],
                instrument_id=row["instrument_id"],
            )
            for ts, row in df_final.iterrows()
        ]
        print(f"[BAR_RESAMPLER] Resampled to {len(bars_out)} bars for {minutes}m interval.")
        logger.info(f"[BAR_RESAMPLER] Resampled to {len(bars_out)} bars for {minutes}m interval.")
        return bars_out

    # ------------------------------------------------------------------
    async def resample_minute_async(self, minutes: int):
        """
        Async version of resample_minute (runs in thread pool).

        Args:
            minutes: Target interval in minutes.

        Returns:
            List of STANDARD_BAR instances (same as resample_minute).

        Example:
            >>> bars_5m = await resampler.resample_minute_async(5)
        """
        return await asyncio.to_thread(self.resample_minute, minutes)

    # ------------------------------------------------------------------
    def resample_day(self, timeframe: Literal["1W", "1M", "3M", "6M", "1Y"]):
        """
        Resample daily bars into calendar-based intervals.

        Uses calendar-aware boundaries for weekly, monthly, quarterly, half-yearly,
        and yearly aggregation. The 6M interval uses custom logic to align with
        fiscal half-years (Jan-Jun and Jul-Dec, ending on Jun 30 and Dec 31).

        Args:
            timeframe: Target timeframe. Supported values:
                - "1W": Weekly (ends Sunday)
                - "1M": Monthly (ends last day of month)
                - "3M": Quarterly (ends Mar/Jun/Sep/Dec)
                - "6M": Half-yearly (ends Jun 30/Dec 31)
                - "1Y": Yearly (ends Dec 31)

        Returns:
            List of STANDARD_BAR instances with:
                - bar_ts: Period ending timestamp
                - open: First price in the period
                - high: Maximum price in the period
                - low: Minimum price in the period
                - close: Last price in the period
                - volume: Sum of volume in the period
                - oi: Last OI in the period
                - oi_change: Difference from previous period's OI (None for first)
                - timeframe: String format, e.g., "1W", "6M"
                - instrument_id: Preserved from input bars

        Raises:
            ValueError: If timeframe is not one of the supported values.

        Example:
            >>> bars_daily = kite_api.get_historical_max(738561, "1D")
            >>> resampler = BAR_RESAMPLER(bars_daily)
            >>> bars_weekly = resampler.resample_day("1W")
            >>> bars_monthly = resampler.resample_day("1M")
            >>> bars_6m = resampler.resample_day("6M")
        """
        freq_map = {
            "1W": "W-SUN",
            "1M": "ME",
            "3M": "QE",
            "1Y": "YE-DEC",
        }

        # Standard rules use direct pandas resampling
        if timeframe in ["1W", "1M", "3M", "1Y"]:
            rule = freq_map.get(timeframe)

            df_resampled = (
                self.df.resample(
                    rule=rule,
                    label="right",
                )
                .agg(self.dict_aggregation)
                .dropna(subset=["open", "close"])
            )

        # Custom handling for 6-month intervals
        elif timeframe == "6M":
            # ------------------------------------------------------------
            # 1️⃣ First, resample daily data to quarterly candles
            df_quarterly = (
                self.df.resample(
                    rule="QE",
                    label="right",
                )
                .agg(self.dict_aggregation)
                .dropna(subset=["open", "close"])
            )

            # ------------------------------------------------------------
            # 2️⃣ Create bins for 6-month periods: always end on Jun 30 and Dec 31
            start_year = df_quarterly.index.min().year
            end_year = df_quarterly.index.max().year

            # Generate boundaries for June and December ends
            bin_edges = []
            for year in range(start_year, end_year + 1):
                bin_edges.append(pd.Timestamp(f"{year}-06-30", tz=self.df.index.tz))
                bin_edges.append(pd.Timestamp(f"{year}-12-31", tz=self.df.index.tz))

            # Add an initial start bound slightly before the first date
            start_bound = pd.Timestamp(f"{start_year}-01-01", tz=self.df.index.tz)
            bins = [start_bound] + bin_edges

            # ------------------------------------------------------------
            # 3️⃣ Assign each quarterly candle to its half-year bin
            df_quarterly = df_quarterly.copy()
            df_quarterly["period"] = pd.cut(df_quarterly.index, bins=bins)

            # ------------------------------------------------------------
            # 4️⃣ Aggregate 2 quarters (Jan–Jun and Jul–Dec) into half-year candles
            df_resampled = (
                df_quarterly.groupby("period", observed=True)
                .agg(self.dict_aggregation)
                .dropna(subset=["open", "close"])
            )

            # ------------------------------------------------------------
            # 5️⃣ Use right bin edge (Jun 30 or Dec 31) as index
            df_resampled.index = [interval.right for interval in df_resampled.index]
        else:
            raise ValueError(f"Unsupported timeframe: {timeframe}")

        df_resampled["oi_change"] = df_resampled["oi"].diff()
        df_resampled["instrument_id"] = self.instrument_id
        df_resampled["timeframe"] = timeframe

        bars_out = [
            models.STANDARD_BAR(
                bar_ts=ts.to_pydatetime(),
                open=row["open"],
                high=row["high"],
                low=row["low"],
                close=row["close"],
                volume=row["volume"],
                oi=row["oi"] if pd.notna(row["oi"]) else None,
                oi_change=row["oi_change"] if pd.notna(row["oi_change"]) else None,
                timeframe=row["timeframe"],
                instrument_id=row["instrument_id"],
            )
            for ts, row in df_resampled.iterrows()
        ]
        print(f"[BAR_RESAMPLER] Resampled to {len(bars_out)} bars for {timeframe} interval.")
        logger.info(f"[BAR_RESAMPLER] Resampled to {len(bars_out)} bars for {timeframe} interval.")
        return bars_out

    # ------------------------------------------------------------------
    #  ASYNC VERSION (same output, non-blocking)
    # ------------------------------------------------------------------
    async def resample_day_async(
        self, timeframe: Literal["1W", "1M", "3M", "6M", "1Y"]
    ):
        return await asyncio.to_thread(self.resample_day, timeframe)


# -----------------------------------------------------------


class BAR_DOWNLOADER:
    def __init__(
        self,
        access_token: str,
        pg_conn: psycopg.Connection,
        schema_name: str = "public",    
        table_name: str = "bars_external",
    ):
        self.kite_api = KITE_API(access_token=access_token)
        self.db_name = config("POSTGRES_DB", cast=str)
        self.schema_name = schema_name
        self.table_name = table_name
        self.pg_conn = pg_conn

    def get_last_bar_ts(
        self,
        instrument_id: int,
        timeframe: Literal["1m", "1D"],
    ):

        crud_pydantic = pg_crud.WITH_PYDANTIC(
            timezone="Asia/Kolkata",
            pg_conn=self.pg_conn,
        )

        last_bar = crud_pydantic.table_select_one(
            db_name=self.db_name,
            schema_name=self.schema_name,
            table_name=self.table_name,
            filters={
                "instrument_id": instrument_id,
                "timeframe": timeframe,
            },
            order_by=[("bar_ts", "DESC")],
        )

        if last_bar is None:
            return datetime(1970, 1, 1)

        last_bar_ts = last_bar["bar_ts"].replace(tzinfo=None)

        msg = f"[BAR_DOWNLOADER] Last bar_ts for instrument_id {instrument_id}, timeframe {timeframe} is {last_bar_ts}"
        print(msg)
        logger.info(msg)
        return last_bar_ts

    def download_bars(
        self,
        instrument_id: int,
        timeframe: Literal["1m", "1D"],
    ) -> int:

        msg = f"[BAR_DOWNLOADER] Downloading bars for instrument_id {instrument_id} with timeframe {timeframe}..."
        print(msg)
        logger.info(msg)

        last_bar_ts = self.get_last_bar_ts(
            instrument_id=instrument_id,
            timeframe=timeframe,
        )

        bars = self.kite_api.get_historical_max(
            instrument_id=instrument_id,
            interval=timeframe,
            last_bar_ts=last_bar_ts,
        )

        msg = f"[BAR_DOWNLOADER] Downloaded {len(bars)} bars for instrument_id {instrument_id} since {last_bar_ts}"
        print(msg)
        logger.info(msg)

        crud_pydantic = pg_crud.WITH_PYDANTIC(
            timezone="Asia/Kolkata",
            pg_conn=self.pg_conn,
        )

        if not crud_pydantic.table_check(
            db_name=self.db_name,
            schema_name=self.schema_name,
            table_name=self.table_name,
        ):

            msg = f"[BAR_DOWNLOADER] Creating table {self.table_name} in database {self.db_name}..."
            print(msg)
            logger.info(msg)

            crud_pydantic.table_create(
                db_name=self.db_name,
                schema_name=self.schema_name,
                table_name=self.table_name,
                model=models.STANDARD_BAR,
            )

            crud_pydantic.table_to_timescaledb_hypertable(
                db_name=self.db_name,
                schema_name=self.schema_name,
                table_name=self.table_name,
                time_column="bar_ts",
                partitioning_column="instrument_id",
                number_partitions=4,
                chunk_time_interval="1 day",
                if_not_exists=True,
                create_default_indexes=False,
            )

            msg = f"[BAR_DOWNLOADER] Table {self.table_name} created."
            print(msg)
            logger.info(msg)

        crud_pydantic.table_upsert(
            db_name=self.db_name,
            schema_name=self.schema_name,
            table_name=self.table_name,
            model=models.STANDARD_BAR,
            list_models=bars,
        )

        msg = f"[BAR_DOWNLOADER] Upserted {len(bars)} bars for instrument_id {instrument_id}."
        print(msg)
        logger.info(msg)

        return len(bars)


# -----------------------------------------------------------


class BAR_COMPILER:

    def __init__(self):
        pass


# -----------------------------------------------------------


class BAR_LOADER:
    """
    Stateless historical bar loader for charting with simple pagination.

    Loading model:
    - Initial load: end_ts = None → latest bars
    - Pagination:   end_ts = oldest_bar_ts → bars before that timestamp
    - Always returns bars in ASCENDING time order
    """

    def __init__(
        self,
        pg_conn: psycopg.Connection,
        schema_name: str = "public",
        table_name: str = "bars",
    ):
        self.pg_conn = pg_conn
        self.schema_name = schema_name
        self.table_name = table_name

    # ----------------------------
    def _build_query(
        self,
        instrument_id: int,
        timeframe: Literal["1m", "1D"],
        end_ts: Optional[datetime],
        start_ts: Optional[datetime],
    ) -> tuple[SQL, Sequence[object]]:
        clauses = [SQL("instrument_id = %s"), SQL("timeframe = %s")]
        params: list[object] = [instrument_id, timeframe]

        if start_ts is not None:
            clauses.append(SQL("bar_ts >= %s"))
            params.append(start_ts)

        if end_ts is not None:
            clauses.append(SQL("bar_ts < %s"))
            params.append(end_ts)

        where_sql = SQL(" WHERE ") + SQL(" AND ").join(clauses)

        query = (
            SQL("SELECT * FROM {schema}.{table}").format(
                schema=Identifier(self.schema_name), table=Identifier(self.table_name)
            )
            + where_sql
            + SQL(" ORDER BY bar_ts DESC")
        )

        return query, params

    # ----------------------------
    def load_bars(
        self,
        instrument_id: int,
        timeframe: Literal["1m", "1D"],
        timestamp_end: Optional[int] = None,
    ) -> list[dict]:
        """
        Fetch historical bars for charting.

        Parameters
        ----------
        instrument_id : int
        timeframe     : Literal["1m", "1D"]
        timestamp_end : int | None
            Unix timestamp in milliseconds
            - None  → latest bars
            - value → bars strictly BEFORE this timestamp

        Returns
        -------
        List[dict] ordered ASC by bar_ts
        """

        # Convert milliseconds to datetime if provided
        end_ts = None
        if timestamp_end is not None:
            end_ts = datetime.fromtimestamp(timestamp_end / 1000, tz=timezone.utc)

        # Add a bounded window to enable chunk pruning and ordered index scans
        # for the common "latest N bars" use case. Clamp to avoid scanning years
        # of history when only a few hundred rows are needed.
        if timeframe == "1m":
            window = timedelta(days=5)
        else:  # "1D"
            window = timedelta(days=1000)

        anchor_ts = end_ts or datetime.now(timezone.utc)
        start_ts = anchor_ts - window

        query, params = self._build_query(
            instrument_id=instrument_id,
            timeframe=timeframe,
            end_ts=end_ts,
            start_ts=start_ts,
        )

        with self.pg_conn.cursor(row_factory=dict_row) as cursor:
            cursor.execute(query, params)
            rows = cursor.fetchall()

        print(f"[BAR_LOADER] Loaded {len(rows)} bars for instrument_id={instrument_id}, timeframe={timeframe}")
        logger.info(f"[BAR_LOADER] Loaded {len(rows)} bars for instrument_id={instrument_id}, timeframe={timeframe}")

        return list(reversed(rows))  # return in ASC order


# -----------------------------------------------------------


class BAR_LOADER_ASYNC:
    """
    Async stateless historical bar loader for charting with simple pagination.

    Loading model:
    - Initial load: timestamp_end = None → latest bars
    - Pagination:   timestamp_end = oldest_bar_ts → bars before that timestamp
    - Always returns bars in ASCENDING time order
    """

    def __init__(
        self,
        pg_conn: psycopg.AsyncConnection,
        schema_name: str = "public",
        table_name: str = "bars",
    ):
        self.pg_conn = pg_conn
        self.schema_name = schema_name
        self.table_name = table_name

    # ----------------------------
    def _build_query(
        self,
        instrument_id: int,
        timeframe: Literal["1m", "1D"],
        end_ts: Optional[datetime],
        start_ts: Optional[datetime],
    ) -> tuple[SQL, Sequence[object]]:
        clauses = [SQL("instrument_id = %s"), SQL("timeframe = %s")]
        params: list[object] = [instrument_id, timeframe]

        if start_ts is not None:
            clauses.append(SQL("bar_ts >= %s"))
            params.append(start_ts)

        if end_ts is not None:
            clauses.append(SQL("bar_ts < %s"))
            params.append(end_ts)

        where_sql = SQL(" WHERE ") + SQL(" AND ").join(clauses)

        query = (
            SQL("SELECT * FROM {schema}.{table}").format(
                schema=Identifier(self.schema_name), table=Identifier(self.table_name)
            )
            + where_sql
            + SQL(" ORDER BY bar_ts DESC")
        )

        return query, params

    # ----------------------------
    async def load_bars(
        self,
        instrument_id: int,
        timeframe: Literal["1m", "1D"],
        timestamp_end: Optional[int] = None,
    ) -> list[dict]:
        """
        Fetch historical bars for charting asynchronously.

        Parameters
        ----------
        instrument_id : int
        timeframe     : Literal["1m", "1D"]
        timestamp_end : int | None
            Unix timestamp in milliseconds
            - None  → latest bars
            - value → bars strictly BEFORE this timestamp

        Returns
        -------
        List[dict] ordered ASC by bar_ts
        """
        # Convert milliseconds to datetime if provided
        end_ts = None
        if timestamp_end is not None:
            end_ts = datetime.fromtimestamp(timestamp_end / 1000, tz=timezone.utc)

        # Mirror sync loader window bounds for consistency
        if timeframe == "1m":
            window = timedelta(days=5)
        else:  # "1D"
            window = timedelta(days=1000)

        anchor_ts = end_ts or datetime.now(timezone.utc)
        start_ts = anchor_ts - window

        query, params = self._build_query(
            instrument_id=instrument_id,
            timeframe=timeframe,
            end_ts=end_ts,
            start_ts=start_ts,
        )

        async with self.pg_conn.cursor(row_factory=dict_row) as cursor:
            await cursor.execute(query, params)
            rows = await cursor.fetchall()

        return list(reversed(rows))  # return in ASC order


# -----------------------------------------------------------


class BAR_DELETER:
    """
    Deletes historical bars based on criteria.

    Deletion model:
    - By instrument and timeframe: Deletes all bars for a given instrument_id and timeframe.
    - After timestamp: Deletes all bars strictly after a given timestamp.

    Usage:
        deleter = BAR_DELETER(pg_conn)
        deleted_count = deleter.delete_by_instrument_timeframe(738561, "1m")
        deleted_count = deleter.delete_after_timestamp(datetime(2023, 1, 1, tzinfo=timezone.utc))
    """

    def __init__(
        self,
        pg_conn: psycopg.Connection,
        schema_name: str = "public",
        table_name: str = "bars",
    ):
        self.pg_conn = pg_conn
        self.schema_name = schema_name
        self.table_name = table_name

    def delete_by_instrument_timeframe(
        self,
        instrument_id: int,
        timeframe: Literal["1m", "1D"],
    ) -> int:
        """Delete bars for a given instrument and timeframe. Returns rows deleted."""

        query = SQL(
            """
            DELETE FROM {schema}.{table}
            WHERE instrument_id = %s AND timeframe = %s
        """
        ).format(
            schema=Identifier(self.schema_name),
            table=Identifier(self.table_name),
        )

        with self.pg_conn.cursor() as cursor:
            cursor.execute(query, (instrument_id, timeframe))
            deleted = cursor.rowcount
        self.pg_conn.commit()

        print(f"[BAR_DELETER] Deleted {deleted} rows for instrument_id={instrument_id}, timeframe={timeframe}")
        logger.info(f"[BAR_DELETER] Deleted {deleted} rows for instrument_id={instrument_id}, timeframe={timeframe}")

        return deleted

    def delete_after_timestamp(
        self,
        dt: datetime,
    ) -> int:
        """Delete bars strictly after the provided timestamp. Returns rows deleted."""

        # Enforce a timezone-aware timestamp to avoid accidental local time deletes.
        if dt.tzinfo is None:
            raise ValueError("dt must be timezone-aware (UTC recommended)")

        query = SQL(
            """
            DELETE FROM {schema}.{table}
            WHERE bar_ts > %s
        """
        ).format(
            schema=Identifier(self.schema_name),
            table=Identifier(self.table_name),
        )

        with self.pg_conn.cursor() as cursor:
            cursor.execute(query, (dt,))
            deleted = cursor.rowcount
        self.pg_conn.commit()

        print(f"[BAR_DELETER] Deleted {deleted} rows after {dt}")
        logger.info(f"[BAR_DELETER] Deleted {deleted} rows after {dt}")

        return deleted


# -----------------------------------------------------------
