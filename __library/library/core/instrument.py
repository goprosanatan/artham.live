# ===================================================================================================
# ===================================================================================================

import os
import logging
import psycopg
from psycopg.rows import class_row, dict_row
from psycopg.sql import SQL, Identifier, Composed, Placeholder
from decimal import Decimal
from typing import Optional, Any, List, Dict, Literal, Type
import math
from datetime import datetime, timedelta, timezone, date
from decouple import config

from library import models
from library.adapters.kite_api import KITE_API
from library.modules import pg_crud

logger = logging.getLogger(__name__)

# ===================================================================================================


class INSTRUMENT_SEARCH:
    """Class to query the instruments table and return models.STANDARD_INSTRUMENT models."""

    def __init__(
        self,
        pg_conn: psycopg.Connection,
    ):
        self.pg_conn = pg_conn
        self.schema_name = "public"
        self.table_name = "instruments"

    # -------------------------------------------------------------------------
    # Private Helper
    # -------------------------------------------------------------------------
    def _run_query(self, where: Optional[str] = None, params: Optional[list] = None):
        """Executes SQL SELECT and returns list of STANDARD_INSTRUMENT models."""
        base_query = SQL("SELECT * FROM {}.{}").format(
            Identifier(self.schema_name),
            Identifier(self.table_name),
        )

        if where:
            base_query += SQL(" WHERE ") + SQL(where)

        # Always return instruments in display order
        base_query += SQL(" ORDER BY display_order ASC")

        with self.pg_conn.cursor(row_factory=dict_row) as cur:
            cur.execute(base_query, params or [])
            rows = cur.fetchall()

        print(f"[INSTRUMENT_SEARCH] Returned {len(rows)} rows")
        logger.info(f"[INSTRUMENT_SEARCH] Returned {len(rows)} rows")
        return [models.STANDARD_INSTRUMENT(**r) for r in rows]

    # -------------------------------------------------------------------------
    # Public Read Methods
    # -------------------------------------------------------------------------
    def get_all(self) -> List[models.STANDARD_INSTRUMENT]:
        """Return all instruments."""
        query = SQL("SELECT * FROM {}.{} ORDER BY display_order ASC").format(
            Identifier(self.schema_name),
            Identifier(self.table_name),
        )

        with self.pg_conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query)
            rows = cur.fetchall()

        return [models.STANDARD_INSTRUMENT(**r) for r in rows]

    # -------------------------------------------------------------------------
    # Filter exact matches
    # -------------------------------------------------------------------------
    def filter(
        self,
        instrument_id: Optional[str] = None,
        exchange: Optional[str] = None,
        segment: Optional[str] = None,
        trading_symbol: Optional[str] = None,
        underlying_instrument_id: Optional[str] = None,
        underlying_trading_symbol: Optional[str] = None,
        instrument_type: Optional[str] = None,
        description: Optional[str] = None,
        isin: Optional[str] = None,
        strike: Optional[Decimal] = None,
        active: Optional[bool] = None,
    ) -> List[models.STANDARD_INSTRUMENT]:
        """Filter instruments by exact matches (case-sensitive)."""
        where_clauses, params = [], []

        if instrument_id:
            where_clauses.append("instrument_id = %s")
            params.append(instrument_id)
        if exchange:
            where_clauses.append("exchange = %s")
            params.append(exchange)
        if segment:
            where_clauses.append("segment = %s")
            params.append(segment)
        if trading_symbol:
            where_clauses.append("trading_symbol = %s")
            params.append(trading_symbol)
        if underlying_instrument_id:
            where_clauses.append("underlying_instrument_id = %s")
            params.append(underlying_instrument_id)
        if underlying_trading_symbol:
            where_clauses.append("underlying_trading_symbol = %s")
            params.append(underlying_trading_symbol)
        if instrument_type:
            where_clauses.append("instrument_type = %s")
            params.append(instrument_type)
        if description:
            where_clauses.append("description = %s")
            params.append(description)
        if isin:
            where_clauses.append("isin = %s")
            params.append(isin)
        if strike:
            where_clauses.append("strike = %s")
            params.append(strike)
        # active=True => only active instruments
        # active=False/None => no filter (return all)
        if active is True:
            where_clauses.append("active = %s")
            params.append(True)

        where = " AND ".join(where_clauses) if where_clauses else None
        return self._run_query(where, params)

    # -------------------------------------------------------------------------
    # Search (partial match, case-insensitive)
    # -------------------------------------------------------------------------
    def search(
        self,
        instrument_id: Optional[str] = None,
        exchange: Optional[str] = None,
        segment: Optional[str] = None,
        trading_symbol: Optional[str] = None,
        underlying_instrument_id: Optional[str] = None,
        underlying_trading_symbol: Optional[str] = None,
        instrument_type: Optional[str] = None,
        description: Optional[str] = None,
        isin: Optional[str] = None,
        strike: Optional[Decimal] = None,
        active: Optional[bool] = None,
    ) -> List[models.STANDARD_INSTRUMENT]:
        """Search instruments by partial match (ILIKE)."""
        where_clauses, params = [], []

        def add_ilike(field, value):
            where_clauses.append(f"{field} ILIKE %s")
            params.append(f"%{value}%")

        if instrument_id:
            add_ilike("instrument_id", instrument_id)
        if exchange:
            add_ilike("exchange", exchange)
        if segment:
            add_ilike("segment", segment)
        if trading_symbol:
            add_ilike("trading_symbol", trading_symbol)
        if underlying_instrument_id:
            add_ilike("underlying_instrument_id", underlying_instrument_id)
        if underlying_trading_symbol:
            add_ilike("underlying_trading_symbol", underlying_trading_symbol)
        if instrument_type:
            add_ilike("instrument_type", instrument_type)
        if description:
            add_ilike("description", description)
        if isin:
            add_ilike("isin", isin)
        if strike:
            where_clauses.append("strike = %s")
            params.append(strike)
        # active=True => only active instruments
        # active=False/None => no filter (return all)
        if active is True:
            where_clauses.append("active = %s")
            params.append(True)

        where = " AND ".join(where_clauses) if where_clauses else None
        return self._run_query(where, params)

    # -------------------------------------------------------------------------
    # Unique values
    # -------------------------------------------------------------------------
    def unique(self, column_name: str) -> List[Any]:
        """Return unique values for a given column."""
        query = SQL(
            """
            SELECT DISTINCT {col}
            FROM {schema}.{table}
            ORDER BY {col}
            """
        ).format(
            col=Identifier(column_name),
            schema=Identifier(self.schema_name),
            table=Identifier(self.table_name),
        )

        with self.pg_conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

        unique_values = [r[0] for r in rows]
        print(
            f"[INSTRUMENT_SEARCH] Found {len(unique_values)} unique values for {column_name}"
        )
        logger.info(f"[INSTRUMENT_SEARCH] Found {len(unique_values)} unique values for {column_name}")
        return unique_values

    # -------------------------------------------------------------------------
    # Derivatives (options + futures)
    # -------------------------------------------------------------------------
    def derivatives(
        self,
        exchange: str,
        underlying_trading_symbol: str,
    ) -> Dict[str, Any]:
        """
        Returns structured options and futures
        Return a structured option chain:
        {
            "options": {
                expiry: {
                    strike: {"CE": STANDARD_INSTRUMENT, "PE": STANDARD_INSTRUMENT}
                }
            },
            "futures": [STANDARD_INSTRUMENT, ...]
        }
        """

        # 2️⃣ Get all derivatives (options + futures)
        query = SQL(
            """
            SELECT * FROM {schema}.{table}
            WHERE underlying_trading_symbol ILIKE %s AND exchange ILIKE %s
            ORDER BY expiry, strike
            """
        ).format(
            schema=Identifier(self.schema_name),
            table=Identifier(self.table_name),
        )

        with self.pg_conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, [f"%{underlying_trading_symbol}%", f"%{exchange}%"])
            rows = cur.fetchall()

        if not rows:
            print(
                f"[INSTRUMENT_SEARCH] No derivatives found for {underlying_trading_symbol}"
            )
            logger.warning(f"[INSTRUMENT_SEARCH] No derivatives found for {underlying_trading_symbol}")
            return {"options": {}, "futures": []}

        all_derivatives = [models.STANDARD_INSTRUMENT(**r) for r in rows]

        # 3️⃣ Separate futures and options
        futures = [r for r in all_derivatives if r.instrument_type == "FUT"]
        options = [r for r in all_derivatives if r.instrument_type in ("CE", "PE")]

        # 4️⃣ Build tree-like option chain
        expiries: Dict[str, Dict[str, models.STANDARD_INSTRUMENT]] = {}

        for opt in options:
            if not opt.expiry or not opt.strike:
                continue

            expiry_key = str(opt.expiry)
            strike_key = str(opt.strike)

            expiries.setdefault(expiry_key, {})
            expiries[expiry_key].setdefault(strike_key, {})
            expiries[expiry_key][strike_key][opt.instrument_type] = opt

        print(
            f"[INSTRUMENT_SEARCH] Derivatives retrieved for {underlying_trading_symbol}: "
            f"{len(expiries)} expiries, {len(options)} options, {len(futures)} futures"
        )
        logger.info(f"[INSTRUMENT_SEARCH] Derivatives retrieved for {underlying_trading_symbol}: {len(expiries)} expiries, {len(options)} options, {len(futures)} futures")

        return {"options": expiries, "futures": futures}


# -----------------------------------------------------------


class INSTRUMENT_SEARCH_ASYNC:
    """Async version of INSTRUMENT_SEARCH."""

    def __init__(
        self,
        pg_conn: psycopg.AsyncConnection,
    ):
        self.pg_conn = pg_conn
        self.schema_name = "public"
        self.table_name = "instruments"

    # -------------------------------------------------------------------------
    # Private Helper
    # -------------------------------------------------------------------------
    async def _run_query(
        self, where: Optional[str] = None, params: Optional[list] = None
    ):
        query = SQL("SELECT * FROM {}.{}").format(
            Identifier(self.schema_name),
            Identifier(self.table_name),
        )

        if where:
            query += SQL(" WHERE ") + SQL(where)

        # Always return instruments in display order
        query += SQL(" ORDER BY display_order ASC")

        async with self.pg_conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(query, params or [])
            rows = await cur.fetchall()

        return [models.STANDARD_INSTRUMENT(**r) for r in rows]

    # -------------------------------------------------------------------------
    # Public Read Methods
    # -------------------------------------------------------------------------
    async def get_all(self) -> List[models.STANDARD_INSTRUMENT]:
        query = SQL("SELECT * FROM {}.{} ORDER BY display_order ASC").format(
            Identifier(self.schema_name), Identifier(self.table_name)
        )

        async with self.pg_conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(query)
            rows = await cur.fetchall()

        return [models.STANDARD_INSTRUMENT(**r) for r in rows]

    # -------------------------------------------------------------------------
    # Filter exact matches
    # -------------------------------------------------------------------------
    async def filter(
        self,
        instrument_id: Optional[str] = None,
        exchange: Optional[str] = None,
        segment: Optional[str] = None,
        trading_symbol: Optional[str] = None,
        underlying_instrument_id: Optional[str] = None,
        underlying_trading_symbol: Optional[str] = None,
        instrument_type: Optional[str] = None,
        name: Optional[str] = None,
        isin: Optional[str] = None,
        strike: Optional[Decimal] = None,
        active: Optional[bool] = None,
    ) -> List[models.STANDARD_INSTRUMENT]:

        where_clauses, params = [], []

        def add(field, v):
            where_clauses.append(f"{field} = %s")
            params.append(v)

        if instrument_id:
            add("instrument_id", instrument_id)
        if exchange:
            add("exchange", exchange)
        if segment:
            add("segment", segment)
        if trading_symbol:
            add("trading_symbol", trading_symbol)
        if underlying_instrument_id:
            add("underlying_instrument_id", underlying_instrument_id)
        if underlying_trading_symbol:
            add("underlying_trading_symbol", underlying_trading_symbol)
        if instrument_type:
            add("instrument_type", instrument_type)
        if name:
            add("name", name)
        if isin:
            add("isin", isin)
        if strike:
            add("strike", strike)
        # active=True => only active instruments
        # active=False/None => no filter (return all)
        if active is True:
            add("active", active)

        where = " AND ".join(where_clauses) if where_clauses else None
        return await self._run_query(where, params)

    # -------------------------------------------------------------------------
    # Search (partial match, case-insensitive)
    # -------------------------------------------------------------------------
    async def search(
        self,
        instrument_id: Optional[str] = None,
        exchange: Optional[str] = None,
        segment: Optional[str] = None,
        trading_symbol: Optional[str] = None,
        underlying_instrument_id: Optional[str] = None,
        underlying_trading_symbol: Optional[str] = None,
        instrument_type: Optional[str] = None,
        name: Optional[str] = None,
        isin: Optional[str] = None,
        strike: Optional[Decimal] = None,
        active: Optional[bool] = None,
    ) -> List[models.STANDARD_INSTRUMENT]:

        where_clauses, params = [], []

        def ilike(field, value):
            where_clauses.append(f"{field} ILIKE %s")
            params.append(f"%{value}%")

        if instrument_id:
            ilike("instrument_id", instrument_id)
        if exchange:
            ilike("exchange", exchange)
        if segment:
            ilike("segment", segment)
        if trading_symbol:
            ilike("trading_symbol", trading_symbol)
        if underlying_instrument_id:
            ilike("underlying_instrument_id", underlying_instrument_id)
        if underlying_trading_symbol:
            ilike("underlying_trading_symbol", underlying_trading_symbol)
        if instrument_type:
            ilike("instrument_type", instrument_type)
        if name:
            ilike("name", name)
        if isin:
            ilike("isin", isin)
        if strike:
            where_clauses.append("strike = %s")
            params.append(strike)
        # active=True => only active instruments
        # active=False/None => no filter (return all)
        if active is True:
            where_clauses.append("active = %s")
            params.append(True)

        where = " AND ".join(where_clauses) if where_clauses else None
        return await self._run_query(where, params)

    # -------------------------------------------------------------------------
    # Unique
    # -------------------------------------------------------------------------
    async def unique(self, column_name: str) -> List[Any]:
        query = SQL(
            """
            SELECT DISTINCT {col}
            FROM {schema}.{table}
            ORDER BY {col}
            """
        ).format(
            col=Identifier(column_name),
            schema=Identifier(self.schema_name),
            table=Identifier(self.table_name),
        )

        async with self.pg_conn.cursor() as cur:
            await cur.execute(query)
            rows = await cur.fetchall()

        return [r[0] for r in rows]

    # -------------------------------------------------------------------------
    # Derivatives
    # -------------------------------------------------------------------------
    async def derivatives(
        self, exchange: str, underlying_trading_symbol: str
    ) -> Dict[str, Any]:
        query = SQL(
            """
            SELECT * FROM {schema}.{table}
            WHERE underlying_trading_symbol ILIKE %s AND exchange ILIKE %s
            ORDER BY expiry, strike
            """
        ).format(
            schema=Identifier(self.schema_name),
            table=Identifier(self.table_name),
        )

        async with self.pg_conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                query, [f"%{underlying_trading_symbol}%", f"%{exchange}%"]
            )
            rows = await cur.fetchall()

        if not rows:
            return {"options": {}, "futures": []}

        all_items = [models.STANDARD_INSTRUMENT(**r) for r in rows]

        futures = [r for r in all_items if r.instrument_type == "FUT"]
        options = [r for r in all_items if r.instrument_type in ("CE", "PE")]

        expiries: Dict[str, Dict[str, Any]] = {}

        for opt in options:
            if not opt.expiry or not opt.strike:
                continue

            expiries.setdefault(str(opt.expiry), {}).setdefault(str(opt.strike), {})[
                opt.instrument_type
            ] = opt

        return {"options": expiries, "futures": futures}


# -----------------------------------------------------------


class INSTRUMENT_DOWNLOADER:
    """Downloads instruments from Kite API and transforms to STANDARD_INSTRUMENT models."""

    def __init__(
        self,
        access_token: str,
        pg_conn: psycopg.Connection,
    ):
        self.kite_api = KITE_API(access_token=access_token)
        self.db_name = config("POSTGRES_DB", cast=str)
        self.schema_name = "public"
        self.table_name = "instruments"
        self.pg_conn = pg_conn

    def download_instruments(self) -> int:

        print("[INSTRUMENT_DOWNLOADER] Starting instrument download...")
        logger.info("[INSTRUMENT_DOWNLOADER] Starting instrument download...")

        # Download fresh data
        instruments = self.kite_api.get_instruments()

        print(f"[INSTRUMENT_DOWNLOADER] Downloaded {len(instruments)} instruments")
        logger.info(f"[INSTRUMENT_DOWNLOADER] Downloaded {len(instruments)} instruments")

        if not instruments:
            print("[INSTRUMENT_DOWNLOADER] No instruments downloaded")
            logger.warning("[INSTRUMENT_DOWNLOADER] No instruments downloaded")
            return 0

        crud_pydantic = pg_crud.WITH_PYDANTIC(
            timezone="Asia/Kolkata",
            pg_conn=self.pg_conn,
        )

        if not crud_pydantic.table_check(
            db_name=self.db_name,
            schema_name=self.schema_name,
            table_name=self.table_name,
        ):
            print(
                f"[INSTRUMENT_DOWNLOADER] Table {self.schema_name}.{self.table_name} does not exist. Creating..."
            )
            logger.info(f"[INSTRUMENT_DOWNLOADER] Table {self.schema_name}.{self.table_name} does not exist. Creating...")

            crud_pydantic.table_create(
                db_name=self.db_name,
                schema_name=self.schema_name,
                table_name=self.table_name,
                model=models.STANDARD_INSTRUMENT,
            )
            print(
                f"[INSTRUMENT_DOWNLOADER] ✓ Created table {self.schema_name}.{self.table_name} for instruments"
            )
            logger.info(f"[INSTRUMENT_DOWNLOADER] ✓ Created table {self.schema_name}.{self.table_name} for instruments")

        else:
            print(
                f"[INSTRUMENT_DOWNLOADER] Table {self.schema_name}.{self.table_name} exists. Proceeding to truncate..."
            )
            logger.info(f"[INSTRUMENT_DOWNLOADER] Table {self.schema_name}.{self.table_name} exists. Proceeding to truncate...")

            crud_pydantic.table_truncate(
                db_name=self.db_name,
                schema_name=self.schema_name,
                table_name=self.table_name,
            )

            print(
                f"[INSTRUMENT_DOWNLOADER] ✓ Truncated table {self.schema_name}.{self.table_name} for fresh insert"
            )
            logger.info(f"[INSTRUMENT_DOWNLOADER] ✓ Truncated table {self.schema_name}.{self.table_name} for fresh insert")

        crud_pydantic.table_insert(
            db_name=self.db_name,
            schema_name=self.schema_name,
            table_name=self.table_name,
            model=models.STANDARD_INSTRUMENT,
            list_models=instruments,
        )

        print(f"[INSTRUMENT_DOWNLOADER] Inserted {len(instruments)} instruments")
        logger.info(f"[INSTRUMENT_DOWNLOADER] Inserted {len(instruments)} instruments")

        return len(instruments)


# -----------------------------------------------------------


class INSTRUMENT_DOWNLOADER_ASYNC:
    """Async version: Downloads instruments from Kite API and transforms to STANDARD_INSTRUMENT models."""

    def __init__(
        self,
        access_token: str,
        pg_conn: psycopg.AsyncConnection,
    ):
        self.kite_api = KITE_API(access_token=access_token)
        self.db_name = config("POSTGRES_DB", cast=str)
        self.schema_name = "public"
        self.table_name = "instruments"
        self.pg_conn = pg_conn

    async def download_instruments(self) -> int:

        print("[INSTRUMENT_DOWNLOADER_ASYNC] Starting instrument download...")
        logger.info("[INSTRUMENT_DOWNLOADER_ASYNC] Starting instrument download...")

        # Download fresh data (KITE_API is synchronous)
        instruments = self.kite_api.get_instruments()

        print(f"[INSTRUMENT_DOWNLOADER_ASYNC] Downloaded {len(instruments)} instruments")
        logger.info(f"[INSTRUMENT_DOWNLOADER_ASYNC] Downloaded {len(instruments)} instruments")

        if not instruments:
            print("[INSTRUMENT_DOWNLOADER_ASYNC] No instruments downloaded")
            logger.warning("[INSTRUMENT_DOWNLOADER_ASYNC] No instruments downloaded")
            return 0

        crud_pydantic = pg_crud.WITH_PYDANTIC(
            timezone="Asia/Kolkata",
            pg_conn=self.pg_conn,
        )

        if not await crud_pydantic.table_check(
            db_name=self.db_name,
            schema_name=self.schema_name,
            table_name=self.table_name,
        ):
            print(
                f"[INSTRUMENT_DOWNLOADER_ASYNC] Table {self.schema_name}.{self.table_name} does not exist. Creating..."
            )
            logger.info(f"[INSTRUMENT_DOWNLOADER_ASYNC] Table {self.schema_name}.{self.table_name} does not exist. Creating...")

            await crud_pydantic.table_create(
                db_name=self.db_name,
                schema_name=self.schema_name,
                table_name=self.table_name,
                model=models.STANDARD_INSTRUMENT,
            )
            print(
                f"[INSTRUMENT_DOWNLOADER_ASYNC] ✓ Created table {self.schema_name}.{self.table_name} for instruments"
            )
            logger.info(f"[INSTRUMENT_DOWNLOADER_ASYNC] ✓ Created table {self.schema_name}.{self.table_name} for instruments")

        else:
            print(
                f"[INSTRUMENT_DOWNLOADER_ASYNC] Table {self.schema_name}.{self.table_name} exists. Proceeding to truncate..."
            )
            logger.info(f"[INSTRUMENT_DOWNLOADER_ASYNC] Table {self.schema_name}.{self.table_name} exists. Proceeding to truncate...")

            await crud_pydantic.table_truncate(
                db_name=self.db_name,
                schema_name=self.schema_name,
                table_name=self.table_name,
            )

            print(
                f"[INSTRUMENT_DOWNLOADER_ASYNC] ✓ Truncated table {self.schema_name}.{self.table_name} for fresh insert"
            )
            logger.info(f"[INSTRUMENT_DOWNLOADER_ASYNC] ✓ Truncated table {self.schema_name}.{self.table_name} for fresh insert")

        await crud_pydantic.table_insert(
            db_name=self.db_name,
            schema_name=self.schema_name,
            table_name=self.table_name,
            model=models.STANDARD_INSTRUMENT,
            list_models=instruments,
        )

        print(f"[INSTRUMENT_DOWNLOADER_ASYNC] Inserted {len(instruments)} instruments")
        logger.info(f"[INSTRUMENT_DOWNLOADER_ASYNC] Inserted {len(instruments)} instruments")

        return len(instruments)


# -----------------------------------------------------------
