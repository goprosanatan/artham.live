# ===================================================================================================
# ===================================================================================================

import logging
import psycopg
from psycopg.sql import SQL, Identifier
import datetime

logger = logging.getLogger(__name__)

# ===================================================================================================


class TICK_LOADER:
    def __init__(
        self,
        pg_conn: psycopg.Connection,
        schema_name: str = "public",
        table_name: str = "ticks",
    ):
        self.pg_conn = pg_conn
        self.schema_name = schema_name
        self.table_name = table_name

    def load_ticks(self, start_dt: datetime.datetime, end_dt: datetime.datetime):

        with self.pg_conn.cursor() as cur:
            query = SQL(
                """
                SELECT * FROM {schema}.{table}
                WHERE ingest_ts >= %s AND ingest_ts <= %s
                ORDER BY ingest_ts ASC
                """
            ).format(
                schema=Identifier(self.schema_name), table=Identifier(self.table_name)
            )
            cur.execute(query, (start_dt, end_dt))
            return cur.fetchall()
