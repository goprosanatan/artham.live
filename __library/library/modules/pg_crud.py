# ===================================================================================================
# CRUD - POSTGRES (HERE TIMESCALEDB POSTGRES)
# ===================================================================================================

import os
import numpy as np
import pandas as pd
import logging
import asyncio
from decouple import config
import psycopg
from psycopg.rows import class_row, dict_row
from psycopg.sql import SQL, Identifier, Composed, Placeholder
from pydantic import BaseModel
from typing import Type, List, Optional, TypeVar, Any, Dict, Tuple, Literal

from library.modules import misc

logger = logging.getLogger(__name__)

# Generic TypeVar for Pydantic models
T = TypeVar("T", bound=BaseModel)

# ===================================================================================================


class GENERAL:
    def __init__(
        self,
        timezone: str = "UTC",
        pg_conn: Optional[psycopg.Connection] = None,
    ):
        print(f"[GENERAL] Initialized with timezone={timezone}")
        logger.info(f"[GENERAL] Initialized with timezone={timezone}")

        self.list_schema_default = [
            "pg_toast",
            "pg_catalog",
            "information_schema",
            "timescaledb_information",
            "timescaledb_experimental",
            "_timescaledb_cache",
            "_timescaledb_catalog",
            "_timescaledb_internal",
            "_timescaledb_config",
            "_timescaledb_functions",
            "_timescaledb_debug",
        ]

        # setting default timezone for data retrieval, so postgres converts data to required timezone
        self.timezone = timezone
        self.pg_conn = pg_conn  # ✅ Optional persistent connection

    def connect(
        self,
        db_name: str = "postgres",
        autocommit: bool = False,
    ):

        # make db name as lowercase , as database cannot have uppercase name
        db_name = db_name.lower()

        print(f"[GENERAL] Connecting to database: {db_name}")
        logger.info(f"[GENERAL] Connecting to database: {db_name}")

        pg_conn = psycopg.connect(
            host=config("POSTGRES_HOST", cast=str),
            port=config("POSTGRES_PORT", cast=str),
            user=config("POSTGRES_USER", cast=str),
            password=config("POSTGRES_PASSWORD", cast=str),
            dbname=db_name,
            autocommit=autocommit,
            options=f"-c timezone={self.timezone}",
        )

        print(f"[GENERAL] Connected to database: {db_name}")
        logger.info(f"[GENERAL] Connected to database: {db_name}")

        return pg_conn

    # -----------------------------------------------------------------------
    # DATABASE

    def database_get_all(self):
        """Get all non-template databases.
        Returns: List[str] - List of database names
        """
        pg_conn = self.connect(db_name="postgres")
        list_databases = []
        try:
            with pg_conn.cursor(row_factory=dict_row) as cursor:
                cursor.execute(
                    "SELECT datname FROM pg_database WHERE datistemplate = false;"
                )
                result = cursor.fetchall()

            list_databases = [d["datname"] for d in result]

        except Exception as e:
            print("❌ ERROR -", e)
            logger.error("❌ ERROR -", e)

        finally:
            pg_conn.close()

        return list_databases

    def database_create(
        self,
        db_name: str,
        timescaledb_extension: bool = False,
    ):
        """Create a new database.
        Args:
            db_name: Name of database to create (will be lowercased)
            timescaledb_extension: If True, install TimescaleDB extension
        """
        pg_conn = self.connect(db_name="postgres", autocommit=True)

        # make db name as lowercase , as database cannot have uppercase name
        db_name = db_name.lower()

        print(f"[GENERAL] Creating database: {db_name}")
        logger.info(f"[GENERAL] Creating database: {db_name}")

        try:
            with pg_conn.cursor() as cursor:
                cursor.execute(SQL("CREATE DATABASE " + db_name + ";"))

            print(f"[GENERAL] Database {db_name} created successfully.")
            logger.info(f"[GENERAL] Database {db_name} created successfully.")

        except psycopg.errors.DuplicateDatabase as e:
            print(f"[GENERAL] Database {db_name} already exists.")
            logger.warning(f"[GENERAL] Database {db_name} already exists.")

        except Exception as e:
            print(f"[GENERAL] Error creating database {db_name}: {e}")
            logger.error(f"[GENERAL] Error creating database {db_name}: {e}")

        finally:
            pg_conn.close()

        if timescaledb_extension:

            pg_conn = self.connect(db_name, autocommit=True)

            try:
                with pg_conn.cursor() as cursor:
                    cursor.execute(
                        SQL("""CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;""")
                    )

            except Exception as e:
                print("❌ ERROR -", e)
                logger.error("❌ ERROR -", e)

            finally:
                pg_conn.close()

    def database_delete(
        self,
        db_name: str,
    ):
        """Drop a database permanently.
        Args:
            db_name: Name of database to drop
        Returns: bool - True if successful, False otherwise
        """
        pg_conn = self.connect(db_name="postgres", autocommit=True)
        success = False

        print(f"[GENERAL] Deleting database: {db_name}")
        logger.info(f"[GENERAL] Deleting database: {db_name}")
        
        try:
            with pg_conn.cursor() as cursor:
                cursor.execute(SQL("DROP DATABASE " + db_name + ";"))

            success = True
            print(f"[GENERAL] Database {db_name} deleted successfully.")
            logger.info(f"[GENERAL] Database {db_name} deleted successfully.")

        except psycopg.errors.InvalidCatalogName:
            print(f"[GENERAL] Database {db_name} does not exist.")
            logger.warning(f"[GENERAL] Database {db_name} does not exist.")

        except Exception as e:
            print(f"[GENERAL] Error deleting database {db_name}: {e}")
            logger.error(f"[GENERAL] Error deleting database {db_name}: {e}")

        finally:
            pg_conn.close()

        return success

    def database_rename(
        self,
        db_name: str,
        db_name_new: str,
    ):
        """Rename an existing database.
        Returns: bool - True if successful, False otherwise
        """
        pg_conn = self.connect(db_name="postgres", autocommit=True)
        success = False

        try:
            with pg_conn.cursor() as cursor:
                cursor.execute(
                    "ALTER DATABASE " + db_name + " RENAME TO " + db_name_new + ";"
                )

            success = True

        except Exception as e:
            pg_conn.rollback()
            print("❌ ERROR -", e)
            logger.error("❌ ERROR -", e)

        finally:
            pg_conn.close()

        return success

    def database_vacuum(
        self,
        full: bool = False,
    ):
        """Optimize all databases by reclaiming space.
        Args:
            full: If True, perform VACUUM FULL (slower but more thorough)
        Returns: bool - True if successful
        """
        list_db = self.database_get_all()
        success = False

        for db_name in list_db:
            pg_conn = self.connect(db_name=db_name, autocommit=True)

            try:
                with pg_conn.cursor() as cursor:
                    # VACUUM Database
                    if full:
                        cursor.execute("VACUUM FULL;")
                    else:
                        cursor.execute("VACUUM;")

                success = True

            except Exception as e:
                print("❌ ERROR -", e)
                logger.error("❌ ERROR -", e)

            finally:
                pg_conn.close()

        return success

    # -----------------------------------------------------------------------
    # SCHEMA

    def schema_get_all(self, db_name: str):
        """Get all user-defined schemas (excludes system schemas).
        Returns: List[str] - List of schema names
        """
        conn = self.pg_conn or self.connect(db_name=db_name)
        local_conn = self.pg_conn is None
        list_schemas = []

        try:
            with conn.cursor(row_factory=dict_row) as cursor:

                cursor.execute("SELECT schema_name FROM information_schema.schemata;")

                result = cursor.fetchall()

            list_schemas = [d["schema_name"] for d in result]

            # remove default schemas present in postgres
            list_schemas = [
                schema
                for schema in list_schemas
                if schema not in self.list_schema_default
            ]

        except Exception as e:
            print("❌ ERROR -", e)
            logger.error("❌ ERROR -", e)

        finally:
            if local_conn:
                conn.close()

        return list_schemas

    def schema_create(
        self,
        db_name: str,
        schema_name: str,
    ):
        """Create a new schema.
        Returns: bool - True if successful, False otherwise
        """
        conn = self.pg_conn or self.connect(db_name=db_name)
        local_conn = self.pg_conn is None
        success = False

        try:
            with conn.cursor() as cursor:

                cursor.execute(
                    SQL("""CREATE SCHEMA IF NOT EXISTS {0};""").format(
                        Identifier(schema_name)
                    )
                )

            conn.commit()
            success = True

        except Exception as e:
            conn.rollback()
            print("❌ ERROR -", e)
            logger.error("❌ ERROR -", e)

        finally:
            if local_conn:
                conn.close()

        return success

    def schema_delete(
        self,
        db_name: str,
        schema_name: str,
    ):
        """Drop a schema and all its contents (CASCADE).
        Returns: bool - True if successful, False otherwise
        """
        conn = self.pg_conn or self.connect(db_name=db_name)
        local_conn = self.pg_conn is None
        success = False

        try:
            with conn.cursor() as cursor:

                cursor.execute(
                    SQL("""DROP SCHEMA IF EXISTS {0} CASCADE;""").format(
                        Identifier(schema_name)
                    )
                )

            conn.commit()
            success = True

        except Exception as e:
            conn.rollback()
            print("❌ ERROR -", e)
            logger.error("❌ ERROR -", e)

        finally:
            if local_conn:
                conn.close()

        return success

    def schema_rename(
        self,
        db_name: str,
        schema_name: str,
        schema_name_new: str,
    ):
        """Rename an existing schema.
        Returns: bool - True if successful, False otherwise
        """
        conn = self.pg_conn or self.connect(db_name=db_name, autocommit=True)
        local_conn = self.pg_conn is None
        success = False

        try:
            with conn.cursor() as cursor:

                cursor.execute(
                    SQL("""ALTER SCHEMA {0} RENAME TO {1};""").format(
                        Identifier(schema_name), Identifier(schema_name_new)
                    )
                )

            success = True

        except Exception as e:
            conn.rollback()
            print("❌ ERROR -", e)
            logger.error("❌ ERROR -", e)

        finally:
            if local_conn:
                conn.close()

        return success

    # -----------------------------------------------------------------------
    # Table

    def table_get_all(
        self,
        db_name: str,
        schema_name: str = "public",
    ):
        """Get all tables in a schema.
        Returns: List[str] - List of table names
        """
        conn = self.pg_conn or self.connect(db_name=db_name)
        local_conn = self.pg_conn is None
        list_tables = []

        try:
            with conn.cursor(row_factory=dict_row) as cursor:

                cursor.execute(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = %s;",
                    (schema_name,),
                )

                result = cursor.fetchall()

            list_tables = [d["table_name"] for d in result]

        except Exception as e:
            conn.rollback()
            print("❌ ERROR -", e)
            logger.error("❌ ERROR -", e)

        finally:
            if local_conn:
                conn.close()

        return list_tables

    def table_delete(
        self,
        db_name: str,
        schema_name: str,
        table_name: str,
    ):
        """Drop a table permanently (removes structure and data).
        ⚠️ This is irreversible. Use CASCADE to drop dependent objects.
        Returns: bool - True if successful, False otherwise
        """
        conn = self.pg_conn or self.connect(db_name=db_name)
        local_conn = self.pg_conn is None
        print(f"[GENERAL] Deleting table: {schema_name}.{table_name}")
        logger.info(f"[GENERAL] Deleting table: {schema_name}.{table_name}")
        success = False

        try:
            with conn.cursor() as cursor:

                cursor.execute(
                    SQL("""DROP TABLE {0}.{1} CASCADE;""").format(
                        Identifier(schema_name), Identifier(table_name)
                    )
                )

            conn.commit()
            success = True
            print(f"[GENERAL] Table {schema_name}.{table_name} deleted successfully.")
            logger.info(f"[GENERAL] Table {schema_name}.{table_name} deleted successfully.")

        except Exception as e:
            print(f"[GENERAL] Error deleting table {schema_name}.{table_name}: {e}")
            logger.error(f"[GENERAL] Error deleting table {schema_name}.{table_name}: {e}")
            conn.rollback()

        finally:
            if local_conn:
                conn.close()

        return success

    def table_truncate(
        self,
        db_name: str,
        schema_name: str,
        table_name: str,
    ):
        """Fast clear all rows from a table using TRUNCATE.
        ✅ Much faster than DELETE for large tables
        ✅ Resets identity/sequence counters
        ❌ Cannot be rolled back after commit
        Returns: bool - True if successful, False otherwise
        """
        conn = self.pg_conn or self.connect(db_name=db_name)
        local_conn = self.pg_conn is None
        success = False

        try:
            with conn.cursor() as cursor:

                cursor.execute(
                    SQL("""TRUNCATE TABLE {0}.{1} CASCADE;""").format(
                        Identifier(schema_name), Identifier(table_name)
                    )
                )

            conn.commit()
            success = True

        except Exception as e:
            conn.rollback()
            print("❌ ERROR -", e)
            logger.error("❌ ERROR -", e)

        finally:
            if local_conn:
                conn.close()

        return success

    def table_rename(
        self,
        db_name: str,
        schema_name: str,
        table_name: str,
        table_name_new: str,
    ):
        """Rename an existing table.
        Returns: bool - True if successful, False otherwise
        """
        conn = self.pg_conn or self.connect(db_name=db_name, autocommit=True)
        local_conn = self.pg_conn is None
        success = False

        try:
            with conn.cursor() as cursor:

                # change table name
                cursor.execute(
                    SQL("""ALTER TABLE {0}.{1} RENAME TO {2};""").format(
                        Identifier(schema_name),
                        Identifier(table_name),
                        Identifier(table_name_new),
                    )
                )

            success = True

        except Exception as e:
            conn.rollback()
            print("❌ ERROR -", e)
            logger.error("❌ ERROR -", e)

        finally:
            if local_conn:
                conn.close()

        return success

    def table_check(
        self,
        db_name: str,
        schema_name: str,
        table_name: str,
    ):
        """Check if a table exists in the schema.
        Returns: bool - True if table exists, False otherwise
        """
        conn = self.pg_conn or self.connect(db_name=db_name)
        local_conn = self.pg_conn is None
        answer = False

        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    SQL(
                        """
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables
                            WHERE  table_schema = %s
                            AND    table_name   = %s
                        );
                        """
                    ),
                    (
                        schema_name,
                        table_name,
                    ),
                )

                answer = cursor.fetchall()[0][0]

        except Exception as e:
            print("❌ ERROR -", e)
            logger.error("❌ ERROR -", e)

        finally:
            if local_conn:
                conn.close()

        return answer

    def table_get(
        self,
        db_name: str,
        schema_name: str,
        table_name: str,
    ):
        """Fetch all rows from a table.
        Returns: List[dict] - List of row dictionaries
        """
        conn = self.pg_conn or self.connect(db_name=db_name)
        local_conn = self.pg_conn is None
        result = []

        try:
            with conn.cursor(row_factory=dict_row) as cursor:
                cursor.execute(
                    SQL("""SELECT * FROM {0}.{1};""").format(
                        Identifier(schema_name), Identifier(table_name)
                    )
                )
                result = cursor.fetchall()

        except Exception as e:
            print("❌ ERROR -", e)
            logger.error("❌ ERROR -", e)

        finally:
            if local_conn:
                conn.close()

        return result

    def table_move(
        self,
        db_name: str,
        schema_name: str,
        table_name: str,
        schema_name_new: str,
    ):
        """Move a table from one schema to another.
        Returns: bool - True if successful, False otherwise
        """
        conn = self.pg_conn or self.connect(db_name=db_name, autocommit=True)
        local_conn = self.pg_conn is None
        success = False

        try:
            with conn.cursor() as cursor:

                # change table name
                cursor.execute(
                    SQL("""ALTER TABLE {0}.{1} SET SCHEMA {2};""").format(
                        Identifier(schema_name),
                        Identifier(table_name),
                        Identifier(schema_name_new),
                    )
                )

            success = True

        except Exception as e:
            conn.rollback()
            print("❌ ERROR -", e)
            logger.error("❌ ERROR -", e)

        finally:
            if local_conn:
                conn.close()

        return success

    def table_download(
        self,
        db_name: str,
        schema_name: str,
        table_name: str,
        directory: str,
    ):
        """Export table data to CSV file.
        Creates file at: {directory}/{db_name}/{schema_name}/{table_name}.csv
        """
        # create the file path for the
        file_path = os.path.join(
            directory,
            db_name,
            schema_name,
            table_name + ".csv",
        )

        # create folder if not exists
        misc.mkdir(file_path)

        # get dataframe
        result = self.table_get(db_name, schema_name, table_name)
        df = pd.DataFrame(result)

        # download df
        df.to_csv(file_path, index=False)

    def table_select_one(
        self,
        db_name: str,
        schema_name: str,
        table_name: str,
        filters: Optional[Dict[str, Any]] = None,
        order_by: Optional[List[Tuple[str, Literal["ASC", "DESC"]]]] = None,
    ):

        conn = self.pg_conn or self.connect(db_name=db_name)
        local_conn = self.pg_conn is None

        try:
            with conn.cursor(row_factory=dict_row) as cur:
                base = SQL("SELECT * FROM {schema}.{table}").format(
                    schema=Identifier(schema_name),
                    table=Identifier(table_name),
                )

                where_parts: List[Composed] = []
                params: List[Any] = []

                if filters:
                    for col, val in filters.items():
                        if val is None:
                            where_parts.append(
                                Composed([Identifier(col), SQL(" IS NULL")])
                            )
                        else:
                            where_parts.append(
                                Composed([Identifier(col), SQL(" = %s")])
                            )
                            params.append(val)

                if where_parts:
                    base = base + SQL(" WHERE ") + SQL(" AND ").join(where_parts)

                if order_by:
                    order_parts: List[Composed] = []
                    for col, direction in order_by:
                        dir_sql = "ASC" if str(direction).upper() == "ASC" else "DESC"
                        order_parts.append(
                            Composed([Identifier(col), SQL(f" {dir_sql}")])
                        )
                    base = base + SQL(" ORDER BY ") + SQL(", ").join(order_parts)

                base = base + SQL(" LIMIT 1;")

                if params:
                    cur.execute(base, tuple(params))
                else:
                    cur.execute(base)

                row = cur.fetchone()

                if not row:
                    return None

                return row

        except Exception as e:
            print("❌ ERROR -", e)
            logger.error("❌ ERROR -", e)
            return None

        finally:
            if local_conn:
                conn.close()

    def table_get_columns(
        self,
        db_name: str,
        schema_name: str,
        table_name: str,
    ):
        """Get all column names in a table.
        Returns: List[str] - List of column names
        """
        conn = self.pg_conn or self.connect(db_name=db_name)
        local_conn = self.pg_conn is None
        list_table_columns = []

        try:
            with conn.cursor(row_factory=dict_row) as cursor:

                cursor.execute(
                    SQL(
                        """
                        SELECT column_name FROM information_schema.columns
                            WHERE  table_schema = %s
                            AND table_name = %s;
                        """
                    ),
                    (
                        schema_name,
                        table_name,
                    ),
                )

                result = cursor.fetchall()

            list_table_columns = [d["column_name"] for d in result]

        except Exception as e:
            print("❌ ERROR -", e)
            logger.error("❌ ERROR -", e)

        finally:
            if local_conn:
                conn.close()

        return list_table_columns

    def table_get_row_count(
        self,
        db_name: str,
        schema_name: str,
        table_name: str,
    ):
        """Get total row count in a table.
        Returns: int - Number of rows, or None if table doesn't exist
        """
        conn = self.pg_conn or self.connect(db_name=db_name)
        local_conn = self.pg_conn is None
        row_count = None

        try:
            if self.table_check(db_name, schema_name, table_name):

                with conn.cursor(row_factory=dict_row) as cursor:

                    cursor.execute(
                        SQL("""SELECT COUNT(*) FROM {0}.{1};""").format(
                            Identifier(schema_name), Identifier(table_name)
                        )
                    )

                    result = cursor.fetchall()

                row_count = result[0]["count"]

        except Exception as e:
            print("❌ ERROR -", e)
            logger.error("❌ ERROR -", e)

        finally:
            if local_conn:
                conn.close()

        return row_count

    def table_delete_column(
        self,
        db_name: str,
        schema_name: str,
        table_name: str,
        column_name: str,
    ):
        """Set all values in a column to NULL (soft delete).
        Note: Doesn't drop the column, just clears values.
        Returns: bool - True if successful, False otherwise
        """
        conn = self.pg_conn or self.connect(db_name=db_name)
        local_conn = self.pg_conn is None
        success = False

        try:
            with conn.cursor() as cursor:

                cursor.execute(
                    SQL("""UPDATE {0}.{1} SET {2} = null;""").format(
                        Identifier(schema_name),
                        Identifier(table_name),
                        Identifier(column_name),
                    )
                )

            conn.commit()
            success = True

        except Exception as e:
            conn.rollback()
            print("❌ ERROR -", e)
            logger.error("❌ ERROR -", e)

        finally:
            if local_conn:
                conn.close()

        return success

    def table_get_column(
        self,
        db_name: str,
        schema_name: str,
        table_name: str,
        column_name: str,
    ):
        """Get all values from a specific column.
        Returns: List - List of column values
        """
        conn = self.pg_conn or self.connect(db_name=db_name)
        local_conn = self.pg_conn is None
        list_column = []

        try:
            with conn.cursor(row_factory=dict_row) as cursor:

                cursor.execute(
                    SQL("""SELECT {0} FROM {1}.{2};""").format(
                        Identifier(column_name),
                        Identifier(schema_name),
                        Identifier(table_name),
                    )
                )

                result = cursor.fetchall()

            list_column = [d[column_name] for d in result]

        except Exception as e:
            print("❌ ERROR -", e)
            logger.error("❌ ERROR -", e)

        finally:
            if local_conn:
                conn.close()

        return list_column

    def table_set_column_value(
        self,
        db_name: str,
        schema_name: str,
        table_name: str,
        column_name: str,
        value,
        where: Optional[dict] = None,
    ) -> bool:
        """
        Update a single column in a table to a given value.
        Optionally filter rows using a WHERE dict of {col: val} conditions.

        Example:
            table_set_column_value("db", "public", "users", "status", "inactive")
            table_set_column_value("db", "public", "orders", "archived", True, {"user_id": 42})
        """
        conn = self.pg_conn or self.connect(db_name=db_name)
        local_conn = self.pg_conn is None
        success = False

        try:
            with conn.cursor() as cursor:
                # Base query: UPDATE schema.table SET column = %s
                query = SQL("UPDATE {schema}.{table} SET {col} = %s").format(
                    schema=Identifier(schema_name),
                    table=Identifier(table_name),
                    col=Identifier(column_name),
                )
                params = [value]

                # Optional WHERE
                if where:
                    conditions = [
                        Composed([Identifier(k), SQL(" = %s")]) for k in where.keys()
                    ]
                    query = query + SQL(" WHERE ") + SQL(" AND ").join(conditions)
                    params.extend(where.values())

                cursor.execute(query, params)

            conn.commit()
            success = True

        except Exception as e:
            conn.rollback()
            print("❌ ERROR -", e)
            logger.error("❌ ERROR -", e)

        finally:
            if local_conn:
                conn.close()

        return success

    def table_get_column_distinct(
        self,
        db_name: str,
        schema_name: str,
        table_name: str,
        column_name: str,
    ):
        """Get unique values from a column.
        Returns: List - List of distinct column values
        """
        conn = self.pg_conn or self.connect(db_name=db_name)
        local_conn = self.pg_conn is None
        list_distinct = []

        try:
            with conn.cursor(row_factory=dict_row) as cursor:

                cursor.execute(
                    SQL("""SELECT DISTINCT {0} FROM {1}.{2};""").format(
                        Identifier(column_name),
                        Identifier(schema_name),
                        Identifier(table_name),
                    )
                )

                result = cursor.fetchall()

            list_distinct = [d[column_name] for d in result]

        except Exception as e:
            print("❌ ERROR -", e)
            logger.error("❌ ERROR -", e)

        finally:
            if local_conn:
                conn.close()

        return list_distinct

    def table_to_timescaledb_hypertable(
        self,
        db_name: str,
        schema_name: str,
        table_name: str,
        time_column: str,
        *,
        partitioning_column: Optional[str] = None,
        number_partitions: Optional[int] = None,
        associated_schema: Optional[str] = None,
        associated_table_prefix: Optional[str] = None,
        chunk_time_interval: Optional[
            object
        ] = None,  # str interval like '1 day' or integer
        create_default_indexes: Optional[bool] = None,
        if_not_exists: Optional[bool] = True,
        partitioning_func: Optional[str] = None,
        time_partitioning_func: Optional[str] = None,
        migrate_data: Optional[bool] = None,
        copy_indexes: Optional[bool] = None,
        # Distributed options (use create_distributed_hypertable)
        distributed: Optional[bool] = None,
        data_nodes: Optional[List[str]] = None,
        replication_factor: Optional[int] = None,
        # Policies / compression options
        compression_enabled: Optional[bool] = None,
        compress_segmentby: Optional[object] = None,  # list[str] or str
        compress_orderby: Optional[str] = None,
        compression_interval: Optional[str] = None,  # e.g., '7 days'
        retention_interval: Optional[str] = None,  # e.g., '90 days'
        reorder_index: Optional[str] = None,
    ) -> bool:
        """Create a (distributed) hypertable with optional attributes and policies.

        Only provided options are applied. Supports TimescaleDB parameters like:
        - partitioning_column, number_partitions, chunk_time_interval
        - create_default_indexes, if_not_exists, partitioning_func, time_partitioning_func
        - migrate_data, copy_indexes
        - distributed, data_nodes, replication_factor (uses create_distributed_hypertable)
        - compression (timescaledb.compress + policies), retention, reorder policy
        """
        conn = self.pg_conn or self.connect(db_name=db_name)
        local_conn = self.pg_conn is None
        success = False

        try:
            with conn.cursor() as cursor:
                full_table_name = f"{schema_name}.{table_name}"

                # Choose function based on distributed flags
                use_distributed = (
                    bool(distributed)
                    or bool(data_nodes)
                    or (replication_factor is not None)
                )
                fn_sql = (
                    SQL("create_distributed_hypertable")
                    if use_distributed
                    else SQL("create_hypertable")
                )

                # Base signature
                query = SQL("SELECT ") + fn_sql + SQL("(%s, %s")
                params: List[object] = [full_table_name, time_column]

                def add_param(name: str, value: object, cast: Optional[str] = None):
                    nonlocal query
                    if value is None:
                        return
                    query += SQL(", ") + SQL(name) + SQL(" => ")
                    if cast:
                        query += SQL("%s::" + cast)
                    else:
                        query += SQL("%s")
                    params.append(value)

                # Optional parameters
                add_param("partitioning_column", partitioning_column)
                add_param("number_partitions", number_partitions)
                add_param("associated_schema", associated_schema)
                add_param("associated_table_prefix", associated_table_prefix)

                # chunk_time_interval can be INTERVAL or BIGINT depending on time type
                if chunk_time_interval is not None:
                    if isinstance(chunk_time_interval, (int, float)):
                        add_param("chunk_time_interval", int(chunk_time_interval))
                    else:
                        add_param(
                            "chunk_time_interval", chunk_time_interval, cast="interval"
                        )

                add_param("create_default_indexes", create_default_indexes)
                # Default if_not_exists to True unless explicitly set False
                if if_not_exists is not None:
                    add_param("if_not_exists", if_not_exists)
                else:
                    add_param("if_not_exists", True)

                add_param("partitioning_func", partitioning_func)
                add_param("time_partitioning_func", time_partitioning_func)
                add_param("migrate_data", migrate_data)
                add_param("copy_indexes", copy_indexes)

                # Distributed-only extras
                if use_distributed:
                    if data_nodes:
                        query += (
                            SQL(", data_nodes => ARRAY[")
                            + SQL(", ").join(SQL("%s") for _ in data_nodes)
                            + SQL("]")
                        )
                        params.extend(list(data_nodes))
                    add_param("replication_factor", replication_factor)

                query += SQL(");")
                cursor.execute(query, params)
                print(
                    f"✅ Hypertable created for {full_table_name} (time column: {time_column})"
                )
                logger.info(
                    f"✅ Hypertable created for {full_table_name} (time column: {time_column})"
                )

                # Optional compression settings via ALTER TABLE
                if compression_enabled or compress_segmentby or compress_orderby:
                    opts: List[Composed] = []
                    # enable compression
                    opts.append(SQL("timescaledb.compress"))
                    if compress_segmentby:
                        if isinstance(compress_segmentby, (list, tuple)):
                            seg = ",".join(str(c) for c in compress_segmentby)
                        else:
                            seg = str(compress_segmentby)
                        opts.append(
                            SQL("timescaledb.compress_segmentby = ") + SQL("%s")
                        )
                    if compress_orderby:
                        opts.append(SQL("timescaledb.compress_orderby = ") + SQL("%s"))

                    alter = (
                        SQL("ALTER TABLE {}.{} SET (").format(
                            Identifier(schema_name), Identifier(table_name)
                        )
                        + SQL(", ").join(opts)
                        + SQL(");")
                    )
                    alter_params: List[object] = []
                    # First param in opts is bare flag timescaledb.compress (no value)
                    # If we provided values above, extend params accordingly
                    if compress_segmentby:
                        alter_params.append(
                            ",".join(compress_segmentby)
                            if isinstance(compress_segmentby, (list, tuple))
                            else str(compress_segmentby)
                        )
                    if compress_orderby:
                        alter_params.append(str(compress_orderby))
                    cursor.execute(alter, alter_params)

                # Optional policies
                if compression_interval:
                    q = SQL("SELECT add_compression_policy(%s, %s::interval);")
                    cursor.execute(q, (full_table_name, compression_interval))

                if retention_interval:
                    q = SQL("SELECT add_retention_policy(%s, %s::interval);")
                    cursor.execute(q, (full_table_name, retention_interval))

                if reorder_index:
                    q = SQL("SELECT add_reorder_policy({}.{}, %s);").format(
                        Identifier(schema_name), Identifier(table_name)
                    )
                    cursor.execute(q, (reorder_index,))

            conn.commit()
            success = True
        except Exception as e:
            conn.rollback()
            print("❌ ERROR -", e)
            logger.error("❌ ERROR -", e)
        finally:
            if local_conn:
                conn.close()

        return success

    # -----------------------------------------------------------------------
    # Materialized View

    def mview_get(
        self,
        db_name: str,
        schema_name: str,
        materialized_view_name: str,
    ):
        """Fetch all rows from a materialized view.
        Returns: List[dict] - List of row dictionaries
        """
        conn = self.pg_conn or self.connect(db_name=db_name)
        local_conn = self.pg_conn is None
        result = []

        try:
            with conn.cursor(row_factory=dict_row) as cursor:

                cursor.execute(
                    SQL("""SELECT * FROM {0}.{1};""").format(
                        Identifier(schema_name),
                        Identifier(materialized_view_name),
                    )
                )

                result = cursor.fetchall()

        except Exception as e:
            conn.rollback()
            print("❌ ERROR -", e)
            logger.error("❌ ERROR -", e)

        finally:
            if local_conn:
                conn.close()

        return result

    def mview_delete(
        self,
        db_name: str,
        schema_name: str,
        materialized_view_name: str,
    ):
        """Drop a materialized view.
        Returns: bool - True if successful, False otherwise
        """
        conn = self.pg_conn or self.connect(db_name=db_name)
        local_conn = self.pg_conn is None
        success = False

        try:
            with conn.cursor(row_factory=dict_row) as cursor:

                cursor.execute(
                    SQL("""DROP MATERIALIZED VIEW IF EXISTS {0}.{1} CASCADE;""").format(
                        Identifier(schema_name), Identifier(materialized_view_name)
                    )
                )

            conn.commit()
            success = True

        except Exception as e:
            conn.rollback()
            print("❌ ERROR -", e)
            logger.error("❌ ERROR -", e)

        finally:
            if local_conn:
                conn.close()

        return success

    def mview_refresh(
        self,
        db_name: str,
        schema_name: str,
        materialized_view_name: str,
        concurrently: bool = False,
    ):
        """
        Refresh a materialized view.
        Args:
            db_name (str): Database name
            schema_name (str): Schema name
            materialized_view_name (str): Materialized view name
            concurrently (bool): Use CONCURRENTLY option
        Returns: bool - True if successful, False otherwise
        """
        conn = self.pg_conn or self.connect(db_name=db_name)
        local_conn = self.pg_conn is None
        success = False

        try:
            with conn.cursor() as cursor:
                if concurrently:
                    cursor.execute(
                        SQL("REFRESH MATERIALIZED VIEW CONCURRENTLY {0}.{1};").format(
                            Identifier(schema_name), Identifier(materialized_view_name)
                        )
                    )
                else:
                    cursor.execute(
                        SQL("REFRESH MATERIALIZED VIEW {0}.{1};").format(
                            Identifier(schema_name), Identifier(materialized_view_name)
                        )
                    )
            conn.commit()
            success = True
        except Exception as e:
            conn.rollback()
            print(
                f"❌ ERROR refreshing materialized view {schema_name}.{materialized_view_name} -",
                e,
            )
            logger.error(
                f"❌ ERROR refreshing materialized view {schema_name}.{materialized_view_name} -",
                e,
            )
        finally:
            if local_conn:
                conn.close()
        return success


# --------------------------------------------------------------------------------------------------


class GENERAL_ASYNC:
    def __init__(
        self,
        timezone: str = "UTC",
        pg_conn: Optional[psycopg.AsyncConnection] = None,
    ):
        self.list_schema_default = [
            "pg_toast",
            "pg_catalog",
            "information_schema",
            "timescaledb_information",
            "timescaledb_experimental",
            "_timescaledb_cache",
            "_timescaledb_catalog",
            "_timescaledb_internal",
            "_timescaledb_config",
            "_timescaledb_functions",
            "_timescaledb_debug",
        ]
        self.timezone = timezone
        self.pg_conn = pg_conn

    async def connect(
        self,
        db_name: str = "postgres",
        autocommit: bool = False,
    ):
        db_name = db_name.lower()
        pg_conn = await psycopg.AsyncConnection.connect(
            host=config("POSTGRES_HOST", cast=str),
            port=config("POSTGRES_PORT", cast=str),
            user=config("POSTGRES_USER", cast=str),
            password=config("POSTGRES_PASSWORD", cast=str),
            dbname=db_name,
            autocommit=autocommit,
            options=f"-c timezone={self.timezone}",
        )
        return pg_conn

    # -----------------------------------------------------------------------
    # DATABASE
    async def database_get_all(self):
        """Get all non-template databases (async).
        Returns: List[str] - List of database names
        """
        pg_conn = await self.connect(db_name="postgres")
        list_databases = []
        try:
            async with pg_conn.cursor(row_factory=dict_row) as cursor:
                await cursor.execute(
                    "SELECT datname FROM pg_database WHERE datistemplate = false;"
                )
                result = await cursor.fetchall()
            list_databases = [d["datname"] for d in result]
        except Exception as e:
            print("❌ ERROR -", e)
            logger.error("❌ ERROR -", e)
        finally:
            await pg_conn.close()
        return list_databases

    async def database_create(
        self,
        db_name: str,
        timescaledb_extension: bool = False,
    ):
        """Create a new database (async).
        Args:
            db_name: Name of database to create (will be lowercased)
            timescaledb_extension: If True, install TimescaleDB extension
        """
        pg_conn = await self.connect(db_name="postgres", autocommit=True)
        db_name = db_name.lower()
        print(f"[GENERAL] Creating database: {db_name}") 
        logger.info(f"[GENERAL] Creating database: {db_name}")
        try:
            async with pg_conn.cursor() as cursor:
                await cursor.execute(SQL("CREATE DATABASE " + db_name + ";"))
        except psycopg.errors.DuplicateDatabase as e:
            print(f"[GENERAL] Database {db_name} already exists.")
            logger.warning(f"[GENERAL] Database {db_name} already exists.")
        except Exception as e:
            print(f"[GENERAL] Error creating database {db_name}: {e}")
            logger.error(f"[GENERAL] Error creating database {db_name}: {e}")
        finally:
            await pg_conn.close()

        if timescaledb_extension:
            pg_conn = await self.connect(db_name, autocommit=True)
            try:
                async with pg_conn.cursor() as cursor:
                    await cursor.execute(
                        SQL("""CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;""")
                    )
            except Exception as e:
                print("❌ ERROR -", e)
                logger.error("❌ ERROR -", e)
            finally:
                await pg_conn.close()

    async def database_delete(self, db_name: str):
        """Drop a database permanently (async).
        Args:
            db_name: Name of database to drop
        Returns: bool - True if successful, False otherwise
        """
        pg_conn = await self.connect(db_name="postgres", autocommit=True)
        print(f"[GENERAL] Deleting database: {db_name}")
        logger.info(f"[GENERAL] Deleting database: {db_name}")
        success = False
        try:
            async with pg_conn.cursor() as cursor:
                await cursor.execute(SQL("DROP DATABASE " + db_name + ";"))
            success = True
            print(f"[GENERAL] Database {db_name} deleted successfully.")
            logger.info(f"[GENERAL] Database {db_name} deleted successfully.")
        except psycopg.errors.InvalidCatalogName:
            print(f"[GENERAL] Database {db_name} does not exist.")
            logger.warning(f"[GENERAL] Database {db_name} does not exist.")
        except Exception as e:
            print(f"[GENERAL] Error deleting database {db_name}: {e}")
            logger.error(f"[GENERAL] Error deleting database {db_name}: {e}")
        finally:
            await pg_conn.close()
        return success

    async def database_rename(self, db_name: str, db_name_new: str):
        """Rename an existing database (async).
        Returns: bool - True if successful, False otherwise
        """
        pg_conn = await self.connect(db_name="postgres", autocommit=True)
        success = False

        try:
            async with pg_conn.cursor() as cursor:
                await cursor.execute(
                    "ALTER DATABASE " + db_name + " RENAME TO " + db_name_new + ";"
                )
            success = True
        except Exception as e:
            await pg_conn.rollback()
            print("❌ ERROR -", e)
            logger.error("❌ ERROR -", e)
        finally:
            await pg_conn.close()
        return success

    async def database_vacuum(self, full: bool = False):
        """Optimize all databases by reclaiming space (async).
        Args:
            full: If True, perform VACUUM FULL (slower but more thorough)
        Returns: bool - True if successful
        """
        list_db = await self.database_get_all()
        success = False
        for db_name in list_db:
            pg_conn = await self.connect(db_name=db_name, autocommit=True)
            try:
                async with pg_conn.cursor() as cursor:
                    if full:
                        await cursor.execute("VACUUM FULL;")
                    else:
                        await cursor.execute("VACUUM;")
                success = True
            except Exception as e:
                print("❌ ERROR -", e)
                logger.error("❌ ERROR -", e)
            finally:
                await pg_conn.close()
        return success

    # -----------------------------------------------------------------------
    # SCHEMA
    async def schema_get_all(self, db_name: str):
        """Get all user-defined schemas (excludes system schemas) (async).
        Returns: List[str] - List of schema names
        """
        conn = self.pg_conn or await self.connect(db_name=db_name)
        local_conn = self.pg_conn is None
        list_schemas = []
        try:
            async with conn.cursor(row_factory=dict_row) as cursor:
                await cursor.execute(
                    "SELECT schema_name FROM information_schema.schemata;"
                )
                result = await cursor.fetchall()
            list_schemas = [d["schema_name"] for d in result]
            list_schemas = [
                schema
                for schema in list_schemas
                if schema not in self.list_schema_default
            ]
        except Exception as e:
            print("❌ ERROR -", e)
            logger.error("❌ ERROR -", e)
        finally:
            if local_conn:
                await conn.close()
        return list_schemas

    async def schema_create(self, db_name: str, schema_name: str):
        """Create a new schema (async).
        Returns: bool - True if successful, False otherwise
        """
        conn = self.pg_conn or await self.connect(db_name=db_name)
        local_conn = self.pg_conn is None
        success = False

        try:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    SQL("""CREATE SCHEMA IF NOT EXISTS {0};""").format(
                        Identifier(schema_name)
                    )
                )
            await conn.commit()
            success = True
        except Exception as e:
            await conn.rollback()
            print("❌ ERROR -", e)
            logger.error("❌ ERROR -", e)
        finally:
            if local_conn:
                await conn.close()
        return success

    async def schema_delete(self, db_name: str, schema_name: str):
        """Drop a schema and all its contents (CASCADE) (async).
        Returns: bool - True if successful, False otherwise
        """
        conn = self.pg_conn or await self.connect(db_name=db_name)
        local_conn = self.pg_conn is None
        success = False

        try:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    SQL("""DROP SCHEMA IF EXISTS {0} CASCADE;""").format(
                        Identifier(schema_name)
                    )
                )
            await conn.commit()
            success = True
        except Exception as e:
            await conn.rollback()
            print("❌ ERROR -", e)
            logger.error("❌ ERROR -", e)
        finally:
            if local_conn:
                await conn.close()
        return success

    async def schema_rename(self, db_name: str, schema_name: str, schema_name_new: str):
        """Rename an existing schema (async).
        Returns: bool - True if successful, False otherwise
        """
        conn = self.pg_conn or await self.connect(db_name=db_name, autocommit=True)
        local_conn = self.pg_conn is None
        success = False

        try:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    SQL("""ALTER SCHEMA {0} RENAME TO {1};""").format(
                        Identifier(schema_name), Identifier(schema_name_new)
                    )
                )
            success = True
        except Exception as e:
            await conn.rollback()
            print("❌ ERROR -", e)
            logger.error("❌ ERROR -", e)
        finally:
            if local_conn:
                await conn.close()
        return success

    # -----------------------------------------------------------------------
    # TABLE
    async def table_get_all(self, db_name: str, schema_name: str = "public"):
        """Get all tables in a schema (async).
        Returns: List[str] - List of table names
        """
        conn = self.pg_conn or await self.connect(db_name=db_name)
        local_conn = self.pg_conn is None
        list_tables = []
        try:
            async with conn.cursor(row_factory=dict_row) as cursor:
                await cursor.execute(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = %s;",
                    (schema_name,),
                )
                result = await cursor.fetchall()
            list_tables = [d["table_name"] for d in result]
        except Exception as e:
            await conn.rollback()
            print("❌ ERROR -", e)
            logger.error("❌ ERROR -", e)
        finally:
            if local_conn:
                await conn.close()
        return list_tables

    async def table_delete(self, db_name: str, schema_name: str, table_name: str):
        """Drop a table permanently (removes structure and data) (async).
        ⚠️ This is irreversible. Use CASCADE to drop dependent objects.
        Returns: bool - True if successful, False otherwise
        """
        conn = self.pg_conn or await self.connect(db_name=db_name)
        local_conn = self.pg_conn is None
        print(f"[GENERAL] Deleting table: {schema_name}.{table_name}")
        logger.info(f"[GENERAL] Deleting table: {schema_name}.{table_name}")
        success = False

        try:
            async with conn.cursor() as cursor:

                await cursor.execute(
                    SQL("""DROP TABLE {0}.{1} CASCADE;""").format(
                        Identifier(schema_name), Identifier(table_name)
                    )
                )

            await conn.commit()
            success = True
            print(f"[GENERAL] Table {schema_name}.{table_name} deleted successfully.")
            logger.info(f"[GENERAL] Table {schema_name}.{table_name} deleted successfully.")

        except Exception as e:
            await conn.rollback()
            print(f"[GENERAL] Error deleting table {schema_name}.{table_name}: {e}")
            logger.error(f"[GENERAL] Error deleting table {schema_name}.{table_name}: {e}")

        finally:
            if local_conn:
                await conn.close()

        return success

    async def table_truncate(self, db_name: str, schema_name: str, table_name: str):
        """Fast clear all rows from a table using TRUNCATE (async).
        ✅ Much faster than DELETE for large tables
        ✅ Resets identity/sequence counters
        ❌ Cannot be rolled back after commit
        Returns: bool - True if successful, False otherwise
        """
        conn = self.pg_conn or await self.connect(db_name=db_name)
        local_conn = self.pg_conn is None
        success = False

        try:
            async with conn.cursor() as cursor:

                await cursor.execute(
                    SQL("""TRUNCATE TABLE {0}.{1} CASCADE;""").format(
                        Identifier(schema_name), Identifier(table_name)
                    )
                )

            await conn.commit()
            success = True
        except Exception as e:
            await conn.rollback()
            print("❌ ERROR -", e)
            logger.error("❌ ERROR -", e)
        finally:
            if local_conn:
                await conn.close()
        return success

    async def table_rename(
        self, db_name: str, schema_name: str, table_name: str, table_name_new: str
    ):
        """Rename an existing table (async).
        Returns: bool - True if successful, False otherwise
        """
        conn = self.pg_conn or await self.connect(db_name=db_name, autocommit=True)
        local_conn = self.pg_conn is None
        success = False

        try:
            async with conn.cursor() as cursor:

                await cursor.execute(
                    SQL("""ALTER TABLE {0}.{1} RENAME TO {2};""").format(
                        Identifier(schema_name),
                        Identifier(table_name),
                        Identifier(table_name_new),
                    )
                )

            success = True
        except Exception as e:
            await conn.rollback()
            print("❌ ERROR -", e)
            logger.error("❌ ERROR -", e) 
        finally:
            if local_conn:
                await conn.close()
        return success

    async def table_check(self, db_name: str, schema_name: str, table_name: str):
        """Check if a table exists in the schema (async).
        Returns: bool - True if table exists, False otherwise
        """
        conn = self.pg_conn or await self.connect(db_name=db_name)
        local_conn = self.pg_conn is None
        answer = False

        try:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    SQL(
                        """
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables
                            WHERE  table_schema = %s
                            AND    table_name   = %s
                        );
                        """
                    ),
                    (schema_name, table_name),
                )
                answer = (await cursor.fetchall())[0][0]
        except Exception as e:
            print("❌ ERROR -", e)
            logger.error("❌ ERROR -", e)
        finally:
            if local_conn:
                await conn.close()
        return answer

    async def table_get(self, db_name: str, schema_name: str, table_name: str):
        """Fetch all rows from a table (async).
        Returns: List[dict] - List of row dictionaries
        """
        conn = self.pg_conn or await self.connect(db_name=db_name)
        local_conn = self.pg_conn is None
        result = []
        try:
            async with conn.cursor(row_factory=dict_row) as cursor:
                await cursor.execute(
                    SQL("""SELECT * FROM {0}.{1};""").format(
                        Identifier(schema_name), Identifier(table_name)
                    )
                )
                result = await cursor.fetchall()
        except Exception as e:
            print("❌ ERROR -", e)
            logger.error("❌ ERROR -", e)
        finally:
            if local_conn:
                await conn.close()
        return result

    async def table_move(
        self, db_name: str, schema_name: str, table_name: str, schema_name_new: str
    ):
        """Move a table from one schema to another (async).
        Returns: bool - True if successful, False otherwise
        """
        conn = self.pg_conn or await self.connect(db_name=db_name, autocommit=True)
        local_conn = self.pg_conn is None
        success = False

        try:
            async with conn.cursor() as cursor:

                await cursor.execute(
                    SQL("""ALTER TABLE {0}.{1} SET SCHEMA {2};""").format(
                        Identifier(schema_name),
                        Identifier(table_name),
                        Identifier(schema_name_new),
                    )
                )

            success = True
        except Exception as e:
            await conn.rollback()
            print("❌ ERROR -", e)
            logger.error("❌ ERROR -", e)
        finally:
            if local_conn:
                await conn.close()
        return success

    async def table_download(
        self, db_name: str, schema_name: str, table_name: str, directory: str
    ):
        """Export table data to CSV file (async).
        Creates file at: {directory}/{db_name}/{schema_name}/{table_name}.csv
        """
        file_path = os.path.join(directory, db_name, schema_name, table_name + ".csv")
        await asyncio.to_thread(misc.mkdir, file_path)

        result = await self.table_get(db_name, schema_name, table_name)

        def write_csv():
            df = pd.DataFrame(result)
            df.to_csv(file_path, index=False)

        await asyncio.to_thread(write_csv)

    async def table_select_one(
        self,
        db_name: str,
        schema_name: str,
        table_name: str,
        filters: Optional[Dict[str, Any]] = None,
        order_by: Optional[List[Tuple[str, Literal["ASC", "DESC"]]]] = None,
    ):
        conn = self.pg_conn or await self.connect(db_name=db_name)
        local_conn = self.pg_conn is None
        try:
            async with conn.cursor(row_factory=dict_row) as cur:
                base = SQL("SELECT * FROM {schema}.{table}").format(
                    schema=Identifier(schema_name),
                    table=Identifier(table_name),
                )
                where_parts: List[Composed] = []
                params: List[Any] = []
                if filters:
                    for col, val in filters.items():
                        if val is None:
                            where_parts.append(
                                Composed([Identifier(col), SQL(" IS NULL")])
                            )
                        else:
                            where_parts.append(
                                Composed([Identifier(col), SQL(" = %s")])
                            )
                            params.append(val)
                if where_parts:
                    base = base + SQL(" WHERE ") + SQL(" AND ").join(where_parts)
                if order_by:
                    order_parts: List[Composed] = []
                    for col, direction in order_by:
                        dir_sql = "ASC" if str(direction).upper() == "ASC" else "DESC"
                        order_parts.append(
                            Composed([Identifier(col), SQL(f" {dir_sql}")])
                        )
                    base = base + SQL(" ORDER BY ") + SQL(", ").join(order_parts)
                base = base + SQL(" LIMIT 1;")
                if params:
                    await cur.execute(base, tuple(params))
                else:
                    await cur.execute(base)
                row = await cur.fetchone()
                if not row:
                    return None
                return row
        except Exception as e:
            print("❌ ERROR -", e)
            logger.error("❌ ERROR -", e)
            return None
        finally:
            if local_conn:
                await conn.close()

    async def table_get_columns(self, db_name: str, schema_name: str, table_name: str):
        """Get all column names in a table (async).
        Returns: List[str] - List of column names
        """
        conn = self.pg_conn or await self.connect(db_name=db_name)
        local_conn = self.pg_conn is None
        list_table_columns = []
        try:
            async with conn.cursor(row_factory=dict_row) as cursor:
                await cursor.execute(
                    SQL(
                        """
                        SELECT column_name FROM information_schema.columns
                            WHERE  table_schema = %s
                            AND table_name = %s;
                        """
                    ),
                    (schema_name, table_name),
                )
                result = await cursor.fetchall()
            list_table_columns = [d["column_name"] for d in result]
        except Exception as e:
            print("❌ ERROR -", e)
            logger.error("❌ ERROR -", e)
        finally:
            if local_conn:
                await conn.close()
        return list_table_columns

    async def table_get_row_count(
        self, db_name: str, schema_name: str, table_name: str
    ):
        """Get total row count in a table (async).
        Returns: int - Number of rows, or None if table doesn't exist
        """
        conn = self.pg_conn or await self.connect(db_name=db_name)
        local_conn = self.pg_conn is None
        row_count = None
        try:
            if await self.table_check(db_name, schema_name, table_name):
                async with conn.cursor(row_factory=dict_row) as cursor:
                    await cursor.execute(
                        SQL("""SELECT COUNT(*) FROM {0}.{1};""").format(
                            Identifier(schema_name), Identifier(table_name)
                        )
                    )
                    result = await cursor.fetchall()
                row_count = result[0]["count"]
        except Exception as e:
            print("❌ ERROR -", e)
            logger.error("❌ ERROR -", e)
        finally:
            if local_conn:
                await conn.close()
        return row_count

    async def table_delete_column(
        self, db_name: str, schema_name: str, table_name: str, column_name: str
    ):
        """Set all values in a column to NULL (soft delete) (async).
        Note: Doesn't drop the column, just clears values.
        Returns: bool - True if successful, False otherwise
        """
        conn = self.pg_conn or await self.connect(db_name=db_name)
        local_conn = self.pg_conn is None
        success = False

        try:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    SQL("""UPDATE {0}.{1} SET {2} = null;""").format(
                        Identifier(schema_name),
                        Identifier(table_name),
                        Identifier(column_name),
                    )
                )
            await conn.commit()
            success = True
        except Exception as e:
            await conn.rollback()
            print("❌ ERROR -", e)
            logger.error("❌ ERROR -", e)
        finally:
            if local_conn:
                await conn.close()
        return success

    async def table_get_column(
        self, db_name: str, schema_name: str, table_name: str, column_name: str
    ):
        """Get all values from a specific column (async).
        Returns: List - List of column values
        """
        conn = self.pg_conn or await self.connect(db_name=db_name)
        local_conn = self.pg_conn is None
        list_column = []
        try:
            async with conn.cursor(row_factory=dict_row) as cursor:
                await cursor.execute(
                    SQL("""SELECT {0} FROM {1}.{2};""").format(
                        Identifier(column_name),
                        Identifier(schema_name),
                        Identifier(table_name),
                    )
                )
                result = await cursor.fetchall()
            list_column = [d[column_name] for d in result]
        except Exception as e:
            print("❌ ERROR -", e)
            logger.error("❌ ERROR -", e)
        finally:
            if local_conn:
                await conn.close()
        return list_column

    async def table_set_column_value(
        self,
        db_name: str,
        schema_name: str,
        table_name: str,
        column_name: str,
        value,
        where: Optional[dict] = None,
    ) -> bool:
        """Update a single column value with optional WHERE clause (async).
        Returns: bool - True if successful, False otherwise
        """
        conn = self.pg_conn or await self.connect(db_name=db_name)
        local_conn = self.pg_conn is None
        success = False

        try:
            async with conn.cursor() as cursor:
                query = SQL("UPDATE {schema}.{table} SET {col} = %s").format(
                    schema=Identifier(schema_name),
                    table=Identifier(table_name),
                    col=Identifier(column_name),
                )
                params = [value]
                if where:
                    conditions = [
                        Composed([Identifier(k), SQL(" = %s")]) for k in where.keys()
                    ]
                    query = query + SQL(" WHERE ") + SQL(" AND ").join(conditions)
                    params.extend(where.values())
                await cursor.execute(query, params)
            await conn.commit()
            success = True
        except Exception as e:
            await conn.rollback()
            print("❌ ERROR -", e)
            logger.error("❌ ERROR -", e)
        finally:
            if local_conn:
                await conn.close()
        return success

    async def table_get_column_distinct(
        self, db_name: str, schema_name: str, table_name: str, column_name: str
    ):
        """Get unique values from a column (async).
        Returns: List - List of distinct column values
        """
        conn = self.pg_conn or await self.connect(db_name=db_name)
        local_conn = self.pg_conn is None
        list_distinct = []
        try:
            async with conn.cursor(row_factory=dict_row) as cursor:
                await cursor.execute(
                    SQL("""SELECT DISTINCT {0} FROM {1}.{2};""").format(
                        Identifier(column_name),
                        Identifier(schema_name),
                        Identifier(table_name),
                    )
                )
                result = await cursor.fetchall()
            list_distinct = [d[column_name] for d in result]
        except Exception as e:
            print("❌ ERROR -", e)
            logger.error("❌ ERROR -", e)
        finally:
            if local_conn:
                await conn.close()
        return list_distinct

    async def table_to_timescaledb_hypertable(
        self,
        db_name: str,
        schema_name: str,
        table_name: str,
        time_column: str,
        *,
        partitioning_column: Optional[str] = None,
        number_partitions: Optional[int] = None,
        associated_schema: Optional[str] = None,
        associated_table_prefix: Optional[str] = None,
        chunk_time_interval: Optional[object] = None,
        create_default_indexes: Optional[bool] = None,
        if_not_exists: Optional[bool] = True,
        partitioning_func: Optional[str] = None,
        time_partitioning_func: Optional[str] = None,
        migrate_data: Optional[bool] = None,
        copy_indexes: Optional[bool] = None,
        distributed: Optional[bool] = None,
        data_nodes: Optional[List[str]] = None,
        replication_factor: Optional[int] = None,
        compression_enabled: Optional[bool] = None,
        compress_segmentby: Optional[object] = None,
        compress_orderby: Optional[str] = None,
        compression_interval: Optional[str] = None,
        retention_interval: Optional[str] = None,
        reorder_index: Optional[str] = None,
    ) -> bool:
        """Async version: create a (distributed) hypertable with optional attributes and policies.
        Only provided options are applied.
        """
        conn = self.pg_conn or await self.connect(db_name=db_name)
        local_conn = self.pg_conn is None
        success = False
        try:
            async with conn.cursor() as cursor:
                full_table_name = f"{schema_name}.{table_name}"

                use_distributed = (
                    bool(distributed)
                    or bool(data_nodes)
                    or (replication_factor is not None)
                )
                fn_sql = (
                    SQL("create_distributed_hypertable")
                    if use_distributed
                    else SQL("create_hypertable")
                )

                query = SQL("SELECT ") + fn_sql + SQL("(%s, %s")
                params: List[object] = [full_table_name, time_column]

                def add_param(name: str, value: object, cast: Optional[str] = None):
                    nonlocal query
                    if value is None:
                        return
                    query += SQL(", ") + SQL(name) + SQL(" => ")
                    if cast:
                        query += SQL("%s::" + cast)
                    else:
                        query += SQL("%s")
                    params.append(value)

                add_param("partitioning_column", partitioning_column)
                add_param("number_partitions", number_partitions)
                add_param("associated_schema", associated_schema)
                add_param("associated_table_prefix", associated_table_prefix)

                # chunk_time_interval can be INTERVAL or BIGINT depending on time type
                if chunk_time_interval is not None:
                    if isinstance(chunk_time_interval, (int, float)):
                        add_param("chunk_time_interval", int(chunk_time_interval))
                    else:
                        add_param(
                            "chunk_time_interval", chunk_time_interval, cast="interval"
                        )

                add_param("create_default_indexes", create_default_indexes)
                # Default if_not_exists to True unless explicitly set False
                if if_not_exists is not None:
                    add_param("if_not_exists", if_not_exists)
                else:
                    add_param("if_not_exists", True)

                add_param("partitioning_func", partitioning_func)
                add_param("time_partitioning_func", time_partitioning_func)
                add_param("migrate_data", migrate_data)
                add_param("copy_indexes", copy_indexes)

                # Distributed-only extras
                if use_distributed:
                    if data_nodes:
                        query += (
                            SQL(", data_nodes => ARRAY[")
                            + SQL(", ").join(SQL("%s") for _ in data_nodes)
                            + SQL("]")
                        )
                        params.extend(list(data_nodes))
                    add_param("replication_factor", replication_factor)

                query += SQL(");")
                await cursor.execute(query, params)
                print(
                    f"✅ Hypertable created for {full_table_name} (time column: {time_column})"
                )
                logger.info(
                    f"✅ Hypertable created for {full_table_name} (time column: {time_column})"
                )

                # Compression settings
                if compression_enabled or compress_segmentby or compress_orderby:
                    opts: List[Composed] = []
                    opts.append(SQL("timescaledb.compress"))
                    if compress_segmentby:
                        opts.append(
                            SQL("timescaledb.compress_segmentby = ") + SQL("%s")
                        )
                    if compress_orderby:
                        opts.append(SQL("timescaledb.compress_orderby = ") + SQL("%s"))
                    alter = (
                        SQL("ALTER TABLE {}.{} SET (").format(
                            Identifier(schema_name), Identifier(table_name)
                        )
                        + SQL(", ").join(opts)
                        + SQL(");")
                    )
                    alter_params: List[object] = []
                    if compress_segmentby:
                        alter_params.append(
                            ",".join(compress_segmentby)
                            if isinstance(compress_segmentby, (list, tuple))
                            else str(compress_segmentby)
                        )
                    if compress_orderby:
                        alter_params.append(str(compress_orderby))
                    await cursor.execute(alter, alter_params)

                if compression_interval:
                    q = SQL("SELECT add_compression_policy(%s, %s::interval);")
                    await cursor.execute(q, (full_table_name, compression_interval))

                if retention_interval:
                    q = SQL("SELECT add_retention_policy(%s, %s::interval);")
                    await cursor.execute(q, (full_table_name, retention_interval))

                if reorder_index:
                    q = SQL("SELECT add_reorder_policy({}.{}, %s);").format(
                        Identifier(schema_name), Identifier(table_name)
                    )
                    await cursor.execute(q, (reorder_index,))

            await conn.commit()
            success = True
        except Exception as e:
            await conn.rollback()
            print("❌ ERROR -", e)
            logger.error("❌ ERROR -", e)
        finally:
            if local_conn:
                await conn.close()
        return success

    # -----------------------------------------------------------------------
    # MATERIALIZED VIEW
    async def mview_get(
        self, db_name: str, schema_name: str, materialized_view_name: str
    ):
        """Fetch all rows from a materialized view (async).
        Returns: List[dict] - List of row dictionaries
        """
        conn = self.pg_conn or await self.connect(db_name=db_name)
        local_conn = self.pg_conn is None
        result = []
        try:
            async with conn.cursor(row_factory=dict_row) as cursor:
                await cursor.execute(
                    SQL("""SELECT * FROM {0}.{1};""").format(
                        Identifier(schema_name),
                        Identifier(materialized_view_name),
                    )
                )
                result = await cursor.fetchall()
        except Exception as e:
            await conn.rollback()
            print("❌ ERROR -", e)
            logger.error("❌ ERROR -", e)
        finally:
            if local_conn:
                await conn.close()
        return result

    async def mview_delete(
        self, db_name: str, schema_name: str, materialized_view_name: str
    ):
        """Drop a materialized view (async).
        Returns: bool - True if successful, False otherwise
        """
        conn = self.pg_conn or await self.connect(db_name=db_name)
        local_conn = self.pg_conn is None
        success = False

        try:
            async with conn.cursor(row_factory=dict_row) as cursor:

                await cursor.execute(
                    SQL("""DROP MATERIALIZED VIEW IF EXISTS {0}.{1} CASCADE;""").format(
                        Identifier(schema_name), Identifier(materialized_view_name)
                    )
                )

            await conn.commit()
            success = True
        except Exception as e:
            await conn.rollback()
            print("❌ ERROR -", e)
            logger.error("❌ ERROR -", e)
        finally:
            if local_conn:
                await conn.close()
        return success

    async def mview_refresh(
        self,
        db_name: str,
        schema_name: str,
        materialized_view_name: str,
        concurrently: bool = False,
    ):
        """
        Refresh a materialized view (async).
        Args:
            db_name (str): Database name
            schema_name (str): Schema name
            materialized_view_name (str): Materialized view name
            concurrently (bool): Use CONCURRENTLY option
        Returns: bool - True if successful, False otherwise
        """
        conn = self.pg_conn or await self.connect(db_name=db_name)
        local_conn = self.pg_conn is None
        success = False

        try:
            async with conn.cursor() as cursor:
                if concurrently:
                    await cursor.execute(
                        SQL("REFRESH MATERIALIZED VIEW CONCURRENTLY {0}.{1};").format(
                            Identifier(schema_name), Identifier(materialized_view_name)
                        )
                    )
                else:
                    await cursor.execute(
                        SQL("REFRESH MATERIALIZED VIEW {0}.{1};").format(
                            Identifier(schema_name), Identifier(materialized_view_name)
                        )
                    )
            await conn.commit()
            success = True
        except Exception as e:
            await conn.rollback()
            print(
                f"❌ ERROR refreshing materialized view {schema_name}.{materialized_view_name} -",
                e,
            )
            logger.error(
                f"❌ ERROR refreshing materialized view {schema_name}.{materialized_view_name} -",
                e,
            )
        finally:
            if local_conn:
                await conn.close()
        return success


# --------------------------------------------------------------------------------------------------


class WITH_PYDANTIC(GENERAL):

    def __init__(
        self,
        timezone: str = "UTC",
        pg_conn: Optional[psycopg.Connection] = None,
    ):
        super().__init__(timezone=timezone, pg_conn=pg_conn)

    def table_get_models(
        self,
        db_name: str,
        schema_name: str,
        table_name: str,
        model: Type[T],
    ):
        """Fetch all rows from a table as Pydantic model instances.
        Args:
            model: Pydantic BaseModel class to use for deserialization
        Returns: List[BaseModel] - List of model instances
        """
        conn = self.pg_conn or self.connect(db_name=db_name)
        local_conn = self.pg_conn is None
        list_models = []

        try:
            with conn.cursor(row_factory=dict_row) as cursor:
                cursor.execute(
                    SQL("""SELECT * FROM {0}.{1};""").format(
                        Identifier(schema_name), Identifier(table_name)
                    )
                )

                rows = cursor.fetchall()

            list_models = [model(**row) for row in rows]

        except Exception as e:
            print("❌ ERROR -", e)
            logger.error("❌ ERROR -", e)

        finally:
            if local_conn:
                conn.close()

            return list_models

    def table_create(
        self,
        db_name: str,
        schema_name: str,
        table_name: str,
        model: Type[T],
    ):
        """
        🧱 Dynamically create a PostgreSQL table from a Pydantic v2 model.

        ✅ Uses field.json_schema_extra["psql_data_type"] for SQL column type
        ✅ Reads primary key columns from `__pkey__ = ["col1", "col2"]`
        ✅ Adds NOT NULL based on Pydantic field requirement
        ✅ Uses psycopg.sql for safety (avoids SQL injection)
        """

        conn = self.pg_conn or self.connect(db_name=db_name)
        local_conn = self.pg_conn is None
        success = False

        # ------------------------------------------------------------------
        # 1️⃣ Collect field definitions and primary key list
        # ------------------------------------------------------------------
        fields = model.__pydantic_fields__
        primary_keys = getattr(model, "__pkey__", [])
        columns_sql = []  # will hold SQL fragments for each column

        # ------------------------------------------------------------------
        # 2️⃣ Build column definitions
        # ------------------------------------------------------------------
        for name, field in fields.items():
            psql_data_type = (
                field.json_schema_extra.get("psql_data_type")
                if field.json_schema_extra
                else None
            )
            if not psql_data_type:
                raise ValueError(
                    f"❌ Missing psql_data_type for field '{name}' in model '{model.__name__}'"
                )

            # moving this to psql_data_type
            # null_clause = SQL("NOT NULL") if field.is_required() and not field.allow_none else SQL("")
            column_def = SQL(" ").join([Identifier(name), SQL(psql_data_type)])
            columns_sql.append(column_def)

        # ------------------------------------------------------------------
        # 3️⃣ Add PRIMARY KEY clause (if any)
        # ------------------------------------------------------------------
        if primary_keys:
            pk_clause = (
                SQL("PRIMARY KEY (")
                + SQL(", ").join(map(Identifier, primary_keys))
                + SQL(")")
            )
            columns_sql.append(pk_clause)

        # ------------------------------------------------------------------
        # 4️⃣ Join all column definitions
        # ------------------------------------------------------------------
        all_columns = SQL(", ").join(columns_sql)

        # ------------------------------------------------------------------
        # 5️⃣ Compose the full CREATE TABLE statement safely
        # ------------------------------------------------------------------
        query = SQL(
            """
            CREATE TABLE IF NOT EXISTS {}.{} (
                {}
            );
        """
        ).format(Identifier(schema_name), Identifier(table_name), all_columns)

        # ------------------------------------------------------------------
        # 6️⃣ Execute safely
        # ------------------------------------------------------------------
        try:
            with conn.cursor() as cur:
                cur.execute(query)

            conn.commit()

            print(f"✅ Table '{table_name}' created in schema '{schema_name}'")
            logger.info(f"✅ Table '{table_name}' created in schema '{schema_name}'")
            success = True
        except Exception as e:
            print("❌ ERROR -", e)
            logger.error("❌ ERROR -", e)
            conn.rollback()
        finally:
            if local_conn:
                conn.close()

            return success

    def table_insert(
        self,
        db_name: str,
        schema_name: str,
        table_name: str,
        model: Type[T],
        list_models: List[T],
    ):
        """
        🧩 Generic INSERT for Pydantic models using psycopg3.sql for safety.

        ✅ Uses parameterized placeholders (safe against SQL injection)
        ✅ Works with multiple rows using executemany()
        ✅ Commits on success, rolls back on failure
        """

        # ------------------------------------------------------------------
        # 1️⃣ Connect to PostgreSQL
        # ------------------------------------------------------------------

        conn = self.pg_conn or self.connect(db_name=db_name)
        local_conn = self.pg_conn is None
        success = False

        # ------------------------------------------------------------------
        # 2️⃣ Early exit if no data
        # ------------------------------------------------------------------
        if not list_models:
            print("⚠️ No insert_data provided for insert.")
            logger.warning("⚠️ No insert_data provided for insert.")
            return False

        # Confirm Pydantic type matches
        if not isinstance(list_models[0], model):
            raise ValueError("❌ Pydantic model and data do not match!")

        # ------------------------------------------------------------------
        # 3️⃣ Convert Pydantic models to dicts
        # ------------------------------------------------------------------
        records = [m.model_dump() for m in list_models]
        columns = list(records[0].keys())

        # ------------------------------------------------------------------
        # 4️⃣ Build SQL query safely
        # ------------------------------------------------------------------
        col_names = SQL(", ").join(map(Identifier, columns))
        placeholders = SQL(", ").join(SQL("%s") for _ in columns)

        query = SQL(
            """
            INSERT INTO {schema}.{table} ({cols})
            VALUES ({values});
            """
        ).format(
            schema=Identifier(schema_name),
            table=Identifier(table_name),
            cols=col_names,
            values=placeholders,
        )

        # ------------------------------------------------------------------
        # 5️⃣ Execute safely using executemany()
        # ------------------------------------------------------------------
        try:
            with conn.cursor() as cur:
                cur.executemany(query, [tuple(r.values()) for r in records])

            conn.commit()

            print(f"✅ Inserted {len(list_models)} rows into '{table_name}'")
            logger.info(f"✅ Inserted {len(list_models)} rows into '{table_name}'")
            success = True

        except Exception as e:
            print("❌ INSERT ERROR -", e)
            logger.error("❌ INSERT ERROR -", e)
            conn.rollback()

        finally:
            if local_conn:
                conn.close()

            return success

    def table_upsert(
        self,
        db_name: str,
        schema_name: str,
        table_name: str,
        model: Type[T],
        list_models: List[T],
    ):
        """
        🔄 Generic UPSERT for Pydantic models using psycopg3.sql for safety.

        ✅ Uses parameterized placeholders (safe against SQL injection)
        ✅ Works with multiple rows using executemany()
        ✅ Returns number of rows upserted
        """

        conn = self.pg_conn or self.connect(db_name=db_name)
        local_conn = self.pg_conn is None
        success = False

        # ------------------------------------------------------------------
        # 1️⃣ Early exit if no data
        # ------------------------------------------------------------------
        if not list_models:
            print("⚠️ No upsert_data provided for upsert.")
            logger.warning("⚠️ No upsert_data provided for upsert.")
            return False

        # confirm if pydantic_model and the models in upsert data is same
        if not isinstance(list_models[0], model):
            raise ValueError("Pydantic model and data do not match!")

        # model_class: Type[BaseModel] = type(list_models[0])

        # ------------------------------------------------------------------
        # 2️⃣ Infer table and conflict keys
        # ------------------------------------------------------------------
        conflict_columns = getattr(model, "__pkey__", [])

        if not conflict_columns:
            raise ValueError(f"❌ Model NO PRIMARY KEYS for upsert operations.")

        # ------------------------------------------------------------------
        # 3️⃣ Convert list_models to list of dicts
        # ------------------------------------------------------------------
        records = [m.model_dump() for m in list_models]
        columns = list(records[0].keys())

        # ------------------------------------------------------------------
        # 4️⃣ Build SQL components safely using psycopg.sql
        # ------------------------------------------------------------------
        col_names = SQL(", ").join(map(Identifier, columns))
        # placeholders = SQL(", ").join(Placeholder() * len(columns))
        placeholders = SQL(", ").join(SQL("%s") for _ in columns)
        # conflict_cols = SQL(", ").join(map(Identifier, conflict_columns))

        if len(conflict_columns) > 1:
            conflict_cols = SQL(", ").join(map(Identifier, conflict_columns))
        else:
            conflict_cols = Identifier(conflict_columns[0])

        # For update → skip conflict columns
        update_set = SQL(", ").join(
            Composed([Identifier(c), SQL(" = EXCLUDED."), Identifier(c)])
            for c in columns
            if c not in conflict_columns
        )

        # ------------------------------------------------------------------
        # 5️⃣ Compose final UPSERT query
        # ------------------------------------------------------------------
        query = SQL(
            """
            INSERT INTO {0}.{1} ({cols})
            VALUES ({values})
            ON CONFLICT ({conflict})
            DO UPDATE SET {updates};
        """
        ).format(
            Identifier(schema_name),
            Identifier(table_name),
            cols=col_names,
            values=placeholders,
            conflict=conflict_cols,
            updates=update_set,
        )

        # ------------------------------------------------------------------
        # 6️⃣ Execute safely using executemany()
        # ------------------------------------------------------------------
        try:
            with conn.cursor() as cur:
                cur.executemany(query, [tuple(r.values()) for r in records])

            conn.commit()
            print(f"✅ Upserted {len(list_models)} rows into '{table_name}'")
            logger.info(f"✅ Upserted {len(list_models)} rows into '{table_name}'")

            success = True

        except Exception as e:
            print("ERROR - ", e)
            logger.error("ERROR - ", e)
            conn.rollback()

        finally:
            if local_conn:
                conn.close()

            return success


# --------------------------------------------------------------------------------------------------


class WITH_PYDANTIC_ASYNC(GENERAL_ASYNC):

    def __init__(
        self,
        timezone: str = "UTC",
        pg_conn: Optional[psycopg.AsyncConnection] = None,
    ):
        super().__init__(timezone=timezone, pg_conn=pg_conn)

    async def table_get_models(
        self,
        db_name: str,
        schema_name: str,
        table_name: str,
        model: Type[T],
    ):
        """Fetch all rows from a table as Pydantic model instances (async).
        Args:
            model: Pydantic BaseModel class to use for deserialization
        Returns: List[BaseModel] - List of model instances
        """
        conn = self.pg_conn or await self.connect(db_name=db_name)
        local_conn = self.pg_conn is None
        list_models = []

        try:
            async with conn.cursor(row_factory=dict_row) as cursor:
                await cursor.execute(
                    SQL("""SELECT * FROM {0}.{1};""").format(
                        Identifier(schema_name), Identifier(table_name)
                    )
                )
                rows = await cursor.fetchall()

            list_models = [model(**row) for row in rows]

        except Exception as e:
            print("❌ ERROR -", e)
            logger.error("❌ ERROR -", e)

        finally:
            if local_conn:
                await conn.close()

            return list_models

    async def table_create(
        self,
        db_name: str,
        schema_name: str,
        table_name: str,
        model: Type[T],
    ):
        """
        🧱 Dynamically create a PostgreSQL table from a Pydantic v2 model.

        ✅ Uses field.json_schema_extra["psql_data_type"] for SQL column type
        ✅ Reads primary key columns from `__pkey__ = ["col1", "col2"]`
        ✅ Adds NOT NULL based on Pydantic field requirement
        ✅ Uses psycopg.sql for safety (avoids SQL injection)
        """

        conn = self.pg_conn or await self.connect(db_name=db_name)
        local_conn = self.pg_conn is None
        success = False

        # ------------------------------------------------------------------
        # 1️⃣ Collect field definitions and primary key list
        # ------------------------------------------------------------------
        fields = model.__pydantic_fields__
        primary_keys = getattr(model, "__pkey__", [])
        columns_sql = []  # will hold SQL fragments for each column

        # ------------------------------------------------------------------
        # 2️⃣ Build column definitions
        # ------------------------------------------------------------------
        for name, field in fields.items():
            psql_data_type = (
                field.json_schema_extra.get("psql_data_type")
                if field.json_schema_extra
                else None
            )
            if not psql_data_type:
                raise ValueError(
                    f"❌ Missing psql_data_type for field '{name}' in model '{model.__name__}'"
                )

            column_def = SQL(" ").join([Identifier(name), SQL(psql_data_type)])
            columns_sql.append(column_def)

        # ------------------------------------------------------------------
        # 3️⃣ Add PRIMARY KEY clause (if any)
        # ------------------------------------------------------------------
        if primary_keys:
            pk_clause = (
                SQL("PRIMARY KEY (")
                + SQL(", ").join(map(Identifier, primary_keys))
                + SQL(")")
            )
            columns_sql.append(pk_clause)

        # ------------------------------------------------------------------
        # 4️⃣ Join all column definitions
        # ------------------------------------------------------------------
        all_columns = SQL(", ").join(columns_sql)

        # ------------------------------------------------------------------
        # 5️⃣ Compose the full CREATE TABLE statement safely
        # ------------------------------------------------------------------
        query = SQL(
            """
            CREATE TABLE IF NOT EXISTS {}.{} (
                {}
            );
        """
        ).format(Identifier(schema_name), Identifier(table_name), all_columns)

        # ------------------------------------------------------------------
        # 6️⃣ Execute safely
        # ------------------------------------------------------------------
        try:
            async with conn.cursor() as cur:
                await cur.execute(query)

            await conn.commit()

            print(f"✅ Table '{table_name}' created in schema '{schema_name}'")
            logger.info(f"✅ Table '{table_name}' created in schema '{schema_name}'")
            success = True
        except Exception as e:
            await conn.rollback()
            print("❌ ERROR -", e)
            logger.error("❌ ERROR -", e)
        finally:
            if local_conn:
                await conn.close()

            return success

    async def table_insert(
        self,
        db_name: str,
        schema_name: str,
        table_name: str,
        model: Type[T],
        list_models: List[T],
    ):
        """
        🧩 Generic INSERT for Pydantic models using psycopg3.sql for safety.

        ✅ Uses parameterized placeholders (safe against SQL injection)
        ✅ Works with multiple rows using executemany()
        ✅ Commits on success, rolls back on failure
        """
        # ------------------------------------------------------------------
        # 1️⃣ Connect to PostgreSQL
        # ------------------------------------------------------------------

        conn = self.pg_conn or await self.connect(db_name=db_name)
        local_conn = self.pg_conn is None
        success = False

        # ------------------------------------------------------------------
        # 2️⃣ Early exit if no data
        # ------------------------------------------------------------------
        if not list_models:
            print("⚠️ No insert_data provided for insert.")
            logger.warning("⚠️ No insert_data provided for insert.")
            return False

        # Confirm Pydantic type matches
        if not isinstance(list_models[0], model):
            raise ValueError("❌ Pydantic model and data do not match!")

        # ------------------------------------------------------------------
        # 3️⃣ Convert Pydantic models to dicts
        # ------------------------------------------------------------------
        records = [m.model_dump() for m in list_models]
        columns = list(records[0].keys())

        # ------------------------------------------------------------------
        # 4️⃣ Build SQL query safely
        # ------------------------------------------------------------------
        col_names = SQL(", ").join(map(Identifier, columns))
        placeholders = SQL(", ").join(SQL("%s") for _ in columns)

        query = SQL(
            """
            INSERT INTO {schema}.{table} ({cols})
            VALUES ({values});
            """
        ).format(
            schema=Identifier(schema_name),
            table=Identifier(table_name),
            cols=col_names,
            values=placeholders,
        )


        # ------------------------------------------------------------------
        # 5️⃣ Execute safely using executemany()
        # ------------------------------------------------------------------
        try:
            async with conn.cursor() as cur:
                await cur.executemany(query, [tuple(r.values()) for r in records])
            await conn.commit()

            print(f"✅ Inserted {len(list_models)} rows into '{table_name}'")
            logger.info(f"✅ Inserted {len(list_models)} rows into '{table_name}'")
            success = True

        except Exception as e:
            print("❌ INSERT ERROR -", e)
            logger.error("❌ INSERT ERROR -", e)
            await conn.rollback()

        finally:
            if local_conn:
                await conn.close()

            return success

    async def table_upsert(
        self,
        db_name: str,
        schema_name: str,
        table_name: str,
        model: Type[T],
        list_models: List[T],
    ):
        """
        🔄 Generic UPSERT for Pydantic models using psycopg3.sql for safety.

        ✅ Uses parameterized placeholders (safe against SQL injection)
        ✅ Works with multiple rows using executemany()
        ✅ Returns number of rows upserted
        """

        conn = self.pg_conn or await self.connect(db_name=db_name)
        local_conn = self.pg_conn is None
        success = False

        # ------------------------------------------------------------------
        # 1️⃣ Early exit if no data
        # ------------------------------------------------------------------
        if not list_models:
            print("⚠️ No upsert_data provided for upsert.")
            logger.warning("⚠️ No upsert_data provided for upsert.")
            return False

        # confirm if pydantic_model and the models in upsert data is same
        if not isinstance(list_models[0], model):
            raise ValueError("Pydantic model and data do not match!")

        # ------------------------------------------------------------------
        # 2️⃣ Infer table and conflict keys
        # ------------------------------------------------------------------
        conflict_columns = getattr(model, "__pkey__", [])

        if not conflict_columns:
            raise ValueError(f"❌ Model NO PRIMARY KEYS for upsert operations.")

        # ------------------------------------------------------------------
        # 3️⃣ Convert list_models to list of dicts
        # ------------------------------------------------------------------
        records = [m.model_dump() for m in list_models]
        columns = list(records[0].keys())

        # ------------------------------------------------------------------
        # 4️⃣ Build SQL components safely using psycopg.sql
        # ------------------------------------------------------------------
        col_names = SQL(", ").join(map(Identifier, columns))
        placeholders = SQL(", ").join(SQL("%s") for _ in columns)

        if len(conflict_columns) > 1:
            conflict_cols = SQL(", ").join(map(Identifier, conflict_columns))
        else:
            conflict_cols = Identifier(conflict_columns[0])

        # For update → skip conflict columns
        update_set = SQL(", ").join(
            Composed([Identifier(c), SQL(" = EXCLUDED."), Identifier(c)])
            for c in columns
            if c not in conflict_columns
        )

        # ------------------------------------------------------------------
        # 5️⃣ Compose final UPSERT query
        # ------------------------------------------------------------------
        query = SQL(
            """
            INSERT INTO {0}.{1} ({cols})
            VALUES ({values})
            ON CONFLICT ({conflict})
            DO UPDATE SET {updates};
        """
        ).format(
            Identifier(schema_name),
            Identifier(table_name),
            cols=col_names,
            values=placeholders,
            conflict=conflict_cols,
            updates=update_set,
        )

        # ------------------------------------------------------------------
        # 6️⃣ Execute safely using executemany()
        # ------------------------------------------------------------------
        try:
            async with conn.cursor() as cur:
                await cur.executemany(query, [tuple(r.values()) for r in records])

            await conn.commit()
            print(f"✅ Upserted {len(list_models)} rows into '{table_name}'")
            logger.info(f"✅ Upserted {len(list_models)} rows into '{table_name}'")

            success = True

        except Exception as e:
            await conn.rollback()
            print("ERROR - ", e)
            logger.error(f"❌ ERROR in table_upsert: {e}")

        finally:
            if local_conn:
                await conn.close()

            return success


# --------------------------------------------------------------------------------------------------
