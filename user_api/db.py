from fastapi import Request
import redis.asyncio as redis
from decouple import config


def get_pg_info():
    return f"""
    host={config('POSTGRES_HOST', cast=str)}
    port={config('POSTGRES_PORT', cast=int)}
    dbname={config('POSTGRES_DB', cast=str)}
    user={config('POSTGRES_USER', cast=str)}
    password={config('POSTGRES_PASSWORD', cast=str)}
    options='-c timezone={config('TIMEZONE_DEFAULT', cast=str)}'
    """


async def get_pg_conn(request: Request):
    """
    can be added as a dependency in an api call
    pg_conn: psycopg.Connection = Depends(get_pg_conn),

    Alternate use -

    # async with app.state.async_pg_pool.connection() as pg_conn:
    #     async with pg_conn.cursor() as cursor:
    #         await cursor.execute("SELECT table_name FROM information_schema.tables;")
    #         aa = await cursor.fetchall()
    #         print(aa)

    """

    async with request.app.state.async_pg_pool.connection() as conn:
        yield conn
        # Do not need to explicitly be closed as context manager is used.


async def get_redis_conn(request: Request):
    """
    can be added as a dependency in an api call
    redis_conn: redis.Redis = Depends(get_redis_conn),

    Alternate use -

    # async with redis.Redis(connection_pool=app.state.async_redis_pool) as redis_conn:
    #     await redis_conn.set("temporary_key", "some_data", ex=60)
    #     all_keys_redis = await redis_conn.get("temporary_key")
    #     print(all_keys_redis)

    """
    # Dependency to get a Redis connection from the pool.
    async with redis.Redis(connection_pool=request.app.state.async_redis_pool) as conn:
        yield conn
        # Do not need to explicitly be closed as context manager is used.
