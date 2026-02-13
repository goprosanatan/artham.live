# ==============================================================================
# Entrypoint for FastAPI

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from decouple import config
import psycopg_pool
import redis.asyncio as redis
import os
import logging
import socketio
import asyncio
import json


try:
    from routes import user, chart, order, replay, websocket
    from db import get_pg_info
except:
    from .routes import user, chart, order, replay, websocket
    from .db import get_pg_info

logging.basicConfig(
    filename=(os.path.join(config("DIR_LOGS", cast=str), "artham_user_01_api.log")),
    encoding="utf-8",
    level=logging.DEBUG,
    datefmt="%Y-%m-%d %H:%M:%S %p %Z",
    format="%(asctime)s %(levelname)-8s %(message)s",
)
logger = logging.getLogger(__name__)
logging.getLogger("psycopg.pool").setLevel(logging.DEBUG)


# ==============================================================================
# REDIS PUBSUB


async def on_message_from_exchange(data: dict):
    print(f"[API] Received from SERVER: {data}")


# ==============================================================================
# Lifespan


# lifespan executes before Fastapi starts and after it stops
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup logic --------------------------------------------
    print("Application startup: Initializing resources...")

    # postgres timescaleDB ---------------
    app.state.async_pg_pool = psycopg_pool.AsyncConnectionPool(
        conninfo=get_pg_info(),
        open=False,
        min_size=4,
        max_size=8,
        # max_idle=300,
        # max_lifetime=3600,
    )

    await app.state.async_pg_pool.open()

    # redis ---------------
    app.state.async_redis_pool = redis.ConnectionPool(
        host=config("REDIS_HOST", cast=str),
        port=config("REDIS_PORT", cast=int),
        db=0,
        decode_responses=True,
    )
    
    # ✅ Attach FastAPI state to ASGIApp manually
    websocket.asgi_app.state = app.state

    # Start WebSocket fanout service
    app.state.fanout_task = asyncio.create_task(
        websocket.websocket_fanout_service(app.state.async_redis_pool)
    )
    logger.info("WebSocket fanout service started")

    # --------------------------------------------
    yield

    # Shutdown logic -------------------------------------------
    print("Application shutdown: Cleaning up resources...")

    # Stop fanout service
    if hasattr(app.state, 'fanout_task'):
        app.state.fanout_task.cancel()
        try:
            await app.state.fanout_task
        except asyncio.CancelledError:
            logger.info("Fanout task cancelled successfully")

    await app.state.async_pg_pool.close()
    await app.state.async_redis_pool.disconnect()

# ==============================================================================
# DEFAULT Config - FastAPI

app = FastAPI(lifespan=lifespan)

# include all routes
app.include_router(user.router)
app.include_router(chart.router)
app.include_router(order.router)
app.include_router(replay.router)

# Mount the Socket.IO application onto the FastAPI app
app.mount(
    path="/websocket",  # final path for websocket = path + socketio_path
    app=websocket.asgi_app,
    name="websocket",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins="*",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["authorization"],
)

# ==============================================================================
# HTTP endpoints


@app.options("/")
async def initiate_handshake():
    return {"allowed_methods": ["GET", "POST", "PUT"]}


# ------------------------------------------------------


# @app.get("/")
# async def welcome(request: Request):
#     return {"MAIN": request.scope.get("root_path")}


# ------------------------------------------------------

@app.get("/test")
async def welcome():
    return "HELLO"

# ===================================================================================================
