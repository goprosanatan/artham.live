# ===================================================================================================
# API for Zerodha Kite - For usage of the API and conversion of API data to GENERIC data
# ===================================================================================================

from datetime import datetime, timezone
from kiteconnect import KiteConnect
from kiteconnect import KiteTicker
import logging
import threading
import multiprocessing
import json
from flatten_json import flatten
import redis
from decouple import config
import time
import asyncio
from typing import List, Optional


logger = logging.getLogger(__name__)

# ===================================================================================================
# DEFAULT CALLBACKS


class KITE_WEBSOCKET:
    def __init__(
        self,
        api_key: str,
        access_token: str,
        tokens: List[int],
    ):
        if KiteTicker is None:
            raise RuntimeError("kiteconnect is not installed. pip install kiteconnect")

        self.api_key = api_key
        self.access_token = access_token
        self.tokens = tokens
        # Main thread asyncio loop to schedule coroutines from KiteTicker thread
        # Capture the current running loop (must be constructed within an async context)
        self.loop = asyncio.get_running_loop()
        self.ws = KiteTicker(api_key, access_token)

        # Bind callbacks
        self.ws.on_ticks = self.on_ticks
        self.ws.on_connect = self.on_connect
        self.ws.on_close = self.on_close
        self.ws.on_error = self.on_error

        # Reconnection strategy
        self.ws.on_reconnect = self.on_reconnect
        self.ws.on_noreconnect = self.on_noreconnect

    def start(self):
        # KiteTicker runs in a background thread; this returns immediately
        self.ws.connect(threaded=True)
        logger.info("KiteTicker connecting...")

    # ----------------------------- Callbacks ---------------------------------
    def on_connect(self, ws, response):
        logger.info("KiteTicker connected, subscribing tokens")
        if self.tokens:
            ws.subscribe(self.tokens)
            ws.set_mode(ws.MODE_FULL, self.tokens)

    def on_close(self, ws, code, reason):
        logger.warning(f"KiteTicker closed: {code} {reason}")

    def on_error(self, ws, code, reason):
        logger.error(f"KiteTicker error: {code} {reason}")

    def on_reconnect(self, ws, attempt_count):
        logger.warning(f"KiteTicker reconnect attempt {attempt_count}")

    def on_noreconnect(self, ws):
        logger.error("KiteTicker gave up reconnecting")

    def on_ticks(self, ws, ticks):
        """Bridge threaded callback → asyncio (thread-safe)."""
        # KiteTicker invokes this in its own thread; schedule coroutine on main loop
        try:
            asyncio.run_coroutine_threadsafe(
                self._publish_ticks_async(ticks), self.loop
            )
        except Exception as e:
            logger.exception(f"Failed to schedule tick publish: {e}")

    async def _publish_ticks_async(self, ticks):
        timestamp_now = datetime.now()
        for tick in ticks:
            try:
                payload = flatten(tick)
                payload["exchange_ts"] = payload.get("exchange_timestamp")
                payload["receive_ts"] = timestamp_now
                token = int(tick.get("instrument_token"))

                await async_redis_conn.xadd(
                    stream_key(token),
                    {"tick": json.dumps(payload, default=str)},
                    maxlen=100000,
                    approximate=True,
                )
                # logger.debug(f"[FEED] {token} → {payload['last_price']}")
            except Exception as e:
                logger.exception(f"Failed to publish tick: {e}")

        logger.info(f"[INGESTED INTO REDIS STREAMS] {len(ticks)} ticks")


# # Initialize WebSocket
# access_token = "DFTJz76J7v5aIo3tigycbn7lJf7AswBr"
# ws = KITE_WEBSOCKET(access_token=access_token)


# @ws.callback("on_ticks")
# def handle_ticks(ws, ticks):
#     print("📊 Ticks received:", ticks)


# @ws.callback("on_connect")
# def handle_connect(ws, response):
#     print("✅ Connected:", response)
#     ws.subscribe([738561, 5633])
#     ws.set_mode(ws.MODE_FULL, [738561])


# @ws.callback("on_close")
# def handle_close(ws, code, reason):
#     print(f"❌ Closed: {code} | {reason}")
#     ws.unsubscribe([738561, 5633])
#     # ws.stop()


# @ws.callback("on_noreconnect")
# def handle_noreconnect(ws, code, reason):
#     print(f"❌ Closed: {code} | {reason}")
#     # ws.unsubscribe([738561, 5633])
#     ws.close()


# # Start WebSocket
# ws.connect()

# ws.websocket.is_connected()

# ws.websocket.unsubscribe([738561, 5633])

# ws.websocket.close()


# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


# access_token = "DFTJz76J7v5aIo3tigycbn7lJf7AswBr"

# # Initialise
# kws = KiteTicker(
#     config("KITE_API_KEY", cast=str),
#     access_token,
# )


# def on_ticks(ws, ticks):
#     # Callback to receive ticks.
#     print("Ticks: {}".format(ticks))


# def on_connect(ws, response):
#     # Callback on successful connect.
#     # Subscribe to a list of instrument_tokens (RELIANCE and ACC here).
#     ws.subscribe([738561, 5633])

#     # Set RELIANCE to tick in `full` mode.
#     ws.set_mode(ws.MODE_FULL, [738561])

#     pass


# def on_close(ws, code, reason):
#     # On connection close stop the main loop
#     # Reconnection will not happen after executing `ws.stop()`
#     # ws.stop()
#     pass


# # Assign the callbacks.
# kws.on_ticks = on_ticks
# kws.on_connect = on_connect
# kws.on_close = on_close

# # Infinite loop on the main thread. Nothing after this will run.
# # You have to use the pre-defined callbacks to manage subscriptions.
# kws.connect(threaded=True)
# kws.resubscribe()

# kws.close()

# print(kws.is_connected())

# kws.unsubscribe([738561, 5633])
