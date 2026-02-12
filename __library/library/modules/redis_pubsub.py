# ===================================================================================================
# ===================================================================================================

import logging
import asyncio
import json
from typing import Awaitable, Callable
import redis.asyncio as redis
from redis.exceptions import ConnectionError

logger = logging.getLogger(__name__)

# ===================================================================================================


class REDIS_PUBSUB:
    def __init__(self, redis_conn: redis.Redis):
        self.redis = redis_conn
        self._tasks = []
        self._pubsubs = []

    async def publish(self, channel: str, message: dict):
        """Publish a JSON message to a channel."""
        payload = json.dumps(message)
        await self.redis.publish(channel, payload)

    async def subscribe(
        self,
        channel: str,
        callback: Callable[[dict], Awaitable[None]],
    ):
        """
        Subscribe to a Redis channel and process incoming messages with the given callback.
        The callback should accept a dict.
        """
        pubsub = self.redis.pubsub()
        await pubsub.subscribe(channel)
        self._pubsubs.append(pubsub)

        async def _listener():
            print(f"📡 Subscribed to channel: {channel}")
            logger.info(f"📡 Subscribed to channel: {channel}")
            try:
                async for message in pubsub.listen():
                    if message["type"] == "message":
                        try:
                            data = json.loads(message["data"])
                            await callback(data)
                        except json.JSONDecodeError:
                            print(
                                f"⚠️ Received non-JSON message on {channel}: {message['data']}"
                            )
            except asyncio.CancelledError:
                print(f"🛑 Subscription to {channel} cancelled")
                logger.info(f"🛑 Subscription to {channel} cancelled")
                await pubsub.close()
            except ConnectionError as e:
                print(f"❌ Redis connection error on channel {channel}: {e}")
                logger.error(f"❌ Redis connection error on channel {channel}: {e}")

        task = asyncio.create_task(_listener())
        self._tasks.append(task)

    async def close(self):
        """Cancel listener tasks and close pubsubs."""
        for task in self._tasks:
            task.cancel()
        for pubsub in self._pubsubs:
            await pubsub.close()
