import os
from typing import Iterable, List

import redis


def chunked(items: List[str], size: int) -> Iterable[List[str]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def delete_keys(r: redis.Redis, keys: List[str]) -> int:
    deleted = 0
    for batch in chunked(keys, 500):
        deleted += r.delete(*batch)
    return deleted


def main() -> None:
    host = os.getenv("REDIS_HOST", "localhost")
    port = int(os.getenv("REDIS_PORT", "6379"))
    db = int(os.getenv("REDIS_DB", "0"))

    r = redis.Redis(host=host, port=port, db=db, decode_responses=True)

    patterns = [
        "oms:bracket:*",
        "oms:order:*",
        "oms:active:brackets",
        "oms:active:instrument:*",
        "oms:active:strategy:*",
        "oms:broker_order_mapping",
    ]

    stream_keys = [
        "oms:api_commands",
        "oms:risk_requests",
        "oms:state_commands",
        "oms:command_responses",
        "oms:commands",
        "oms:order_updates",
        "oms:events",
    ]

    total_deleted = 0

    for pattern in patterns:
        keys = list(r.scan_iter(match=pattern))
        if not keys:
            continue
        total_deleted += delete_keys(r, keys)

    for key in stream_keys:
        total_deleted += r.delete(key)

    print(f"Deleted {total_deleted} OMS keys/streams")


if __name__ == "__main__":
    main()
