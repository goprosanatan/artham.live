import os
from typing import Any, Iterable, List, Set, cast

import redis


def chunked(items: List[str], size: int) -> Iterable[List[str]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def delete_keys(r: redis.Redis, keys: List[str]) -> int:
    if not keys:
        return 0
    deleted = 0
    for batch in chunked(keys, 500):
        deleted += cast(int, r.delete(*batch))
    return deleted


def main() -> None:
    host = os.getenv("REDIS_HOST", "localhost")
    port = int(os.getenv("REDIS_PORT", "6379"))
    db = int(os.getenv("REDIS_DB", "0"))

    replay_stream_prefix = os.getenv("REPLAY_STREAM_PREFIX", "replay")
    replay_sessions_index_key = os.getenv("REPLAY_SESSIONS_INDEX_KEY", "replay:sessions")
    replay_session_key_prefix = os.getenv("REPLAY_SESSION_KEY_PREFIX", "replay:session:")
    replay_control_stream = os.getenv("REPLAY_CONTROL_STREAM", "replay:control")

    r = cast(Any, redis.Redis(host=host, port=port, db=db, decode_responses=True))

    keys_to_delete: Set[str] = set()

    # Collect session IDs from index and from session hash keys.
    session_ids = set(r.smembers(replay_sessions_index_key) or [])
    for session_key in r.scan_iter(match=f"{replay_session_key_prefix}*"):
        keys_to_delete.add(session_key)
        sid = str(session_key).replace(replay_session_key_prefix, "", 1)
        if sid:
            session_ids.add(sid)

    # Collect per-session replay keys (ticks, bars, clock, etc.)
    for session_id in session_ids:
        pattern = f"{replay_stream_prefix}:{session_id}:*"
        for key in r.scan_iter(match=pattern):
            keys_to_delete.add(key)
        keys_to_delete.add(f"{replay_session_key_prefix}{session_id}")

    # Also remove index + control stream so replay starts clean.
    keys_to_delete.add(replay_sessions_index_key)
    keys_to_delete.add(replay_control_stream)

    total_deleted = delete_keys(r, sorted(keys_to_delete))

    print(
        f"Deleted {total_deleted} replay keys/streams "
        f"(sessions found: {len(session_ids)})"
    )


if __name__ == "__main__":
    main()
