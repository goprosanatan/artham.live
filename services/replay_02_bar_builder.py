import asyncio
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional, cast
from zoneinfo import ZoneInfo

from decouple import config
from prometheus_client import Counter, Gauge, Histogram, start_http_server
from redis.asyncio import Redis


def time_converter(*args):
    return datetime.now(ZoneInfo("Asia/Kolkata")).timetuple()


logger = logging.getLogger(__name__)
logging.Formatter.converter = time_converter
DIR_LOGS = str(config("DIR_LOGS", cast=str))
logging.basicConfig(
    filename=(os.path.join(DIR_LOGS, "artham_replay_02_bar_builder.log")),
    encoding="utf-8",
    level=logging.DEBUG,
    datefmt="%Y-%m-%d %H:%M:%S %p %Z",
    format="%(asctime)s %(levelname)-8s %(message)s",
)

REDIS_HOST = str(config("REDIS_HOST", cast=str))
REDIS_PORT = int(config("REDIS_PORT", cast=int))

REDIS_CONN = Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    decode_responses=True,
)

REPLAY_STREAM_PREFIX = str(config("REPLAY_STREAM_PREFIX", cast=str))
REPLAY_CONTROL_STREAM = str(config("REPLAY_CONTROL_STREAM", cast=str))
REPLAY_BAR_BUILDER_METRICS_PORT = int(
    config("REPLAY_BAR_BUILDER_METRICS_PORT", cast=int)
)

REPLAY_BAR_TICKS_READ_TOTAL = Counter(
    "replay_bar_ticks_read_total", "Total replay ticks consumed"
)
REPLAY_BAR_TICKS_PROCESSED_TOTAL = Counter(
    "replay_bar_ticks_processed_total", "Total replay ticks processed into bars"
)
REPLAY_BAR_PUBLISH_1M_TOTAL = Counter(
    "replay_bar_publish_1m_total", "Total replay 1m bar publishes"
)
REPLAY_BAR_PUBLISH_1D_TOTAL = Counter(
    "replay_bar_publish_1d_total", "Total replay 1D bar publishes"
)
REPLAY_BAR_ERRORS_TOTAL = Counter(
    "replay_bar_errors_total", "Total replay bar builder errors"
)
REPLAY_BAR_ACTIVE_SESSIONS = Gauge(
    "replay_bar_active_sessions", "Replay bar builder active sessions"
)
REPLAY_BAR_PROCESS_DURATION_SECONDS = Histogram(
    "replay_bar_process_duration_seconds", "Replay tick processing latency in seconds"
)


def replay_stream_key(session_id: str, stream_tail: str) -> str:
    return f"{REPLAY_STREAM_PREFIX}:{session_id}:{stream_tail}"


def minute_key(dt: datetime):
    return dt.replace(second=0, microsecond=0).isoformat()


def day_key(dt: datetime):
    return dt.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()


def parse_ts_ms(tick_payload: dict):
    candidates = [
        tick_payload.get("bar_ts"),
        tick_payload.get("ts_ms"),
        tick_payload.get("timestamp"),
        tick_payload.get("exchange_ts"),
    ]
    for value in candidates:
        try:
            if value is None:
                continue
            if isinstance(value, str) and "T" in value:
                parsed_dt = datetime.fromisoformat(value)
                return int(parsed_dt.timestamp() * 1000)
            parsed = int(float(value))
            if parsed > 0:
                return parsed
        except Exception:
            continue
    return None


def parse_exchange_dt(tick_payload: dict) -> Optional[datetime]:
    value = tick_payload.get("exchange_ts")
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except Exception:
            return None
    try:
        parsed = float(value)
        if parsed > 1_000_000_000_000:
            return datetime.fromtimestamp(parsed / 1000, tz=timezone.utc)
        if parsed > 0:
            return datetime.fromtimestamp(parsed, tz=timezone.utc)
    except Exception:
        return None
    return None


def parse_price(tick_payload: dict):
    candidates = [
        tick_payload.get("last_price"),
        tick_payload.get("ltp"),
        tick_payload.get("close"),
        tick_payload.get("price"),
    ]
    for value in candidates:
        try:
            if value is None:
                continue
            parsed = float(value)
            if parsed > 0:
                return parsed
        except Exception:
            continue
    return None


def parse_volume(tick_payload: dict):
    value = tick_payload.get("volume_traded")
    if value is None:
        return 0
    try:
        parsed = int(float(value))
        return parsed if parsed >= 0 else 0
    except Exception:
        return 0


def parse_oi(tick_payload: dict):
    try:
        oi_value = tick_payload.get("oi")
        if oi_value is None:
            return 0
        return int(float(oi_value))
    except Exception:
        return 0


def normalize_bar_payload(bar: dict) -> Dict[str, str]:
    return {
        "instrument_id": str(bar["instrument_id"]),
        "timeframe": str(bar["timeframe"]),
        "bar_ts": str(bar["bar_ts"]),
        "open": str(bar["open"]),
        "high": str(bar["high"]),
        "low": str(bar["low"]),
        "close": str(bar["close"]),
        "volume": str(bar["volume"]),
        "oi": str(bar["oi"]),
        "oi_change": str(bar["oi_change"]),
        "session_id": str(bar["session_id"]),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


class ReplayBarBuilderSession:
    def __init__(self, session_id: str):
        self.session_id = session_id
        self.task: Optional[asyncio.Task[Any]] = None
        self.run_event = asyncio.Event()
        self.stop_event = asyncio.Event()
        self.run_event.set()
        self.last_tick_id = "$"
        self.minute_bars = {}
        self.daily_bars = {}
        self.current_minute_key = {}
        self.current_day_key = {}
        self.minute_start_volume = {}
        self.day_start_volume = {}


SESSIONS = {}


def _publish_stream_for_timeframe(session_id: str, timeframe: str) -> str:
    if timeframe == "1D":
        return replay_stream_key(session_id, "bar:1D")
    return replay_stream_key(session_id, "bar:1m")


async def _publish_bar(redis_conn: Redis, session_id: str, bar: dict):
    stream_key = _publish_stream_for_timeframe(session_id, bar["timeframe"])
    payload = cast(dict, normalize_bar_payload(bar))
    await redis_conn.xadd(
        stream_key,
        payload,
        maxlen=200000,
        approximate=True,
    )
    logger.debug(
        "[REPLAY BAR] published %s bar: session=%s instrument=%s ts=%s ohlc=%s/%s/%s/%s vol=%s oi=%s",
        bar["timeframe"],
        session_id,
        bar.get("instrument_id"),
        bar.get("bar_ts"),
        bar.get("open"),
        bar.get("high"),
        bar.get("low"),
        bar.get("close"),
        bar.get("volume"),
        bar.get("oi"),
    )
    if bar["timeframe"] == "1D":
        REPLAY_BAR_PUBLISH_1D_TOTAL.inc()
    else:
        REPLAY_BAR_PUBLISH_1M_TOTAL.inc()


def _upsert_bar(existing_bar: dict, price: float, volume: int, oi: int):
    if not existing_bar:
        return {
            "open": price,
            "high": price,
            "low": price,
            "close": price,
            "volume": volume,
            "oi_start": oi,
            "oi": oi,
            "oi_change": 0,
        }

    existing_bar["high"] = max(existing_bar["high"], price)
    existing_bar["low"] = min(existing_bar["low"], price)
    existing_bar["close"] = price
    existing_bar["volume"] = volume
    existing_bar["oi"] = oi
    existing_bar["oi_change"] = oi - existing_bar.get("oi_start", oi)
    return existing_bar


async def _process_tick(session: ReplayBarBuilderSession, tick_payload: dict):
    started_at = time.perf_counter()
    instrument_id = tick_payload.get("instrument_id")
    price = parse_price(tick_payload)

    exchange_dt = parse_exchange_dt(tick_payload)
    if exchange_dt is None:
        ts_ms = parse_ts_ms(tick_payload)
        if ts_ms is not None:
            exchange_dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)

    if not instrument_id or exchange_dt is None or price is None:
        REPLAY_BAR_PROCESS_DURATION_SECONDS.observe(time.perf_counter() - started_at)
        return

    instrument_id = str(instrument_id)
    mkey = minute_key(exchange_dt)
    dkey = day_key(exchange_dt)
    volume_traded = parse_volume(tick_payload)
    oi = parse_oi(tick_payload)

    prev_mkey = session.current_minute_key.get(instrument_id)
    prev_dkey = session.current_day_key.get(instrument_id)

    minute_changed = prev_mkey is not None and prev_mkey != mkey
    day_changed = prev_dkey is not None and prev_dkey != dkey

    session.current_minute_key[instrument_id] = mkey
    session.current_day_key[instrument_id] = dkey

    # Upsert current 1m bar.
    if instrument_id not in session.minute_bars:
        session.minute_bars[instrument_id] = {}
    minute_bar = session.minute_bars[instrument_id].get(mkey)

    min_vol_map = session.minute_start_volume.setdefault(instrument_id, {})
    if mkey not in min_vol_map:
        min_vol_map[mkey] = volume_traded
    minute_volume = volume_traded - min_vol_map[mkey]
    min_vol_map[f"{mkey}_end"] = volume_traded

    minute_bar = _upsert_bar(minute_bar, price=price, volume=minute_volume, oi=oi)
    session.minute_bars[instrument_id][mkey] = minute_bar

    # Upsert current 1D bar.
    if instrument_id not in session.daily_bars:
        session.daily_bars[instrument_id] = {}
    day_bar = session.daily_bars[instrument_id].get(dkey)

    day_vol_map = session.day_start_volume.setdefault(instrument_id, {})
    if dkey not in day_vol_map:
        day_vol_map[dkey] = volume_traded
    day_volume = volume_traded - day_vol_map[dkey]

    day_bar = _upsert_bar(day_bar, price=price, volume=day_volume, oi=oi)
    session.daily_bars[instrument_id][dkey] = day_bar

    # Finalize previous buckets on rollover.
    if minute_changed and prev_mkey:
        prev_end_vol = min_vol_map.get(f"{prev_mkey}_end")
        if prev_end_vol is not None and mkey not in min_vol_map:
            min_vol_map[mkey] = prev_end_vol

        prev = session.minute_bars.get(instrument_id, {}).pop(prev_mkey, None)
        if prev:
            await _publish_bar(
                REDIS_CONN,
                session.session_id,
                {
                    "instrument_id": instrument_id,
                    "timeframe": "1m",
                    "bar_ts": prev_mkey,
                    **prev,
                    "session_id": session.session_id,
                },
            )
        min_vol_map.pop(prev_mkey, None)
        min_vol_map.pop(f"{prev_mkey}_end", None)

    if day_changed and prev_dkey:
        prev = session.daily_bars.get(instrument_id, {}).pop(prev_dkey, None)
        if prev:
            await _publish_bar(
                REDIS_CONN,
                session.session_id,
                {
                    "instrument_id": instrument_id,
                    "timeframe": "1D",
                    "bar_ts": prev_dkey,
                    **prev,
                    "session_id": session.session_id,
                },
            )
        day_vol_map.pop(prev_dkey, None)

    # Publish live snapshots every tick.
    await _publish_bar(
        REDIS_CONN,
        session.session_id,
        {
            "instrument_id": instrument_id,
            "timeframe": "1m",
            "bar_ts": mkey,
            **minute_bar,
            "session_id": session.session_id,
        },
    )
    REPLAY_BAR_TICKS_PROCESSED_TOTAL.inc()
    REPLAY_BAR_PROCESS_DURATION_SECONDS.observe(time.perf_counter() - started_at)
    await _publish_bar(
        REDIS_CONN,
        session.session_id,
        {
            "instrument_id": instrument_id,
            "timeframe": "1D",
            "bar_ts": dkey,
            **day_bar,
            "session_id": session.session_id,
        },
    )


async def _session_loop(session: ReplayBarBuilderSession):
    logger.info(f"[REPLAY BAR] session loop started: {session.session_id}")
    tick_stream_key = replay_stream_key(session.session_id, "md:ticks")

    while not session.stop_event.is_set():
        await session.run_event.wait()
        if session.stop_event.is_set():
            break

        try:
            resp = await REDIS_CONN.xread(
                streams={tick_stream_key: session.last_tick_id},
                count=500,
                block=1000,
            )
            if not resp:
                continue

            for _, messages in resp:
                for msg_id, values in messages:
                    REPLAY_BAR_TICKS_READ_TOTAL.inc()
                    await _process_tick(session, values)
                    session.last_tick_id = msg_id
        except asyncio.CancelledError:
            raise
        except Exception as e:
            REPLAY_BAR_ERRORS_TOTAL.inc()
            logger.exception(
                f"[REPLAY BAR] session loop error for {session.session_id}: {e}"
            )
            await asyncio.sleep(1)

    logger.info(f"[REPLAY BAR] session loop stopped: {session.session_id}")


async def _start_session(session_id: str):
    existing = SESSIONS.get(session_id)
    if existing and existing.task and not existing.task.done():
        existing.run_event.set()
        return

    session = ReplayBarBuilderSession(session_id=session_id)
    session.task = asyncio.create_task(_session_loop(session))
    SESSIONS[session_id] = session
    REPLAY_BAR_ACTIVE_SESSIONS.set(len(SESSIONS))


async def _pause_session(session_id: str):
    session = SESSIONS.get(session_id)
    if not session:
        return
    session.run_event.clear()


async def _resume_session(session_id: str):
    session = SESSIONS.get(session_id)
    if not session:
        return
    session.run_event.set()


async def _stop_session(session_id: str):
    session = SESSIONS.get(session_id)
    if not session:
        return
    session.stop_event.set()
    session.run_event.set()
    if session.task:
        session.task.cancel()
        try:
            await session.task
        except asyncio.CancelledError:
            pass
    SESSIONS.pop(session_id, None)
    REPLAY_BAR_ACTIVE_SESSIONS.set(len(SESSIONS))


async def _restart_session(session_id: str):
    """Restart a session: clear all internal state and reset to read from beginning"""
    logger.info(f"[REPLAY BAR] restarting session {session_id} - clearing all state")

    session = SESSIONS.get(session_id)
    if not session:
        return

    # Clear all aggregation state
    session.minute_bars.clear()
    session.daily_bars.clear()
    session.current_minute_key.clear()
    session.current_day_key.clear()
    session.minute_start_volume.clear()
    session.day_start_volume.clear()

    # Reset stream reading position to beginning
    session.last_tick_id = "$"

    # Resume if paused
    session.run_event.set()

    logger.info(
        f"[REPLAY BAR] session {session_id} state cleared, ready for fresh replay"
    )


async def _bootstrap_existing_sessions():
    # Optional bootstrap so service can recover after restart.
    replay_sessions_key = str(
        config("REPLAY_SESSIONS_INDEX_KEY", cast=str, default="replay:sessions")
    )
    redis_conn_any = cast(Any, REDIS_CONN)
    session_ids = await redis_conn_any.smembers(replay_sessions_key)
    if not session_ids:
        return

    replay_session_key_prefix = str(
        config("REPLAY_SESSION_KEY_PREFIX", cast=str, default="replay:session:")
    )
    for session_id in session_ids:
        session_key = replay_session_key_prefix + str(session_id)
        state = await redis_conn_any.hgetall(session_key)
        if not state:
            continue
        status = str(state.get("status", "")).lower().strip()
        if status in {"running", "paused"}:
            await _start_session(str(session_id))
            if status == "paused":
                await _pause_session(str(session_id))


async def run_control_loop():
    logger.info("[REPLAY BAR] service starting")
    start_http_server(REPLAY_BAR_BUILDER_METRICS_PORT)
    logger.info(f"[REPLAY BAR] metrics on :{REPLAY_BAR_BUILDER_METRICS_PORT}")
    await _bootstrap_existing_sessions()

    last_id = "$"
    while True:
        try:
            resp = await REDIS_CONN.xread(
                streams={REPLAY_CONTROL_STREAM: last_id},
                count=200,
                block=1000,
            )
            if not resp:
                continue

            for _, messages in resp:
                for msg_id, values in messages:
                    event = str(values.get("event", "")).strip().lower()
                    session_id = str(values.get("session_id", "")).strip()
                    if not session_id:
                        last_id = msg_id
                        continue

                    if event == "session_start":
                        await _start_session(session_id)
                    elif event == "session_pause":
                        await _pause_session(session_id)
                    elif event == "session_resume":
                        await _resume_session(session_id)
                    elif event == "session_stop":
                        await _stop_session(session_id)
                    elif event == "session_restart":
                        await _restart_session(session_id)

                    last_id = msg_id
        except asyncio.CancelledError:
            logger.info("[REPLAY BAR] service cancelled")
            raise
        except Exception as e:
            REPLAY_BAR_ERRORS_TOTAL.inc()
            logger.exception(f"[REPLAY BAR] control loop error: {e}")
            await asyncio.sleep(1)


async def main():
    await run_control_loop()


if __name__ == "__main__":
    asyncio.run(main())
