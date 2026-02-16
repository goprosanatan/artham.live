"""
Order State Manager Service
===========================

Single writer for bracket and order state. Consumes approved commands and
broker updates, persists state, and publishes lifecycle events.
"""

import asyncio
import json
import logging
import os
import time
import uuid
from datetime import datetime
from typing import Optional
from enum import Enum
from decouple import config
from redis.asyncio import Redis
from prometheus_client import Counter, Gauge, Histogram, start_http_server
from zoneinfo import ZoneInfo


def time_converter(*args):
    return datetime.now(ZoneInfo("Asia/Kolkata")).timetuple()


logger = logging.getLogger(__name__)
logging.Formatter.converter = time_converter

logging.basicConfig(
    filename=os.path.join(config("DIR_LOGS", cast=str), "artham_order_04_state_manager.log"),
    encoding="utf-8",
    level=logging.DEBUG,
    datefmt="%Y-%m-%d %H:%M:%S %p %Z",
    format="%(asctime)s %(levelname)-8s %(message)s",
)

REDIS_CONN = Redis(
    host=config("REDIS_HOST", cast=str),
    port=config("REDIS_PORT", cast=int),
    decode_responses=True,
)

GROUP_NAME = config("ORDER_STATE_MANAGER_GROUP", cast=str)
METRICS_PORT = config("ORDER_STATE_MANAGER_METRICS_PORT", cast=int)
PAPER_TRADING = config("ORDER_MANAGER_PAPER_TRADING", cast=bool)

STREAM_ORDER_BROKER_COMMANDS = config("STREAM_ORDER_BROKER_COMMANDS", cast=str)
STREAM_ORDER_STATE_COMMANDS = config("STREAM_ORDER_STATE_COMMANDS", cast=str)
STREAM_ORDER_COMMAND_RESPONSES = config("STREAM_ORDER_COMMAND_RESPONSES", cast=str)
STREAM_ORDER_UPDATES = config("STREAM_ORDER_UPDATES", cast=str)
STREAM_ORDER_EVENTS = config("STREAM_ORDER_EVENTS", cast=str)

BROKER_ORDER_MAPPING = config("BROKER_ORDER_MAPPING", cast=str)


class BracketState(str, Enum):
    CREATED = "CREATED"
    ENTRY_PLACED = "ENTRY_PLACED"
    ENTRY_FILLED = "ENTRY_FILLED"
    EXIT_ORDERS_PLACED = "EXIT_ORDERS_PLACED"
    TARGET_FILLED = "TARGET_FILLED"
    STOPLOSS_FILLED = "STOPLOSS_FILLED"
    COMPLETED = "COMPLETED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"


class OrderState(str, Enum):
    PENDING = "PENDING"
    PLACED = "PLACED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"


class OrderType(str, Enum):
    LIMIT = "LIMIT"
    MARKET = "MARKET"
    SL = "SL"
    SL_M = "SL-M"


STATE_COMMANDS_TOTAL = Counter("state_commands_total", "State commands processed")
STATE_UPDATES_TOTAL = Counter("state_updates_total", "Broker updates processed")
STATE_EVENTS_TOTAL = Counter("state_events_total", "Events published", ["event_type"])
STATE_BRACKETS_ACTIVE = Gauge("state_brackets_active", "Active brackets")
STATE_LATENCY_SECONDS = Histogram("state_latency_seconds", "State operation latency")
STATE_REDIS_CONNECTED = Gauge("state_redis_connected", "Redis connectivity")


def generate_id() -> str:
    return str(uuid.uuid4())


def now_iso() -> str:
    return datetime.now(ZoneInfo("Asia/Kolkata")).isoformat()


def create_state_transition(state: str, timestamp: Optional[str] = None) -> dict:
    """Create a state transition record."""
    return {
        "state": state,
        "timestamp": timestamp or now_iso()
    }


def get_state_transitions_list(state_transitions_str: Optional[str]) -> list:
    """Parse state transitions from JSON string, return list."""
    if not state_transitions_str:
        return []
    try:
        return json.loads(state_transitions_str) if isinstance(state_transitions_str, str) else state_transitions_str
    except:
        return []


def update_state_transitions(existing_str: Optional[str], new_state: str) -> str:
    """Add a new state transition to the list and return as JSON string."""
    transitions = get_state_transitions_list(existing_str)
    transitions.append(create_state_transition(new_state))
    return json.dumps(transitions)



def normalize_for_redis(payload: dict) -> dict:
    out = {}
    for k, v in payload.items():
        if v is None:
            continue
        elif isinstance(v, bool):
            out[k] = int(v)
        elif isinstance(v, datetime):
            out[k] = v.isoformat()
        elif isinstance(v, Enum):
            out[k] = v.value
        else:
            out[k] = v
    return out


def print_state_change(bracket_id: str, old_state: str, new_state: str):
    print(f"[STATE_MANAGER] Bracket {bracket_id} state: {old_state} -> {new_state}")


async def publish_event(event_type: str, bracket_id: str, order_id: Optional[str] = None, details: Optional[dict] = None):
    try:
        event = {
            "event_type": event_type,
            "bracket_id": bracket_id,
            "order_id": order_id or "",
            "timestamp": now_iso(),
            "details": json.dumps(details or {}),
        }
        await REDIS_CONN.xadd(
            name=STREAM_ORDER_EVENTS,
            fields=normalize_for_redis(event),
            maxlen=100000,
            approximate=True,
        )
        STATE_EVENTS_TOTAL.labels(event_type=event_type).inc()
    except Exception as e:
        logger.exception(f"Failed to publish event {event_type}: {e}")


async def publish_broker_command(command: str, payload: dict):
    try:
        cmd = {"command": command, **payload}
        await REDIS_CONN.xadd(
            name=STREAM_ORDER_BROKER_COMMANDS,
            fields=normalize_for_redis(cmd),
            maxlen=100000,
            approximate=True,
        )
    except Exception as e:
        logger.exception(f"Failed to publish broker command {command}: {e}")


async def send_response(request_id: str, success: bool, message: str, data: dict = None):
    if not request_id:
        return
    try:
        response = {
            "request_id": request_id,
            "success": int(success),
            "message": message,
            "timestamp": now_iso(),
            "data": json.dumps(data or {}),
        }
        await REDIS_CONN.xadd(
            name=STREAM_ORDER_COMMAND_RESPONSES,
            fields=normalize_for_redis(response),
            maxlen=50000,
            approximate=True,
        )
    except Exception as e:
        logger.exception(f"Failed to send response: {e}")


async def create_bracket(cmd: dict):
    try:
        request_id = cmd.get("request_id")
        bracket_id = generate_id()
        entry_order_id = generate_id()
        target_order_id = generate_id()
        stoploss_order_id = generate_id()

        bracket = {
            "bracket_id": bracket_id,
            "strategy_id": cmd.get("strategy_id"),
            "instrument_id": str(cmd.get("instrument_id")),
            "symbol": cmd.get("symbol", ""),
            "exchange": cmd.get("exchange", "NSE"),
            "side": cmd.get("side"),
            "qty": int(cmd.get("qty")),
            "entry_order_id": entry_order_id,
            "target_order_id": target_order_id,
            "stoploss_order_id": stoploss_order_id,
            "entry_price": float(cmd.get("entry_price")),
            "target_price": float(cmd.get("target_price")),
            "stoploss_price": float(cmd.get("stoploss_price")),
            "entry_start_ts": str(cmd.get("entry_start_ts", "")),
            "entry_end_ts": str(cmd.get("entry_end_ts", "")),
            "target_start_ts": str(cmd.get("target_start_ts", "")),
            "target_end_ts": str(cmd.get("target_end_ts", "")),
            "stop_start_ts": str(cmd.get("stop_start_ts", "")),
            "stop_end_ts": str(cmd.get("stop_end_ts", "")),
            "filled_entry_price": "",
            "state": BracketState.CREATED.value,
            "state_transitions": update_state_transitions(None, BracketState.CREATED.value),
            "created_at": now_iso(),
            "updated_at": now_iso(),
        }

        await REDIS_CONN.hset(
            f"oms:bracket:{bracket_id}",
            mapping=normalize_for_redis(bracket),
        )
        print_state_change(bracket_id, "NONE", BracketState.CREATED.value)

        entry_order = {
            "order_id": entry_order_id,
            "bracket_id": bracket_id,
            "instrument_id": str(cmd.get("instrument_id")),
            "symbol": cmd.get("symbol", ""),
            "exchange": cmd.get("exchange", "NSE"),
            "side": cmd.get("side"),
            "qty": int(cmd.get("qty")),
            "order_type": OrderType.LIMIT.value,
            "price": float(cmd.get("entry_price")),
            "trigger_price": "",
            "state": OrderState.PLACED.value,
            "filled_price": "",
            "filled_qty": "",
            "broker_order_id": "",
            "created_at": now_iso(),
            "updated_at": now_iso(),
        }

        await REDIS_CONN.hset(
            f"oms:order:{entry_order_id}",
            mapping=normalize_for_redis(entry_order),
        )

        await REDIS_CONN.sadd("oms:active:brackets", bracket_id)
        await REDIS_CONN.sadd(f"oms:active:instrument:{cmd.get('instrument_id')}", bracket_id)
        await REDIS_CONN.sadd(f"oms:active:strategy:{cmd.get('strategy_id')}", bracket_id)

        STATE_BRACKETS_ACTIVE.inc()

        await publish_event("BRACKET_CREATED", bracket_id, details=bracket)
        await publish_event("ENTRY_PLACED", bracket_id, entry_order_id, details=entry_order)

        if not PAPER_TRADING:
            await publish_broker_command(
                "PLACE_ORDER",
                {
                    "order_id": entry_order_id,
                    "instrument_id": str(cmd.get("instrument_id")),
                    "symbol": cmd.get("symbol", ""),
                    "exchange": cmd.get("exchange", "NSE"),
                    "side": cmd.get("side"),
                    "qty": int(cmd.get("qty")),
                    "order_type": OrderType.LIMIT.value,
                    "price": str(float(cmd.get("entry_price"))),
                    "trigger_price": "",
                },
            )

        await REDIS_CONN.hset(
            f"oms:bracket:{bracket_id}",
            mapping={"state": BracketState.ENTRY_PLACED.value, "state_transitions": json.dumps([create_state_transition(BracketState.CREATED.value), create_state_transition(BracketState.ENTRY_PLACED.value)]), "updated_at": now_iso()},
        )
        print_state_change(bracket_id, BracketState.CREATED.value, BracketState.ENTRY_PLACED.value)

        await send_response(request_id, True, "Bracket created", {"bracket_id": bracket_id})
        logger.info(f"Created bracket {bracket_id}")

    except Exception as e:
        logger.exception(f"Failed to create bracket: {e}")
        await send_response(cmd.get("request_id"), False, f"Failed to create bracket: {str(e)}")


async def cancel_bracket(cmd: dict):
    try:
        request_id = cmd.get("request_id")
        bracket_id = cmd.get("bracket_id")

        bracket = await REDIS_CONN.hgetall(f"oms:bracket:{bracket_id}")
        if not bracket:
            await send_response(request_id, False, "Bracket not found")
            return

        for order_key in ["entry_order_id", "target_order_id", "stoploss_order_id"]:
            order_id = bracket.get(order_key)
            if order_id:
                await REDIS_CONN.hset(
                    f"oms:order:{order_id}",
                    mapping={"state": OrderState.CANCELLED.value, "updated_at": now_iso()},
                )

        old_state = bracket.get("state", "UNKNOWN")
        new_state = BracketState.CANCELLED.value
        state_transitions = update_state_transitions(bracket.get("state_transitions"), new_state)
        
        await REDIS_CONN.hset(
            f"oms:bracket:{bracket_id}",
            mapping={"state": new_state, "state_transitions": state_transitions, "updated_at": now_iso()},
        )
        print_state_change(bracket_id, old_state, new_state)

        await REDIS_CONN.srem("oms:active:brackets", bracket_id)
        await REDIS_CONN.srem(f"oms:active:instrument:{bracket.get('instrument_id')}", bracket_id)
        await REDIS_CONN.srem(f"oms:active:strategy:{bracket.get('strategy_id')}", bracket_id)

        STATE_BRACKETS_ACTIVE.dec()

        await publish_event("BRACKET_CANCELLED", bracket_id)
        await send_response(request_id, True, "Bracket cancelled")
        logger.info(f"Cancelled bracket {bracket_id}")

    except Exception as e:
        logger.exception(f"Failed to cancel bracket: {e}")
        await send_response(cmd.get("request_id"), False, f"Failed to cancel: {str(e)}")


async def modify_sl_tp(cmd: dict):
    try:
        request_id = cmd.get("request_id")
        bracket_id = cmd.get("bracket_id")
        target_price = cmd.get("target_price")
        stoploss_price = cmd.get("stoploss_price")

        bracket = await REDIS_CONN.hgetall(f"oms:bracket:{bracket_id}")
        if not bracket:
            await send_response(request_id, False, "Bracket not found")
            return

        updates = {"updated_at": now_iso()}
        if target_price not in [None, ""]:
            updates["target_price"] = float(target_price)
        if stoploss_price not in [None, ""]:
            updates["stoploss_price"] = float(stoploss_price)

        await REDIS_CONN.hset(f"oms:bracket:{bracket_id}", mapping=updates)

        if bracket.get("state") == BracketState.EXIT_ORDERS_PLACED.value:
            if target_price not in [None, ""]:
                await REDIS_CONN.hset(
                    f"oms:order:{bracket.get('target_order_id')}",
                    mapping={"price": float(target_price), "updated_at": now_iso()},
                )
            if stoploss_price not in [None, ""]:
                await REDIS_CONN.hset(
                    f"oms:order:{bracket.get('stoploss_order_id')}",
                    mapping={"trigger_price": float(stoploss_price), "updated_at": now_iso()},
                )

        await publish_event(
            "SL_TP_MODIFIED",
            bracket_id,
            details={"target_price": target_price, "stoploss_price": stoploss_price},
        )
        await send_response(request_id, True, "Modified SL/TP")
        logger.info(f"Modified SL/TP for bracket {bracket_id}")

    except Exception as e:
        logger.exception(f"Failed to modify SL/TP: {e}")
        await send_response(cmd.get("request_id"), False, f"Failed to modify: {str(e)}")


async def force_exit(cmd: dict):
    try:
        request_id = cmd.get("request_id")
        bracket_id = cmd.get("bracket_id")
        exit_price = cmd.get("exit_price")

        bracket = await REDIS_CONN.hgetall(f"oms:bracket:{bracket_id}")
        if not bracket:
            await send_response(request_id, False, "Bracket not found")
            return

        state = bracket.get("state")
        if state in [BracketState.CREATED.value, BracketState.ENTRY_PLACED.value]:
            await cancel_bracket({"request_id": request_id, "bracket_id": bracket_id})
            await publish_event("FORCE_EXIT", bracket_id)
            return

        if state == BracketState.ENTRY_FILLED.value:
            await place_exit_orders(bracket)

        price = None
        if exit_price not in [None, ""]:
            try:
                price = float(exit_price)
            except Exception:
                price = None
        if price is None:
            try:
                price = float(bracket.get("filled_entry_price") or bracket.get("entry_price"))
            except Exception:
                price = None

        await execute_exit(bracket_id, "stoploss", filled_price=price)
        await publish_event("FORCE_EXIT", bracket_id, details={"exit_price": price})
        await send_response(request_id, True, "Force exit executed")

    except Exception as e:
        logger.exception(f"Failed to force exit: {e}")
        await send_response(cmd.get("request_id"), False, f"Failed to force exit: {str(e)}")


async def mark_entry_filled(bracket: dict, fill_price: float, actual_filled_qty: int = None):
    """Mark entry order as filled, handling partial fill scenarios.
    
    Args:
        bracket: Bracket data dict
        fill_price: Average fill price
        actual_filled_qty: Actual quantity filled (may be less than bracket qty)
                          If None, uses full bracket qty (backward compatible)
    """
    bracket_id = bracket.get("bracket_id")
    entry_order_id = bracket.get("entry_order_id")
    original_qty = int(bracket.get("qty", 0))
    
    # Use actual filled qty if provided, otherwise default to full qty
    filled_qty = actual_filled_qty if actual_filled_qty is not None else original_qty
    remaining_qty = original_qty - filled_qty
    
    # Update entry order with actual filled quantity
    await REDIS_CONN.hset(
        f"oms:order:{entry_order_id}",
        mapping={
            "state": OrderState.FILLED.value,
            "filled_price": fill_price,
            "filled_qty": filled_qty,
            "updated_at": now_iso(),
        },
    )

    # Update bracket with actual filled quantity and remaining quantity
    old_state = bracket.get("state", "UNKNOWN")
    new_state = BracketState.ENTRY_FILLED.value
    state_transitions = update_state_transitions(bracket.get("state_transitions"), new_state)
    
    bracket_updates = {
        "state": new_state,
        "state_transitions": state_transitions,
        "filled_entry_price": fill_price,
        "filled_qty": filled_qty,
        "remaining_qty": remaining_qty,
        "updated_at": now_iso(),
    }
    
    await REDIS_CONN.hset(
        f"oms:bracket:{bracket_id}",
        mapping=bracket_updates,
    )
    print_state_change(bracket_id, old_state, new_state)
    
    if remaining_qty > 0:
        logger.warning(f"Partial fill detected for bracket {bracket_id}: filled {filled_qty}/{original_qty}, remaining {remaining_qty}")

    await publish_event(
        "ENTRY_FILLED",
        bracket_id,
        entry_order_id,
        details={
            "filled_price": fill_price,
            "filled_qty": filled_qty,
            "original_qty": original_qty,
            "remaining_qty": remaining_qty,
        },
    )


async def handle_entry_hit(cmd: dict):
    try:
        bracket_id = cmd.get("bracket_id")
        if not bracket_id:
            return

        bracket = await REDIS_CONN.hgetall(f"oms:bracket:{bracket_id}")
        if not bracket:
            return

        state = bracket.get("state")
        if state not in [BracketState.ENTRY_PLACED.value, BracketState.CREATED.value]:
            return

        try:
            fill_price = float(cmd.get("filled_price"))
        except Exception:
            try:
                fill_price = float(bracket.get("entry_price", 0))
            except Exception:
                fill_price = 0

        # Support partial fills from external fills
        actual_filled_qty = None
        if cmd.get("filled_qty"):
            try:
                actual_filled_qty = int(cmd.get("filled_qty"))
            except Exception:
                pass

        await mark_entry_filled(bracket, fill_price, actual_filled_qty)
        # Refetch bracket to get updated state
        bracket = await REDIS_CONN.hgetall(f"oms:bracket:{bracket_id}")
        await place_exit_orders(bracket)
    except Exception as e:
        logger.exception(f"Failed to handle entry hit: {e}")


async def place_exit_orders(bracket: dict):
    try:
        bracket_id = bracket.get("bracket_id")
        target_order_id = bracket.get("target_order_id")
        stoploss_order_id = bracket.get("stoploss_order_id")
        
        # Use actual filled quantity for exit orders (partial fill support)
        exit_qty = int(bracket.get("filled_qty")) if bracket.get("filled_qty") else int(bracket.get("qty", 0))
        remaining_qty = int(bracket.get("remaining_qty", 0))
        
        if exit_qty <= 0:
            logger.error(f"Cannot place exit orders for bracket {bracket_id}: exit_qty={exit_qty}")
            return

        target_order = {
            "order_id": target_order_id,
            "bracket_id": bracket_id,
            "instrument_id": bracket.get("instrument_id"),
            "symbol": bracket.get("symbol", ""),
            "exchange": bracket.get("exchange", "NSE"),
            "side": "SELL" if bracket.get("side") == "BUY" else "BUY",
            "qty": exit_qty,
            "order_type": OrderType.LIMIT.value,
            "price": float(bracket.get("target_price")),
            "trigger_price": "",
            "state": OrderState.PLACED.value,
            "filled_price": "",
            "filled_qty": "",
            "broker_order_id": "",
            "created_at": now_iso(),
            "updated_at": now_iso(),
        }

        await REDIS_CONN.hset(
            f"oms:order:{target_order_id}",
            mapping=normalize_for_redis(target_order),
        )

        stoploss_order = {
            "order_id": stoploss_order_id,
            "bracket_id": bracket_id,
            "instrument_id": bracket.get("instrument_id"),
            "symbol": bracket.get("symbol", ""),
            "exchange": bracket.get("exchange", "NSE"),
            "side": "SELL" if bracket.get("side") == "BUY" else "BUY",
            "qty": exit_qty,
            "order_type": OrderType.SL_M.value,
            "price": "",
            "trigger_price": float(bracket.get("stoploss_price")),
            "state": OrderState.PLACED.value,
            "filled_price": "",
            "filled_qty": "",
            "broker_order_id": "",
            "created_at": now_iso(),
            "updated_at": now_iso(),
        }

        await REDIS_CONN.hset(
            f"oms:order:{stoploss_order_id}",
            mapping=normalize_for_redis(stoploss_order),
        )
        
        if remaining_qty > 0:
            logger.warning(f"Bracket {bracket_id} has partial fill: exit orders placed for {exit_qty}, but {remaining_qty} units remain unfilled")

        old_state = bracket.get("state", "UNKNOWN")
        new_state = BracketState.EXIT_ORDERS_PLACED.value
        state_transitions = update_state_transitions(bracket.get("state_transitions"), new_state)
        
        await REDIS_CONN.hset(
            f"oms:bracket:{bracket_id}",
            mapping={"state": new_state, "state_transitions": state_transitions, "updated_at": now_iso()},
        )
        print_state_change(bracket_id, old_state, new_state)

        await publish_event(
            "EXIT_ORDERS_PLACED",
            bracket_id,
            details={
                "target_order_id": target_order_id,
                "stoploss_order_id": stoploss_order_id,
                "exit_qty": exit_qty,
                "remaining_qty": remaining_qty,
            },
        )
        await publish_event("TARGET_PLACED", bracket_id, target_order_id, details=target_order)
        await publish_event("STOPLOSS_PLACED", bracket_id, stoploss_order_id, details=stoploss_order)

        if not PAPER_TRADING:
            # Cancel any remaining unfilled entry quantity
            if remaining_qty > 0:
                logger.info(f"Cancelling remaining {remaining_qty} units of entry order for bracket {bracket_id}")
                await publish_broker_command(
                    "CANCEL_ORDER",
                    {
                        "order_id": bracket.get("entry_order_id"),
                        "partial_cancel": True,
                        "cancel_qty": remaining_qty,
                    },
                )
            
            # Place exit orders for the actual filled quantity
            await publish_broker_command(
                "PLACE_ORDER",
                {
                    "order_id": target_order_id,
                    "instrument_id": bracket.get("instrument_id"),
                    "symbol": bracket.get("symbol", ""),
                    "exchange": bracket.get("exchange", "NSE"),
                    "side": target_order.get("side"),
                    "qty": exit_qty,
                    "order_type": OrderType.LIMIT.value,
                    "price": float(bracket.get("target_price")),
                    "trigger_price": "",
                },
            )
            await publish_broker_command(
                "PLACE_ORDER",
                {
                    "order_id": stoploss_order_id,
                    "instrument_id": bracket.get("instrument_id"),
                    "symbol": bracket.get("symbol", ""),
                    "exchange": bracket.get("exchange", "NSE"),
                    "side": stoploss_order.get("side"),
                    "qty": exit_qty,
                    "order_type": OrderType.SL_M.value,
                    "price": "",
                    "trigger_price": float(bracket.get("stoploss_price")),
                },
            )

    except Exception as e:
        logger.exception(f"Failed to place exit orders: {e}")


async def execute_exit(bracket_id: str, exit_type: str, filled_price: Optional[float] = None, filled_qty: Optional[int] = None):
    try:
        bracket = await REDIS_CONN.hgetall(f"oms:bracket:{bracket_id}")
        if not bracket:
            return

        if exit_type == "target":
            filled_order_id = bracket.get("target_order_id")
            cancel_order_id = bracket.get("stoploss_order_id")
            new_bracket_state = BracketState.TARGET_FILLED.value
            event_type = "TARGET_FILLED"
        else:
            filled_order_id = bracket.get("stoploss_order_id")
            cancel_order_id = bracket.get("target_order_id")
            new_bracket_state = BracketState.STOPLOSS_FILLED.value
            event_type = "STOPLOSS_FILLED"

        filled_mapping = {
            "state": OrderState.FILLED.value,
            "filled_qty": filled_qty if filled_qty is not None else bracket.get("qty"),
            "updated_at": now_iso(),
        }
        if filled_price is not None:
            filled_mapping["filled_price"] = filled_price

        await REDIS_CONN.hset(
            f"oms:order:{filled_order_id}",
            mapping=filled_mapping,
        )

        await REDIS_CONN.hset(
            f"oms:order:{cancel_order_id}",
            mapping={"state": OrderState.CANCELLED.value, "updated_at": now_iso()},
        )

        if not PAPER_TRADING:
            await publish_broker_command("CANCEL_ORDER", {"order_id": cancel_order_id})
            await publish_event("EXIT_CANCELLED", bracket_id, cancel_order_id)

        old_state = bracket.get("state", "UNKNOWN")
        state_transitions = update_state_transitions(bracket.get("state_transitions"), new_bracket_state)
        
        await REDIS_CONN.hset(
            f"oms:bracket:{bracket_id}",
            mapping={"state": new_bracket_state, "state_transitions": state_transitions, "updated_at": now_iso()},
        )
        print_state_change(bracket_id, old_state, new_bracket_state)

        state_transitions = update_state_transitions(state_transitions, BracketState.COMPLETED.value)
        await REDIS_CONN.hset(
            f"oms:bracket:{bracket_id}",
            mapping={"state": BracketState.COMPLETED.value, "state_transitions": state_transitions, "updated_at": now_iso()},
        )
        print_state_change(bracket_id, new_bracket_state, BracketState.COMPLETED.value)

        await REDIS_CONN.srem("oms:active:brackets", bracket_id)
        await REDIS_CONN.srem(f"oms:active:instrument:{bracket.get('instrument_id')}", bracket_id)
        await REDIS_CONN.srem(f"oms:active:strategy:{bracket.get('strategy_id')}", bracket_id)

        STATE_BRACKETS_ACTIVE.dec()

        await publish_event(event_type, bracket_id, filled_order_id)
        await publish_event("BRACKET_COMPLETED", bracket_id)

    except Exception as e:
        logger.exception(f"Failed to execute exit for bracket {bracket_id}: {e}")


async def handle_order_update(update: dict):
    try:
        order_id = update.get("order_id")
        broker_order_id = update.get("broker_order_id")

        if order_id and broker_order_id:
            await REDIS_CONN.hset(BROKER_ORDER_MAPPING, broker_order_id, order_id)
        if not order_id and broker_order_id:
            order_id = await REDIS_CONN.hget(BROKER_ORDER_MAPPING, broker_order_id)

        if not order_id:
            logger.warning("Order update missing order_id")
            return

        order = await REDIS_CONN.hgetall(f"oms:order:{order_id}")
        if not order:
            logger.warning(f"Order {order_id} not found for update")
            return

        bracket_id = order.get("bracket_id")
        bracket = await REDIS_CONN.hgetall(f"oms:bracket:{bracket_id}") if bracket_id else {}

        status = (update.get("status") or "").upper()
        filled_qty = update.get("filled_qty") or update.get("filled_quantity")
        filled_price = update.get("filled_price") or update.get("average_price")

        order_updates = {"updated_at": now_iso()}
        if broker_order_id:
            order_updates["broker_order_id"] = broker_order_id

        if status in ["COMPLETE", "FILLED"]:
            order_updates["state"] = OrderState.FILLED.value
            if filled_qty is not None:
                order_updates["filled_qty"] = int(filled_qty)
            if filled_price is not None:
                order_updates["filled_price"] = float(filled_price)
        elif status in ["CANCELLED", "CANCELED"]:
            order_updates["state"] = OrderState.CANCELLED.value
        elif status in ["REJECTED"]:
            order_updates["state"] = OrderState.REJECTED.value
        elif status in ["PLACED", "OPEN", "PENDING", "TRIGGER PENDING"]:
            order_updates["state"] = OrderState.PLACED.value

        await REDIS_CONN.hset(f"oms:order:{order_id}", mapping=order_updates)

        if not bracket:
            return

        if order_id == bracket.get("entry_order_id"):
            if status in ["COMPLETE", "FILLED"]:
                fill_price = float(filled_price) if filled_price is not None else float(bracket.get("entry_price", 0))
                actual_filled_qty = int(filled_qty) if filled_qty is not None else None
                await mark_entry_filled(bracket, fill_price, actual_filled_qty)
                # Refetch bracket to get updated state
                bracket = await REDIS_CONN.hgetall(f"oms:bracket:{bracket_id}")
                await place_exit_orders(bracket)
            elif status in ["REJECTED", "CANCELLED", "CANCELED"]:
                old_state = bracket.get("state", "UNKNOWN")
                await REDIS_CONN.hset(
                    f"oms:bracket:{bracket_id}",
                    mapping={"state": BracketState.REJECTED.value, "updated_at": now_iso()},
                )
                print_state_change(bracket_id, old_state, BracketState.REJECTED.value)
                await REDIS_CONN.srem("oms:active:brackets", bracket_id)
                await REDIS_CONN.srem(f"oms:active:instrument:{bracket.get('instrument_id')}", bracket_id)
                await REDIS_CONN.srem(f"oms:active:strategy:{bracket.get('strategy_id')}", bracket_id)
                STATE_BRACKETS_ACTIVE.dec()
                await publish_event(
                    "BRACKET_REJECTED",
                    bracket_id,
                    order_id,
                    details={"status": status, "reason": update.get("status_message", "")},
                )
        elif order_id in [bracket.get("target_order_id"), bracket.get("stoploss_order_id")]:
            if status in ["COMPLETE", "FILLED"]:
                exit_type = "target" if order_id == bracket.get("target_order_id") else "stoploss"
                await execute_exit(
                    bracket_id,
                    exit_type,
                    filled_price=float(filled_price) if filled_price is not None else None,
                    filled_qty=int(filled_qty) if filled_qty is not None else None,
                )
            elif status in ["REJECTED"]:
                await publish_event(
                    "ORDER_REJECTED",
                    bracket_id,
                    order_id,
                    details={"status": status, "reason": update.get("status_message", "")},
                )

        STATE_UPDATES_TOTAL.inc()

    except Exception as e:
        logger.exception(f"Failed to handle order update: {e}")


async def process_state_command(cmd: dict):
    t0 = time.perf_counter()
    command_type = (cmd.get("command") or "").upper()

    try:
        if command_type == "PLACE_BRACKET":
            await create_bracket(cmd)
        elif command_type == "CANCEL_BRACKET":
            await cancel_bracket(cmd)
        elif command_type == "MODIFY_SL_TP":
            await modify_sl_tp(cmd)
        elif command_type == "FORCE_EXIT":
            await force_exit(cmd)
        elif command_type == "ENTRY_HIT":
            await handle_entry_hit(cmd)
        elif command_type == "EXIT_HIT":
            await execute_exit(
                cmd.get("bracket_id"),
                cmd.get("exit_type"),
                filled_price=float(cmd.get("filled_price")) if cmd.get("filled_price") not in [None, ""] else None,
            )
        else:
            logger.warning(f"Unknown command type: {command_type}")

        STATE_COMMANDS_TOTAL.inc()
        STATE_LATENCY_SECONDS.observe(time.perf_counter() - t0)

    except Exception as e:
        logger.exception(f"Failed to process state command: {e}")


async def process_state_commands():
    logger.info("State command processor starting")
    streams = {STREAM_ORDER_STATE_COMMANDS: ">"}

    while True:
        try:
            resp = await REDIS_CONN.xreadgroup(
                groupname=GROUP_NAME,
                consumername="state_processor",
                streams=streams,
                count=100,
                block=3000,
            )

            if not resp:
                continue

            for stream, entries in resp:
                for msg_id, cmd in entries:
                    try:
                        await process_state_command(cmd)
                    except Exception as e:
                        logger.exception(f"Failed to process state command: {e}")

                    await REDIS_CONN.xack(stream, GROUP_NAME, msg_id)

        except Exception as e:
            logger.exception(f"State command loop error: {e}")
            await asyncio.sleep(1)


async def process_order_updates():
    logger.info("Order update processor starting")
    streams = {STREAM_ORDER_UPDATES: ">"}

    while True:
        try:
            resp = await REDIS_CONN.xreadgroup(
                groupname=GROUP_NAME,
                consumername="update_processor",
                streams=streams,
                count=200,
                block=3000,
            )

            if not resp:
                continue

            for stream, entries in resp:
                for msg_id, update in entries:
                    try:
                        await handle_order_update(update)
                    except Exception as e:
                        logger.exception(f"Failed to process update: {e}")

                    await REDIS_CONN.xack(stream, GROUP_NAME, msg_id)

        except Exception as e:
            logger.exception(f"Order update loop error: {e}")
            await asyncio.sleep(1)


async def init_consumer_groups():
    try:
        await REDIS_CONN.xgroup_create(
            name=STREAM_ORDER_STATE_COMMANDS,
            groupname=GROUP_NAME,
            id="0",
            mkstream=True,
        )
    except Exception:
        pass

    try:
        await REDIS_CONN.xgroup_create(
            name=STREAM_ORDER_UPDATES,
            groupname=GROUP_NAME,
            id="0",
            mkstream=True,
        )
    except Exception:
        pass


async def worker():
    try:
        if await REDIS_CONN.ping():
            print("[STATE_MANAGER] Connected to Redis")
            logger.info("Connected to Redis")
            STATE_REDIS_CONNECTED.set(1)
    except Exception as e:
        print(f"[STATE_MANAGER][ERROR] Redis connection failed: {e}")
        logger.error(f"Redis connection failed: {e}")
        STATE_REDIS_CONNECTED.set(0)
        return

    await init_consumer_groups()
    logger.info("Consumer groups initialized")

    print(f"[STATE_MANAGER] Starting processor. group={GROUP_NAME}")
    logger.info(f"Starting processor. group={GROUP_NAME}")

    await asyncio.gather(
        process_state_commands(),
        process_order_updates(),
    )


if __name__ == "__main__":
    try:
        start_http_server(METRICS_PORT)
        logger.info(f"Prometheus metrics server started on :{METRICS_PORT}")
    except Exception as e:
        logger.error(f"Failed to start metrics server: {e}")
        raise SystemExit(1)

    asyncio.run(worker())
