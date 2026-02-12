# ==============================================================================

from fastapi import APIRouter, Depends, HTTPException, status, Body
from fastapi.responses import JSONResponse
import logging
from typing import Optional
from pydantic import BaseModel, Field
from decouple import config

try:
    from . import auth, order_helper as helper
    from db import get_redis_conn
except Exception:
    from . import auth, order_helper as helper
    from ..db import get_redis_conn

# ==============================================================================


router = APIRouter(prefix="/order")
logger = logging.getLogger(__name__)

# ==============================================================================


OMS_COMMAND_STREAM = config("OMS_COMMAND_STREAM", cast=str, default="oms:api_commands")
OMS_REDIS_NAMESPACE = config("OMS_REDIS_NAMESPACE", cast=str, default="oms:bracket")


class BracketIntent(BaseModel):
    """Payload expected by the synthetic OCO OMS.

    Only PLACE_BRACKET commands are supported: entry + target + stoploss.
    """

    strategy_id: str = Field(default="manual_ui")
    instrument_id: int
    side: str  # BUY | SELL
    qty: int = Field(gt=0)
    entry_price: float
    target_price: float
    stoploss_price: float
    entry_start_ts: Optional[int] = None
    entry_end_ts: Optional[int] = None
    target_start_ts: Optional[int] = None
    target_end_ts: Optional[int] = None
    stop_start_ts: Optional[int] = None
    stop_end_ts: Optional[int] = None
    symbol: Optional[str] = None
    exchange: Optional[str] = None


class CancelBracketRequest(BaseModel):
    bracket_id: str


@router.post("/bracket")
async def submit_bracket_order(
    intent: BracketIntent,
    email_id=Depends(auth.verify_access_token),
    redis_conn=Depends(get_redis_conn),
):
    """Queue a PLACE_BRACKET command into the OMS command stream."""
    try:
        side = intent.side.upper()
        if side not in {"BUY", "SELL"}:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="side must be BUY or SELL",
            )

        cmd = {
            "command": "PLACE_BRACKET",
            "strategy_id": intent.strategy_id,
            "instrument_id": intent.instrument_id,
            "side": side,
            "qty": intent.qty,
            "entry_price": intent.entry_price,
            "target_price": intent.target_price,
            "stoploss_price": intent.stoploss_price,
            "entry_start_ts": intent.entry_start_ts,
            "entry_end_ts": intent.entry_end_ts,
            "target_start_ts": intent.target_start_ts,
            "target_end_ts": intent.target_end_ts,
            "stop_start_ts": intent.stop_start_ts,
            "stop_end_ts": intent.stop_end_ts,
            "symbol": intent.symbol or "",
            "exchange": intent.exchange or "",
        }
        stream_id = await helper.enqueue_command(
            redis_conn,
            stream=OMS_COMMAND_STREAM,
            data=cmd,
            maxlen=100000,
        )
    except Exception as e:
        logger.exception("Failed to enqueue bracket order")
        print(f"[ORDER] Failed to enqueue bracket order: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to submit order",
        ) from e

    return JSONResponse(
        content={
            "status": "queued",
            "stream": OMS_COMMAND_STREAM,
            "stream_id": stream_id,
        },
        status_code=status.HTTP_202_ACCEPTED,
    )


@router.get("/list")
async def list_bracket_orders(
    limit: int = 100,
    email_id=Depends(auth.verify_access_token),
    redis_conn=Depends(get_redis_conn),
):
    try:
        orders = await helper.list_brackets(
            redis_conn,
            namespace=OMS_REDIS_NAMESPACE,
            limit=max(1, min(limit, 500)),
        )
        queued = await helper.list_pending_intents(
            redis_conn,
            stream=OMS_COMMAND_STREAM,
            limit=50,
        )
        # Avoid duplicates if OMS already persisted same bracket_id or same intent payload
        seen = {o.get("bracket_id") for o in orders if o.get("bracket_id")}

        def intent_matches_order(intent, order):
            # Match on key fields; order state not considered
            return (
                str(intent.get("instrument_id")) == str(order.get("instrument_id"))
                and (intent.get("side") or "").upper()
                == (order.get("side") or "").upper()
                and int(intent.get("qty", 0)) == int(order.get("qty", 0))
                and str(intent.get("entry_price")) == str(order.get("entry_price"))
                and str(intent.get("target_price")) == str(order.get("target_price"))
                and str(intent.get("stoploss_price"))
                == str(order.get("stoploss_price"))
            )

        for item in queued:
            if item.get("bracket_id") and item["bracket_id"] in seen:
                continue

            # Skip queued intent if an existing persisted order with same intent exists
            duplicate = any(
                intent_matches_order(item, existing)
                for existing in orders
                if existing.get("state") and existing.get("state") != "QUEUED"
            )
            if duplicate:
                continue

            orders.append(item)
    except Exception as e:
        logger.exception("Failed to fetch orders")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch orders",
        ) from e

    return JSONResponse(content={"orders": orders}, status_code=status.HTTP_200_OK)


@router.get("/bracket/{bracket_id}")
async def get_bracket_order(
    bracket_id: str,
    email_id=Depends(auth.verify_access_token),
    redis_conn=Depends(get_redis_conn),
):
    """Fetch a single bracket order by ID."""
    try:
        bracket = await helper.get_bracket(redis_conn, bracket_id)
        if not bracket:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Bracket not found",
            )
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Failed to fetch bracket {bracket_id}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch bracket",
        ) from e

    return JSONResponse(content={"bracket": bracket}, status_code=status.HTTP_200_OK)


@router.delete("/bracket")
async def cancel_bracket_order(
    payload: CancelBracketRequest = Body(...),
    email_id=Depends(auth.verify_access_token),
    redis_conn=Depends(get_redis_conn),
):
    """Cancel an active bracket order by sending a CANCEL_BRACKET command to OMS."""
    try:
        bracket_id = payload.bracket_id
        if not bracket_id:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="bracket_id is required",
            )

        cmd = {
            "command": "CANCEL_BRACKET",
            "bracket_id": bracket_id,
        }
        stream_id = await helper.enqueue_command(
            redis_conn,
            stream=OMS_COMMAND_STREAM,
            data=cmd,
            maxlen=100000,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Failed to cancel bracket {payload.bracket_id}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to cancel bracket",
        ) from e

    return JSONResponse(
        content={
            "status": "cancel_queued",
            "stream": OMS_COMMAND_STREAM,
            "stream_id": stream_id,
        },
        status_code=status.HTTP_202_ACCEPTED,
    )


@router.delete("/bracket/delete")
async def delete_bracket_order(
    payload: CancelBracketRequest = Body(...),
    email_id=Depends(auth.verify_access_token),
    redis_conn=Depends(get_redis_conn),
):
    """Soft-delete a completed or cancelled bracket order.
    
    This prevents ghost orders by marking the bracket as deleted instead of 
    removing it from Redis, preserving the audit trail while filtering it 
    from API listings and WebSocket updates.
    """
    try:
        bracket_id = payload.bracket_id
        if not bracket_id:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="bracket_id is required",
            )

        # Fetch the bracket to check its state
        bracket = await helper.get_bracket(redis_conn, bracket_id)
        if not bracket:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Bracket not found or already deleted",
            )

        # Only allow deletion of completed or cancelled orders
        state = (bracket.get("state") or "").upper()
        if state not in ["COMPLETED", "CANCELLED", "REJECTED"]:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Cannot delete bracket in state {state}. Only COMPLETED, CANCELLED, or REJECTED orders can be deleted.",
            )

        # Perform soft delete
        success, message = await helper.soft_delete_bracket(redis_conn, bracket_id)
        if not success:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=message,
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Failed to delete bracket {payload.bracket_id}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to delete bracket",
        ) from e

    return JSONResponse(
        content={
            "status": "deleted",
            "bracket_id": bracket_id,
            "message": "Bracket order deleted successfully",
        },
        status_code=status.HTTP_200_OK,
    )
