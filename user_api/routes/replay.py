from fastapi import APIRouter, status, Depends, Body
from fastapi.responses import JSONResponse
import json
import uuid

try:
    from . import auth, replay_helper as helper
    from db import get_redis_conn
except:
    from . import auth, replay_helper as helper
    from ..db import get_redis_conn


router = APIRouter(prefix="/replay")


@router.post("/session/start")
async def start_replay_session(
    payload: dict = Body(default={}),
    email_id=Depends(auth.verify_access_token),
    redis_conn=Depends(get_redis_conn),
):
    instrument_id = payload.get("instrument_id")
    speed = float(payload.get("speed", 1.0))
    timestamp_start = payload.get("timestamp_start")
    timestamp_end = payload.get("timestamp_end")

    session_id = payload.get("session_id") or uuid.uuid4().hex[:16]

    response = await helper.create_replay_session(
        redis_conn=redis_conn,
        session_id=session_id,
        instrument_id=instrument_id,
        speed=speed,
        timestamp_start=timestamp_start,
        timestamp_end=timestamp_end,
    )

    return JSONResponse(
        content=json.dumps(response, default=str),
        status_code=status.HTTP_200_OK,
    )


@router.get("/session/list")
async def replay_session_list(
    email_id=Depends(auth.verify_access_token),
    redis_conn=Depends(get_redis_conn),
):
    sessions = await helper.list_replay_sessions(redis_conn=redis_conn)
    return JSONResponse(
        content=json.dumps({"sessions": sessions}, default=str),
        status_code=status.HTTP_200_OK,
    )


@router.get("/session/{session_id}")
async def replay_session_get(
    session_id: str,
    email_id=Depends(auth.verify_access_token),
    redis_conn=Depends(get_redis_conn),
):
    session = await helper.get_replay_session(redis_conn=redis_conn, session_id=session_id)
    if not session:
        return JSONResponse(
            content=json.dumps({"message": "Replay session not found"}),
            status_code=status.HTTP_404_NOT_FOUND,
        )

    return JSONResponse(
        content=json.dumps(session, default=str),
        status_code=status.HTTP_200_OK,
    )


@router.post("/session/{session_id}/control")
async def replay_session_control(
    session_id: str,
    payload: dict = Body(default={}),
    email_id=Depends(auth.verify_access_token),
    redis_conn=Depends(get_redis_conn),
):
    action = str(payload.get("action", "")).lower().strip()
    if action not in {"pause", "resume", "stop", "restart"}:
        return JSONResponse(
            content=json.dumps({"message": "Invalid action. Use pause, resume, stop, or restart."}),
            status_code=status.HTTP_400_BAD_REQUEST,
        )

    session = await helper.control_replay_session(
        redis_conn=redis_conn,
        session_id=session_id,
        action=action,
    )
    if not session:
        return JSONResponse(
            content=json.dumps({"message": "Replay session not found"}),
            status_code=status.HTTP_404_NOT_FOUND,
        )

    return JSONResponse(
        content=json.dumps(session, default=str),
        status_code=status.HTTP_200_OK,
    )


@router.delete("/session/{session_id}")
async def delete_replay_session(
    session_id: str,
    email_id=Depends(auth.verify_access_token),
    redis_conn=Depends(get_redis_conn),
):
    result = await helper.delete_replay_session(redis_conn=redis_conn, session_id=session_id)
    if not result:
        return JSONResponse(
            content=json.dumps({"message": "Replay session not found"}),
            status_code=status.HTTP_404_NOT_FOUND,
        )

    return JSONResponse(
        content=json.dumps({"message": "Replay session deleted", "session_id": session_id}),
        status_code=status.HTTP_200_OK,
    )
