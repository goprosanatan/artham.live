# ==============================================================================

from fastapi import (
    APIRouter,
    Request,
    status,
    Depends,
    HTTPException,
)
from fastapi.responses import JSONResponse
import logging
import pandas as pd
import redis.asyncio as redis
from decouple import config
import json

from . import auth, user_helper as helper

# ==============================================================================

router = APIRouter(prefix="/user")
logger = logging.getLogger(__name__)

# ==============================================================================


@router.post("/login")
async def login(login_form: helper.LoginForm):

    user = helper.authenticate_user(
        email_id=login_form.email_id,
        password=login_form.password,
    )

    if user:
        access_token = auth.create_access_token(email_id=login_form.email_id)

        return JSONResponse(
            content="Login Successful",
            status_code=status.HTTP_200_OK,
            headers={"authorization": access_token},
        )

    else:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={},
        )


# ------------------------------------------------------


@router.post("/dashboard")
async def intitiate_dashboard(
    request: Request,
    email_id=Depends(auth.verify_access_token),
):
    # Access the validated user data
    user = helper.get_user(email_id=email_id)

    logger.info("Dashboard Returned")

    return JSONResponse(
        content={
            "profile": {
                "email_id": user["email_id"],
                "full_name": user["full_name"],
            }
        },
        status_code=status.HTTP_200_OK,
        headers={},
    )


# ==============================================================================
