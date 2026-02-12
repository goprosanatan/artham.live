# ==============================================================================
# Auth Management


from fastapi import status, Depends, HTTPException
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import jwt
import jwt.exceptions
from decouple import config
from datetime import datetime, timedelta, timezone


# ==============================================================================
# DEFAULTS

JWT_SECRET = config("JWT_SECRET_KEY", cast=str)
JWT_ALGORITHM = config("JWT_ALGORITHM", cast=str)
JWT_PREFIX = config("JWT_PREFIX", cast=str)
JWT_VALIDITY = timedelta(
    days=0,
    minutes=config("JWT_EXPIRATION_MINUTES", cast=int),
    seconds=0,
)


# ==============================================================================
# Functions


def verify_access_token(
    credentials: str | HTTPAuthorizationCredentials = Depends(HTTPBearer()),
):
    try:
        # works for api and websocket both respectively
        try:
            token = credentials.credentials
        except:
            token = credentials.split(" ")[1]

        access_token_payload = jwt.decode(
            jwt=token,
            key=JWT_SECRET,
            algorithms=[JWT_ALGORITHM],
            verify=True,
            options={
                "verify_signature": True,
                "require": ["iat", "nbf", "exp", "email_id"],
                "verify_exp": True,
                "verify_nbf": True,
                "verify_iat": True,
                # 'verify_aud': True,
                # 'verify_iss': True,
            },
        )

        return access_token_payload["email_id"]

    except jwt.exceptions.ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Access token EXPIRED. Log in AGAIN",
            headers={},
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Access token INVALID - " + str(e),
            headers={},
        )


# ------------------------------------------------------


def create_access_token(email_id):
    
    token = jwt.encode(
        payload={
            "iat": datetime.now(timezone.utc),
            "nbf": datetime.now(timezone.utc),
            "exp": datetime.now(timezone.utc) + JWT_VALIDITY,
            "email_id": email_id,
        },
        key=JWT_SECRET,
        algorithm=JWT_ALGORITHM,
    )
    return f"{JWT_PREFIX} {token}"


# ------------------------------------------------------
