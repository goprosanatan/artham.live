# ==============================================================================

from passlib.context import CryptContext
from pydantic import BaseModel
from pydantic_csv import BasemodelCSVReader, BasemodelCSVWriter

# ==============================================================================
# BASICS


class User(BaseModel):
    email_id: str
    full_name: str
    hashed_password: str
    disabled: bool


class LoginForm(BaseModel):
    email_id: str
    password: str


pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


# ==============================================================================
# Functions


def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)


# ------------------------------------------------------


def get_password_hash(password):
    return pwd_context.hash(password)


# ------------------------------------------------------


def get_users_db():
    # using file on disk
    with open("db/users.csv") as csv:
        reader = BasemodelCSVReader(
            file_obj=csv,
            model=User,
            use_alias=True,
            validate_header=True,
        )

        return list(reader)


# ------------------------------------------------------


def update_users_db(users_list: list):

    # using file on disk
    with open("db/users.csv", mode="w") as csv:
        writer = BasemodelCSVWriter(
            file_obj=csv,
            data=users_list,
            model=User,
            use_alias=True,
        )
        writer.write()


# ------------------------------------------------------


def get_user(email_id: str):
    users_list = get_users_db()

    for user in users_list:

        user_dict = dict(user)
        if user_dict["email_id"] == email_id:
            return user_dict
        else:
            None


# ------------------------------------------------------


def authenticate_user(email_id: str, password: str):
    user = get_user(email_id)
    if not user:
        return False
    if not verify_password(password, user["hashed_password"]):
        return False
    return user


# ------------------------------------------------------
