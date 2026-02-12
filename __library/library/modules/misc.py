# ===================================================================================================
# MISCELLANEOUS - HELPER
# ===================================================================================================

import os
import logging
import psycopg
from psycopg.rows import dict_row
from decouple import config
import redis
import shutil
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import psycopg
from psycopg.rows import class_row, dict_row
from psycopg.sql import SQL, Identifier
from typing import Type, TypeVar, get_type_hints, Union
from pydantic import BaseModel
from dataclasses import asdict, is_dataclass
from decimal import Decimal

logger = logging.getLogger(__name__)

T = TypeVar("T")

# ===================================================================================================
# GENERAL PURPOSE UTILITY FUNCTIONS


def mkdir(filepath):

    # create directories if they do not exist
    if not os.path.exists(os.path.dirname(filepath)):
        os.makedirs(os.path.dirname(filepath))


# --------------------------------------------------------------------------------------------------


def df_to_dict(df, key_column, value_column):
    return dict(zip(df[key_column], df[value_column]))


# --------------------------------------------------------------------------------------------------


def to_list(item):

    # To convert any item to list
    if type(item) != list:
        list_item = []
        list_item.append(item)
    else:
        list_item = item

    return list_item


# --------------------------------------------------------------------------------------------------


def to_unique_list(list_not_unique):

    list_unique = np.unique(np.array(list_not_unique)).tolist()
    list_unique.sort()

    return list_unique


# --------------------------------------------------------------------------------------------------


def create_zip_archive(source, destination):
    base = os.path.basename(destination)
    name = base.split(".")[0]
    format = base.split(".")[1]
    archive_from = os.path.dirname(source)
    archive_to = os.path.basename(source.strip(os.sep))
    # shutil.make_archive(name, format, archive_from, archive_to)
    shutil.make_archive(
        base_name=name,
        format=format,
        root_dir=archive_from,
        base_dir=archive_to,
    )
    # shutil.move("%s.%s" % (name, format), destination)


# --------------------------------------------------------------------------------------------------


def get_directory_content_list(directory_path, directory_only, full_path=False):

    if directory_only:

        if full_path:
            list_all = [
                os.path.join(directory_path, file)
                for file in os.listdir(directory_path)
                if os.path.isdir(os.path.join(directory_path, file))
            ]

        else:
            list_all = [
                file
                for file in os.listdir(directory_path)
                if os.path.isdir(os.path.join(directory_path, file))
            ]

    else:

        if full_path:
            list_all = [
                os.path.join(directory_path, file)
                for file in os.listdir(directory_path)
                if os.path.isfile(os.path.join(directory_path, file))
            ]

        else:
            list_all = [
                file
                for file in os.listdir(directory_path)
                if os.path.isfile(os.path.join(directory_path, file))
            ]

    return list_all


# --------------------------------------------------------------------------------------------------


# def pydantic_to_dataclass(
#     pydantic_obj: BaseModel,
#     dataclass_type: Type[TypeVar("T")],
# ) -> TypeVar("T"):
#     """
#     Converts a Pydantic model to a dataclass instance recursively.
#     """
#     if not hasattr(pydantic_obj, "model_dump"):
#         raise TypeError("Object must be a Pydantic model")

#     data = pydantic_obj.model_dump()
#     return dataclass_type(**data)


# # --------------------------------------------------------------------------------------------------


# def dataclass_to_pydantic(
#     dataclass_obj,
#     pydantic_type: Type[BaseModel],
# ) -> BaseModel:
#     """
#     Converts a dataclass instance to a Pydantic model recursively.
#     """
#     if not is_dataclass(dataclass_obj):
#         raise TypeError("Object must be a dataclass")

#     return pydantic_type(**asdict(dataclass_obj))


def pydantic_to_dataclass(pydantic_obj: BaseModel, dataclass_type: Type[T]) -> T:
    """
    Recursively converts a Pydantic model (and any nested models) to a dataclass.
    """
    if not hasattr(pydantic_obj, "model_dump"):
        raise TypeError("Object must be a Pydantic model")

    data = {}
    for field, value in pydantic_obj.model_dump().items():
        if isinstance(value, BaseModel):
            # Recursively convert nested Pydantic model
            target_cls = get_type_hints(dataclass_type).get(field)
            data[field] = pydantic_to_dataclass(value, target_cls)
        elif isinstance(value, list):
            # Convert list elements recursively
            data[field] = [
                pydantic_to_dataclass(v, get_type_hints(dataclass_type).get(field)) if isinstance(v, BaseModel) else v
                for v in value
            ]
        elif isinstance(value, dict):
            # Convert dict values recursively
            field_type = get_type_hints(dataclass_type).get(field)
            sub_type = None
            if hasattr(field_type, "__args__") and len(field_type.__args__) == 2:
                # Dict[str, SubType]
                sub_type = field_type.__args__[1]
            data[field] = {
                k: pydantic_to_dataclass(v, sub_type) if isinstance(v, BaseModel) else v
                for k, v in value.items()
            }
        else:
            data[field] = value
    return dataclass_type(**data)


def dataclass_to_pydantic(dc_obj, pydantic_type: Type[BaseModel]) -> BaseModel:
    """
    Recursively converts a dataclass (and nested dataclasses) into a Pydantic model.
    """
    if not is_dataclass(dc_obj):
        raise TypeError("Object must be a dataclass")

    data = {}
    for field, value in asdict(dc_obj).items():
        if is_dataclass(value):
            # Recursively convert nested dataclass
            target_cls = get_type_hints(pydantic_type).get(field)
            data[field] = dataclass_to_pydantic(value, target_cls)
        elif isinstance(value, list):
            data[field] = [
                dataclass_to_pydantic(v, get_type_hints(pydantic_type).get(field))
                if is_dataclass(v) else v for v in value
            ]
        elif isinstance(value, dict):
            # Handle dicts of dataclasses
            field_type = get_type_hints(pydantic_type).get(field)
            sub_type = None
            if hasattr(field_type, "__args__") and len(field_type.__args__) == 2:
                sub_type = field_type.__args__[1]
            data[field] = {
                k: dataclass_to_pydantic(v, sub_type) if is_dataclass(v) else v
                for k, v in value.items()
            }
        else:
            data[field] = value
    return pydantic_type(**data)

# ===================================================================================================
# POSTGRES

# check if bars are correctly ascendingly timed
def track_df_datetime_order(df, column_name):
    error = 0
    for index, row in df.iterrows():
        try:
            if df[column_name].iloc[index] >= df[column_name].iloc[index + 1]:
                error += 1

        except:
            pass

    if error > 0:
        print("DATETIME Order ERRORS == " + str(error))


# --------------------------------------------------------------------------------------------------
