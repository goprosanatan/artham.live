from pydantic import BaseModel
from datetime import date
from typing import Dict, Optional
from decimal import Decimal

from .standard import *


# =================================================================================
# TICK


class KITE_TICK_FULL(BaseModel):
    exchange_timestamp: Optional[datetime] = Field(None, json_schema_extra={"psql_data_type": "TIMESTAMP WITHOUT TIME ZONE"})
    tradable: bool = Field(..., json_schema_extra={"psql_data_type": "BOOLEAN"})
    mode: str = Field(..., json_schema_extra={"psql_data_type": "TEXT"})
    instrument_token: int = Field(..., json_schema_extra={"psql_data_type": "BIGINT"})
    last_price: Decimal = Field(..., json_schema_extra={"psql_data_type": "NUMERIC(12,2)"})
    last_traded_quantity: Optional[int] = Field(None, json_schema_extra={"psql_data_type": "INTEGER"})
    average_traded_price: Optional[Decimal] = Field(None, json_schema_extra={"psql_data_type": "NUMERIC(15,2)"})
    volume_traded: Optional[int] = Field(None, json_schema_extra={"psql_data_type": "BIGINT"})
    total_buy_quantity: Optional[int] = Field(None, json_schema_extra={"psql_data_type": "BIGINT"})
    total_sell_quantity: Optional[int] = Field(None, json_schema_extra={"psql_data_type": "BIGINT"})
    ohlc_open: Optional[Decimal] = Field(None, json_schema_extra={"psql_data_type": "NUMERIC(10,2)"})
    ohlc_high: Optional[Decimal] = Field(None, json_schema_extra={"psql_data_type": "NUMERIC(10,2)"})
    ohlc_low: Optional[Decimal] = Field(None, json_schema_extra={"psql_data_type": "NUMERIC(10,2)"})
    ohlc_close: Optional[Decimal] = Field(None, json_schema_extra={"psql_data_type": "NUMERIC(10,2)"})
    change: Optional[Decimal] = Field(None, json_schema_extra={"psql_data_type": "NUMERIC(10,4)"})
    last_trade_time: Optional[datetime] = Field(None, json_schema_extra={"psql_data_type": "TIMESTAMP"})
    oi: Optional[int] = Field(None, json_schema_extra={"psql_data_type": "BIGINT"})
    oi_day_high: Optional[int] = Field(None, json_schema_extra={"psql_data_type": "BIGINT"})
    oi_day_low: Optional[int] = Field(None, json_schema_extra={"psql_data_type": "BIGINT"})

    # Depth - Buy side (5 levels)
    depth_buy_0_quantity: Optional[int] = Field(None, json_schema_extra={"psql_data_type": "NUMERIC(12,0)"})
    depth_buy_0_price: Optional[Decimal] = Field(None, json_schema_extra={"psql_data_type": "NUMERIC(12,2)"})
    depth_buy_0_orders: Optional[int] = Field(None, json_schema_extra={"psql_data_type": "NUMERIC(12,0)"})
    depth_buy_1_quantity: Optional[int] = Field(None, json_schema_extra={"psql_data_type": "NUMERIC(12,0)"})
    depth_buy_1_price: Optional[Decimal] = Field(None, json_schema_extra={"psql_data_type": "NUMERIC(12,2)"})
    depth_buy_1_orders: Optional[int] = Field(None, json_schema_extra={"psql_data_type": "NUMERIC(12,0)"})
    depth_buy_2_quantity: Optional[int] = Field(None, json_schema_extra={"psql_data_type": "NUMERIC(12,0)"})
    depth_buy_2_price: Optional[Decimal] = Field(None, json_schema_extra={"psql_data_type": "NUMERIC(12,2)"})
    depth_buy_2_orders: Optional[int] = Field(None, json_schema_extra={"psql_data_type": "NUMERIC(12,0)"})
    depth_buy_3_quantity: Optional[int] = Field(None, json_schema_extra={"psql_data_type": "NUMERIC(12,0)"})
    depth_buy_3_price: Optional[Decimal] = Field(None, json_schema_extra={"psql_data_type": "NUMERIC(12,2)"})
    depth_buy_3_orders: Optional[int] = Field(None, json_schema_extra={"psql_data_type": "NUMERIC(12,0)"})
    depth_buy_4_quantity: Optional[int] = Field(None, json_schema_extra={"psql_data_type": "NUMERIC(12,0)"})
    depth_buy_4_price: Optional[Decimal] = Field(None, json_schema_extra={"psql_data_type": "NUMERIC(12,2)"})
    depth_buy_4_orders: Optional[int] = Field(None, json_schema_extra={"psql_data_type": "NUMERIC(12,0)"})

    # Depth - Sell side (5 levels)
    depth_sell_0_quantity: Optional[int] = Field(None, json_schema_extra={"psql_data_type": "NUMERIC(12,0)"})
    depth_sell_0_price: Optional[Decimal] = Field(None, json_schema_extra={"psql_data_type": "NUMERIC(12,2)"})
    depth_sell_0_orders: Optional[int] = Field(None, json_schema_extra={"psql_data_type": "NUMERIC(12,0)"})
    depth_sell_1_quantity: Optional[int] = Field(None, json_schema_extra={"psql_data_type": "NUMERIC(12,0)"})
    depth_sell_1_price: Optional[Decimal] = Field(None, json_schema_extra={"psql_data_type": "NUMERIC(12,2)"})
    depth_sell_1_orders: Optional[int] = Field(None, json_schema_extra={"psql_data_type": "NUMERIC(12,0)"})
    depth_sell_2_quantity: Optional[int] = Field(None, json_schema_extra={"psql_data_type": "NUMERIC(12,0)"})
    depth_sell_2_price: Optional[Decimal] = Field(None, json_schema_extra={"psql_data_type": "NUMERIC(12,2)"})
    depth_sell_2_orders: Optional[int] = Field(None, json_schema_extra={"psql_data_type": "NUMERIC(12,0)"})
    depth_sell_3_quantity: Optional[int] = Field(None, json_schema_extra={"psql_data_type": "NUMERIC(12,0)"})
    depth_sell_3_price: Optional[Decimal] = Field(None, json_schema_extra={"psql_data_type": "NUMERIC(12,2)"})
    depth_sell_3_orders: Optional[int] = Field(None, json_schema_extra={"psql_data_type": "NUMERIC(12,0)"})
    depth_sell_4_quantity: Optional[int] = Field(None, json_schema_extra={"psql_data_type": "NUMERIC(12,0)"})
    depth_sell_4_price: Optional[Decimal] = Field(None, json_schema_extra={"psql_data_type": "NUMERIC(12,2)"})
    depth_sell_4_orders: Optional[int] = Field(None, json_schema_extra={"psql_data_type": "NUMERIC(12,0)"})

    __pkey__ = []

class KITE_TICK_LTP(BaseModel):
    tradable: bool
    mode: str
    instrument_token: int
    last_price: Decimal


class KITE_TICK_QUOTE(BaseModel):
    tradable: bool
    mode: str
    instrument_token: int
    last_price: Decimal
    last_traded_quantity: int
    average_traded_price: Decimal
    volume_traded: int
    total_buy_quantity: int
    total_sell_quantity: int
    ohlc_open: Decimal
    ohlc_high: Decimal
    ohlc_low: Decimal
    ohlc_close: Decimal
    change: Decimal


# =================================================================================s
# CANDLESTICK


# =================================================================================
# SCRIP


class KITE_INSTRUMENT(BaseModel):
    instrument_token: int
    exchange_token: int
    tradingsymbol: str
    name: str | None
    last_price: Decimal | None
    expiry: date | None
    strike: Decimal | None
    tick_size: Decimal
    lot_size: int
    instrument_type: str
    segment: str
    exchange: str


# =================================================================================
# HOLDING


# =================================================================================
# POSITION


# =================================================================================
# ORDER


# =================================================================================
# MARGIN


# =================================================================================
# QUOTE
