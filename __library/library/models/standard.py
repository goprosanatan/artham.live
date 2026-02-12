from pydantic import BaseModel, Field
from typing import Dict, Optional, Literal
from decimal import Decimal
from datetime import date, datetime


# =================================================================================
# TICK


class STANDARD_TICK_MD5(BaseModel):
    instrument_id: int = Field(
        ..., json_schema_extra={"psql_data_type": "BIGINT NOT NULL"}
    )
    instrument_type: str = Field(
        ..., json_schema_extra={"psql_data_type": "TEXT NOT NULL"}
    )

    exchange_ts: datetime = Field(
        ..., json_schema_extra={"psql_data_type": "TIMESTAMPTZ NOT NULL"}
    )
    ingest_ts: datetime = Field(
        ..., json_schema_extra={"psql_data_type": "TIMESTAMPTZ NOT NULL"}
    )
    db_ts: datetime = Field(
        ...,
        json_schema_extra={"psql_data_type": "TIMESTAMPTZ NOT NULL"},
    )
    last_trade_time: datetime = Field(
        ..., json_schema_extra={"psql_data_type": "TIMESTAMPTZ NOT NULL"}
    )

    tradable: bool = Field(
        ..., json_schema_extra={"psql_data_type": "BOOLEAN NOT NULL"}
    )

    last_price: Decimal = Field(
        ..., json_schema_extra={"psql_data_type": "NUMERIC(12,2) NOT NULL"}
    )
    last_traded_quantity: Optional[int] = Field(
        None, json_schema_extra={"psql_data_type": "INTEGER"}
    )
    average_traded_price: Optional[Decimal] = Field(
        None, json_schema_extra={"psql_data_type": "NUMERIC(12,2)"}
    )
    volume_traded: Optional[int] = Field(
        None, json_schema_extra={"psql_data_type": "BIGINT"}
    )
    total_buy_quantity: Optional[int] = Field(
        None, json_schema_extra={"psql_data_type": "BIGINT"}
    )
    total_sell_quantity: Optional[int] = Field(
        None, json_schema_extra={"psql_data_type": "BIGINT"}
    )

    ohlc_open: Optional[Decimal] = Field(
        None, json_schema_extra={"psql_data_type": "NUMERIC(10,2)"}
    )
    ohlc_high: Optional[Decimal] = Field(
        None, json_schema_extra={"psql_data_type": "NUMERIC(10,2)"}
    )
    ohlc_low: Optional[Decimal] = Field(
        None, json_schema_extra={"psql_data_type": "NUMERIC(10,2)"}
    )
    ohlc_close: Optional[Decimal] = Field(
        None, json_schema_extra={"psql_data_type": "NUMERIC(10,2)"}
    )
    change: Optional[Decimal] = Field(
        None, json_schema_extra={"psql_data_type": "NUMERIC(8,4)"}
    )

    oi: Optional[int] = Field(None, json_schema_extra={"psql_data_type": "BIGINT"})
    oi_day_high: Optional[int] = Field(
        None, json_schema_extra={"psql_data_type": "BIGINT"}
    )
    oi_day_low: Optional[int] = Field(
        None, json_schema_extra={"psql_data_type": "BIGINT"}
    )

    # Depth - Buy side (5 levels)
    depth_buy_0_quantity: Optional[int] = Field(
        None, json_schema_extra={"psql_data_type": "NUMERIC(12,0)"}
    )
    depth_buy_0_price: Optional[Decimal] = Field(
        None, json_schema_extra={"psql_data_type": "NUMERIC(12,2)"}
    )
    depth_buy_0_orders: Optional[int] = Field(
        None, json_schema_extra={"psql_data_type": "NUMERIC(12,0)"}
    )

    depth_buy_1_quantity: Optional[int] = Field(
        None, json_schema_extra={"psql_data_type": "NUMERIC(12,0)"}
    )
    depth_buy_1_price: Optional[Decimal] = Field(
        None, json_schema_extra={"psql_data_type": "NUMERIC(12,2)"}
    )
    depth_buy_1_orders: Optional[int] = Field(
        None, json_schema_extra={"psql_data_type": "NUMERIC(12,0)"}
    )

    depth_buy_2_quantity: Optional[int] = Field(
        None, json_schema_extra={"psql_data_type": "NUMERIC(12,0)"}
    )
    depth_buy_2_price: Optional[Decimal] = Field(
        None, json_schema_extra={"psql_data_type": "NUMERIC(12,2)"}
    )
    depth_buy_2_orders: Optional[int] = Field(
        None, json_schema_extra={"psql_data_type": "NUMERIC(12,0)"}
    )

    depth_buy_3_quantity: Optional[int] = Field(
        None, json_schema_extra={"psql_data_type": "NUMERIC(12,0)"}
    )
    depth_buy_3_price: Optional[Decimal] = Field(
        None, json_schema_extra={"psql_data_type": "NUMERIC(12,2)"}
    )
    depth_buy_3_orders: Optional[int] = Field(
        None, json_schema_extra={"psql_data_type": "NUMERIC(12,0)"}
    )

    depth_buy_4_quantity: Optional[int] = Field(
        None, json_schema_extra={"psql_data_type": "NUMERIC(12,0)"}
    )
    depth_buy_4_price: Optional[Decimal] = Field(
        None, json_schema_extra={"psql_data_type": "NUMERIC(12,2)"}
    )
    depth_buy_4_orders: Optional[int] = Field(
        None, json_schema_extra={"psql_data_type": "NUMERIC(12,0)"}
    )

    # Depth - Sell side (5 levels)
    depth_sell_0_quantity: Optional[int] = Field(
        None, json_schema_extra={"psql_data_type": "NUMERIC(12,0)"}
    )
    depth_sell_0_price: Optional[Decimal] = Field(
        None, json_schema_extra={"psql_data_type": "NUMERIC(12,2)"}
    )
    depth_sell_0_orders: Optional[int] = Field(
        None, json_schema_extra={"psql_data_type": "NUMERIC(12,0)"}
    )

    depth_sell_1_quantity: Optional[int] = Field(
        None, json_schema_extra={"psql_data_type": "NUMERIC(12,0)"}
    )
    depth_sell_1_price: Optional[Decimal] = Field(
        None, json_schema_extra={"psql_data_type": "NUMERIC(12,2)"}
    )
    depth_sell_1_orders: Optional[int] = Field(
        None, json_schema_extra={"psql_data_type": "NUMERIC(12,0)"}
    )

    depth_sell_2_quantity: Optional[int] = Field(
        None, json_schema_extra={"psql_data_type": "NUMERIC(12,0)"}
    )
    depth_sell_2_price: Optional[Decimal] = Field(
        None, json_schema_extra={"psql_data_type": "NUMERIC(12,2)"}
    )
    depth_sell_2_orders: Optional[int] = Field(
        None, json_schema_extra={"psql_data_type": "NUMERIC(12,0)"}
    )

    depth_sell_3_quantity: Optional[int] = Field(
        None, json_schema_extra={"psql_data_type": "NUMERIC(12,0)"}
    )
    depth_sell_3_price: Optional[Decimal] = Field(
        None, json_schema_extra={"psql_data_type": "NUMERIC(12,2)"}
    )
    depth_sell_3_orders: Optional[int] = Field(
        None, json_schema_extra={"psql_data_type": "NUMERIC(12,0)"}
    )

    depth_sell_4_quantity: Optional[int] = Field(
        None, json_schema_extra={"psql_data_type": "NUMERIC(12,0)"}
    )
    depth_sell_4_price: Optional[Decimal] = Field(
        None, json_schema_extra={"psql_data_type": "NUMERIC(12,2)"}
    )
    depth_sell_4_orders: Optional[int] = Field(
        None, json_schema_extra={"psql_data_type": "NUMERIC(12,0)"}
    )

    __pkey__ = []


# =================================================================================s
# CANDLESTICK/BAR


class STANDARD_BAR(BaseModel):
    # bar identity
    instrument_id: int = Field(
        ..., json_schema_extra={"psql_data_type": "BIGINT NOT NULL"}
    )
    timeframe: str = Field(
        ..., json_schema_extra={"psql_data_type": "TEXT NOT NULL"}
    )
    bar_ts: datetime = Field(
        ..., json_schema_extra={"psql_data_type": "TIMESTAMPTZ NOT NULL"}
    )

    # OHLC
    open: float = Field(
        ..., json_schema_extra={"psql_data_type": "NUMERIC(12,2) NOT NULL"}
    )
    high: float = Field(
        ..., json_schema_extra={"psql_data_type": "NUMERIC(12,2) NOT NULL"}
    )
    low: float = Field(
        ..., json_schema_extra={"psql_data_type": "NUMERIC(12,2) NOT NULL"}
    )
    close: float = Field(
        ..., json_schema_extra={"psql_data_type": "NUMERIC(12,2) NOT NULL"}
    )

    # Volume (always required)
    volume: int = Field(..., json_schema_extra={"psql_data_type": "BIGINT NOT NULL"})

    # Derivatives only (nullable)
    oi: Optional[int] = Field(None, json_schema_extra={"psql_data_type": "BIGINT"})
    oi_change: Optional[int] = Field(
        None, json_schema_extra={"psql_data_type": "BIGINT"}
    )

    __pkey__ = ["instrument_id", "timeframe", "bar_ts"]


class STANDARD_CANDLESTICK(BaseModel):
    date: datetime = Field(
        ..., json_schema_extra={"psql_data_type": "TIMESTAMP WITH TIME ZONE"}
    )
    open: Decimal = Field(..., json_schema_extra={"psql_data_type": "NUMERIC(12,2)"})
    high: Decimal = Field(..., json_schema_extra={"psql_data_type": "NUMERIC(12,2)"})
    low: Decimal = Field(..., json_schema_extra={"psql_data_type": "NUMERIC(12,2)"})
    close: Decimal = Field(..., json_schema_extra={"psql_data_type": "NUMERIC(12,2)"})
    volume: int | None = Field(
        ..., json_schema_extra={"psql_data_type": "NUMERIC(15,0)"}
    )

    __pkey__ = ["date"]


class STANDARD_OHLC(BaseModel):
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal


# =================================================================================
# INSTRUMENT


class STANDARD_INSTRUMENT(BaseModel):
    instrument_id: int = Field(
        ...,
        json_schema_extra={"psql_data_type": "BIGINT"},
    )
    exchange: str = Field(..., json_schema_extra={"psql_data_type": "TEXT NOT NULL"})
    segment: str = Field(..., json_schema_extra={"psql_data_type": "TEXT NOT NULL"})
    trading_symbol: str = Field(
        ..., json_schema_extra={"psql_data_type": "TEXT NOT NULL"}
    )
    underlying_instrument_id: int | None = Field(
        ..., json_schema_extra={"psql_data_type": "BIGINT"}
    )
    underlying_trading_symbol: str | None = Field(
        ..., json_schema_extra={"psql_data_type": "TEXT"}
    )
    timezone: str = Field(..., json_schema_extra={"psql_data_type": "TEXT NOT NULL"})
    instrument_type: str = Field(
        ..., json_schema_extra={"psql_data_type": "TEXT NOT NULL"}
    )
    description: str | None = Field(..., json_schema_extra={"psql_data_type": "TEXT"})
    isin: str | None = Field(..., json_schema_extra={"psql_data_type": "TEXT"})
    strike: Decimal | None = Field(
        ..., json_schema_extra={"psql_data_type": "NUMERIC(10,2)"}
    )
    expiry: date | None = Field(..., json_schema_extra={"psql_data_type": "DATE"})
    lot_size: int | None = Field(
        ..., json_schema_extra={"psql_data_type": "NUMERIC(10,0)"}
    )
    tick_size: Decimal | None = Field(
        ..., json_schema_extra={"psql_data_type": "NUMERIC(10,4)"}
    )
    expired: bool = Field(..., json_schema_extra={"psql_data_type": "BOOLEAN NOT NULL"})
    display_order: Decimal | None = Field(
        ..., json_schema_extra={"psql_data_type": "NUMERIC(1,0)"}
    )

    __pkey__ = ["instrument_id"]


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
