from pydantic import BaseModel
from typing import Dict
from decimal import Decimal

from .standard import *


# =================================================================================
# TICK


class GROWW_TICK_LTP(BaseModel):
    tsLocal: Decimal
    tsInMillis: Decimal
    ltp: Decimal
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    value: Decimal
    bidQty: Decimal
    offerQty: Decimal
    avgPrice: Decimal
    highPriceRange: Decimal
    lowPriceRange: Decimal
    openInterest: Decimal
    lowTradeRange: Decimal
    highTradeRange: Decimal


class GROWW_TICK_MARKET_DEPTH_ITEM(BaseModel):
    price: Decimal
    qty: Decimal


class GROWW_TICK_MARKET_DEPTH(BaseModel):
    tsLocal: Decimal
    tsInMillis: Decimal
    buyBook: Dict[str, GROWW_TICK_MARKET_DEPTH_ITEM]
    sellBook: Dict[str, GROWW_TICK_MARKET_DEPTH_ITEM]


class GROWW_TICK_INDEX_VALUE(BaseModel):
    tsLocal: Decimal
    tsInMillis: Decimal
    value: Decimal


# =================================================================================s
# CANDLESTICK

# =================================================================================
# INSTRUMENT


class GROWW_INSTRUMENT(BaseModel):
    exchange: str
    exchange_token: str
    trading_symbol: str
    groww_symbol: str
    name: str | None
    instrument_type: str
    segment: str
    series: str | None
    isin: str | None
    underlying_symbol: str | None
    underlying_exchange_token: str | None
    expiry_date: str | None
    strike_price: Decimal | None
    lot_size: int | None
    tick_size: Decimal | None
    freeze_quantity: int | None
    is_reserved: bool | None
    buy_allowed: bool
    sell_allowed: bool
    internal_trading_symbol: str | None
    is_intraday: bool
    exchange_trading_symbol: str


# =================================================================================
# HOLDING


class GROWW_HOLDING(BaseModel):
    isin: str
    trading_symbol: str
    quantity: Decimal
    average_price: Decimal
    pledge_quantity: Decimal
    demat_locked_quantity: Decimal
    groww_locked_quantity: Decimal
    repledge_quantity: Decimal
    t1_quantity: Decimal
    demat_free_quantity: Decimal
    corporate_action_additional_quantity: Decimal
    active_demat_transfer_quantity: Decimal


# =================================================================================
# POSITION


class GROWW_POSITION(BaseModel):
    trading_symbol: str
    segment: str
    credit_quantity: Decimal
    credit_price: Decimal
    debit_quantity: Decimal
    debit_price: Decimal
    carry_forward_credit_quantity: Decimal
    carry_forward_credit_price: Decimal
    carry_forward_debit_quantity: Decimal
    carry_forward_debit_price: Decimal
    exchange: str
    symbol_isin: str
    quantity: Decimal
    product: str
    net_carry_forward_quantity: Decimal
    net_price: Decimal
    net_carry_forward_price: Decimal


# =================================================================================
# ORDER


class GROWW_ORDER_STATUS(BaseModel):
    groww_order_id: str
    order_status: str
    remark: str
    filled_quantity: int
    order_reference_id: str


class GROWW_ORDER_DETAIL(BaseModel):
    groww_order_id: str
    trading_symbol: str
    order_status: str
    remark: str
    quantity: int
    price: Decimal | None
    trigger_price: Decimal | None
    filled_quantity: int
    remaining_quantity: int
    average_fill_price: Decimal | None
    deliverable_quantity: int
    amo_status: str
    validity: str
    exchange: str
    order_type: str
    transaction_type: str
    segment: str
    product: str
    created_at: str
    exchange_time: str | None
    trade_date: str | None
    order_reference_id: str


class GROWW_ORDER_DETAIL_TRADE(BaseModel):
    price: Decimal
    isin: str
    quantity: int
    groww_order_id: str
    groww_trade_id: str
    exchange_trade_id: str
    exchange_order_id: str
    trade_status: str
    trading_symbol: str
    remark: str
    exchange: str
    segment: str
    product: str
    transaction_type: str
    created_at: str
    trade_date_time: str
    settlement_number: str


# =================================================================================
# MARGIN


class GROWW_MARGIN_AVAILABLE_FNO(BaseModel):
    net_fno_margin_used: Decimal
    span_margin_used: Decimal
    exposure_margin_used: Decimal
    future_balance_available: Decimal
    option_buy_balance_available: Decimal
    option_sell_balance_available: Decimal


class GROWW_MARGIN_AVAILABLE_EQUITY(BaseModel):
    net_equity_margin_used: Decimal
    cnc_margin_used: Decimal
    mis_margin_used: Decimal
    cnc_balance_available: Decimal
    mis_balance_available: Decimal


class GROWW_MARGIN_AVAILABLE(BaseModel):
    clear_cash: Decimal
    net_margin_used: Decimal
    brokerage_and_charges: Decimal
    collateral_used: Decimal
    collateral_available: Decimal
    adhoc_margin: Decimal
    fno_margin_details: GROWW_MARGIN_AVAILABLE_FNO
    equity_margin_details: GROWW_MARGIN_AVAILABLE_EQUITY


class GROWW_MARGIN_REQUIRED(BaseModel):
    exposure_required: Decimal
    span_required: Decimal
    option_buy_premium: Decimal
    brokerage_and_charges: Decimal
    total_requirement: Decimal
    cash_cnc_margin_required: Decimal
    physical_delivery_margin_requirement: Decimal


class GROWW_MARGIN_REQUIRED_ORDER(BaseModel):
    trading_symbol: str
    transaction_type: str
    quantity: int
    # Optional: Price (include for limit orders; omit or adjust if not applicable).
    price: Decimal | None
    order_type: str
    product: str
    exchange: str


# =================================================================================
# QUOTE


class GROWW_QUOTE_MARKET_DEPTH_ITEM(BaseModel):
    price: Decimal
    quantity: int


class GROWW_QUOTE_MARKET_DEPTH(BaseModel):
    buy: list[GROWW_QUOTE_MARKET_DEPTH_ITEM]
    sell: list[GROWW_QUOTE_MARKET_DEPTH_ITEM]


class GROWW_QUOTE(BaseModel):
    average_price: Decimal | None
    bid_quantity: int | None
    bid_price: Decimal | None
    day_change: Decimal | None
    day_change_perc: Decimal | None
    upper_circuit_limit: Decimal | None
    lower_circuit_limit: Decimal | None
    ohlc: STANDARD_OHLC
    depth: GROWW_QUOTE_MARKET_DEPTH
    high_trade_range: Decimal | None
    implied_volatility: Decimal | None
    last_trade_quantity: int | None
    last_trade_time: int | None
    low_trade_range: Decimal | None
    last_price: Decimal | None
    market_cap: Decimal | None
    offer_price: Decimal | None
    offer_quantity: int | None
    oi_day_change: Decimal | None
    oi_day_change_percentage: Decimal | None
    open_interest: Decimal | None
    previous_open_interest: Decimal | None
    total_buy_quantity: int | None
    total_sell_quantity: int | None
    volume: int | None
    week_52_high: Decimal | None
    week_52_low: Decimal | None
