from datetime import datetime, timedelta, timezone
from decouple import config
from growwapi import GrowwAPI
import pyotp
from decimal import Decimal
import pandas as pd
import numpy as np
from typing import Optional, Any
import math
import pytz

from library import models


class GROWW_API:

    def __init__(self):

        # ------------------------------------------------------------------
        # connect with api key, api secret and totp (new method)
        access_token = GrowwAPI.get_access_token(
            api_key=config("GROWW_API_KEY", cast=str),
            totp=pyotp.TOTP(config("GROWW_API_SECRET", cast=str)).now(),
        )
        self.api = GrowwAPI(access_token)

        self.timeout = 10  # default timeout for api calls
        print("GROWW_API initialized")

        # Define ranges as tuples: (min_interval, max_interval, max_duration, historical_duration)
        self.INTERVAL_RANGES = [
            (1, 4, timedelta(days=7), timedelta(days=90)),  # 1–4 min
            (5, 9, timedelta(days=15), timedelta(days=90)),  # 5–9 min
            (10, 59, timedelta(days=30), timedelta(days=90)),  # 10–59 min
            (60, 239, timedelta(days=150), timedelta(days=90)),  # 1–3 hr
            (240, 1439, timedelta(days=365), timedelta(days=90)),  # 4 hr – <1 day
            (1440, 10079, timedelta(days=1080), timedelta(days=12000)),  # 1 day–<1 week
            (10080, float("inf"), None, timedelta(days=12000)),  # ≥1 week
        ]

    def get_instrument_all(self):
        df = self.api.get_all_instruments()

        # replace NaN with None
        df_converted = df.replace({np.nan: None})

        # combine exchange and trading_symbol to create exchange_trading_symbol
        df_converted["exchange_trading_symbol"] = (
            df_converted["exchange"] + "_" + df_converted["trading_symbol"]
        )

        #  convert to pydantic model
        list_instrument = df_converted.to_dict(orient="records")
        list_instrument = [models.GROWW_INSTRUMENT(**item) for item in list_instrument]

        return list_instrument

    def replace_dict_nan_to_none(self, dict_item: dict):

        for key, value in dict_item.items():
            if isinstance(value, float) and (
                math.isnan(value) or np.isnan(value)
            ):  # Check for both types of NaN
                dict_item[key] = None

        return dict_item

    def get_instrument_by_groww_symbol(self, groww_symbol: str):

        instrument = self.api.get_instrument_by_groww_symbol(groww_symbol=groww_symbol)

        # combine exchange and trading_symbol to create exchange_trading_symbol
        instrument["exchange_trading_symbol"] = (
            instrument["exchange"] + "_" + instrument["trading_symbol"]
        )

        instrument = self.replace_dict_nan_to_none(instrument)
        instrument = models.GROWW_INSTRUMENT(**instrument)

        return instrument

    def get_instrument_by_exchange_and_trading_symbol(
        self, exchange: str, trading_symbol: str
    ):

        instrument = self.api.get_instrument_by_exchange_and_trading_symbol(
            exchange=exchange,
            trading_symbol=trading_symbol,
        )

        # combine exchange and trading_symbol to create exchange_trading_symbol
        instrument["exchange_trading_symbol"] = (
            instrument["exchange"] + "_" + instrument["trading_symbol"]
        )

        instrument = self.replace_dict_nan_to_none(instrument)
        instrument = models.GROWW_INSTRUMENT(**instrument)

        return instrument

    def get_instrument_by_exchange_token(self, exchange_token: str):

        instrument = self.api.get_instrument_by_exchange_token(
            exchange_token=exchange_token
        )

        # combine exchange and trading_symbol to create exchange_trading_symbol
        instrument["exchange_trading_symbol"] = (
            instrument["exchange"] + "_" + instrument["trading_symbol"]
        )

        instrument = self.replace_dict_nan_to_none(instrument)
        instrument = models.GROWW_INSTRUMENT(**instrument)

        return instrument

    def _convert_datetime_to_string(self, dt: datetime):
        # Convert datetime to string in the format "yyyy-MM-dd HH:mm:ss"
        return dt.strftime("%Y-%m-%d %H:%M:%S")

    def _convert_timestamp_to_datetime(self, timestamp):

        datetime_tz = datetime.fromtimestamp(
            timestamp, tz=pytz.timezone("Asia/Kolkata")
        )

        return datetime_tz

    def get_historical(
        self,
        exchange: str,
        segment: str,
        trading_symbol: str,
        start_time: datetime,
        end_time: datetime,
        interval_in_minutes: int = 1440,  # default 1 day (1440 minutes)
    ):

        response = self.api.get_historical_candle_data(
            trading_symbol=trading_symbol,
            exchange=exchange,
            segment=segment,
            start_time=self._convert_datetime_to_string(start_time),
            end_time=self._convert_datetime_to_string(end_time),
            interval_in_minutes=interval_in_minutes,  # default for api is 1 minute
            timeout=self.timeout,
        )

        response_candlestick = response.get("candles", [])

        data_candlestick = []
        for item in response_candlestick:

            candlestick = {
                "date": self._convert_timestamp_to_datetime(item[0]),
                "open": item[1],
                "high": item[2],
                "low": item[3],
                "close": item[4],
                "volume": item[5],
            }

            try:
                candlestick = models.STANDARD_CANDLESTICK(**candlestick)
                data_candlestick.append(candlestick)
            except:
                print("ERROR in candlestick", instrument, candlestick)

        return data_candlestick

    def _get_historical_limits_for_interval(self, interval: int):
        """Finds applicable max and historical duration for a given interval."""
        for min_i, max_i, max_dur, hist_dur in self.INTERVAL_RANGES:
            if min_i <= interval <= max_i:
                return max_dur, hist_dur
        # fallback if none matched
        return timedelta(days=7), timedelta(days=90)

    def get_historical_max(
        self,
        exchange: str,
        segment: str,
        trading_symbol: str,
        end_time: datetime,
        interval_in_minutes: int = 1440,
    ):
        """
        Fetch maximum historical data possible for a given interval range.
        Automatically determines per-request duration and full available duration.
        """
        max_duration, available_duration = self._get_historical_limits_for_interval(
            interval_in_minutes
        )

        data_candlestick = []
        current_end = end_time
        start_limit = end_time - available_duration

        while current_end > start_limit:
            current_start = current_end - max_duration if max_duration else start_limit

            if current_start < start_limit:
                current_start = start_limit

            chunk_data = self.get_historical(
                exchange=exchange,
                segment=segment,
                trading_symbol=trading_symbol,
                start_time=current_start,
                end_time=current_end,
                interval_in_minutes=interval_in_minutes,
            )

            print(
                f"Fetching: {current_start.strftime('%Y-%m-%d')} → {current_end.strftime('%Y-%m-%d')} === {len(chunk_data)} candlesticks"
            )

            data_candlestick = chunk_data + data_candlestick  # prepend older data

            if not max_duration or current_start == start_limit:
                break

            current_end = current_start - timedelta(seconds=1)

        return data_candlestick

    def get_holdings(self):

        response = self.api.get_holdings_for_user(
            timeout=self.timeout,
        )

        holdings = response.get("holdings", {})

        list_holdings = []
        for i in holdings:
            item = self.replace_dict_nan_to_none(i)

            # Specific to GROWW response because it returns one item with trading_symbol as None
            if item.get("trading_symbol") is None:
                continue

            list_holdings.append(models.GROWW_HOLDING(**item))

        return list_holdings

    def get_positions(self, segment: str | None = None):

        response = self.api.get_positions_for_user(
            segment=segment,
            timeout=self.timeout,
        )

        list_positions = response.get("positions", [])
        list_positions = [
            models.GROWW_POSITION(**self.replace_dict_nan_to_none(item))
            for item in list_positions
        ]

        return list_positions

    def get_position_for_instrument(self, segment: str, trading_symbol: str):

        response = self.api.get_position_for_trading_symbol(
            trading_symbol=trading_symbol,
            segment=segment,
            timeout=self.timeout,
        )

        list_positions = response.get("positions", [])
        list_positions = [
            models.GROWW_POSITION(**self.replace_dict_nan_to_none(item))
            for item in list_positions
        ]

        return list_positions

    def get_ltp(self, exchange: str, segment: str, trading_symbol: str):

        exchange_trading_symbol = f"{exchange}_{trading_symbol}"

        response = self.api.get_ltp(
            exchange_trading_symbols=exchange_trading_symbol,
            segment=segment,
            timeout=self.timeout,
        )

        ltp = response.get(exchange_trading_symbol)

        return ltp

    def get_ohlc(self, exchange: str, segment: str, trading_symbol: str):

        exchange_trading_symbol = f"{exchange}_{trading_symbol}"

        response = self.api.get_ohlc(
            exchange_trading_symbols=exchange_trading_symbol,
            segment=segment,
            timeout=self.timeout,
        )

        ohlc = response.get(exchange_trading_symbol)

        return models.STANDARD_OHLC(**ohlc)

    def get_quote(self, exchange: str, segment: str, trading_symbol: str):

        response = self.api.get_quote(
            trading_symbol=trading_symbol,
            exchange=exchange,
            segment=segment,
            timeout=self.timeout,
        )

        quote = models.GROWW_QUOTE(**response)

        return quote

    def get_available_margin(self):

        response = self.api.get_available_margin_details(
            timeout=self.timeout,
        )

        available_margin = models.GROWW_MARGIN_AVAILABLE(**response)

        return available_margin

    def get_required_margin(
        self,
        segment: str,
        order: models.GROWW_MARGIN_REQUIRED_ORDER,
    ):

        order_dict = order.model_dump()

        response = self.api.get_order_margin_details(
            segment=segment,
            orders=[order_dict],
            timeout=self.timeout,
        )

        required_margin = models.GROWW_MARGIN_REQUIRED(**response)

        return required_margin

    def place_order(
        self,
        exchange: str,
        segment: str,
        trading_symbol: str,
        validity: str,
        order_type: str,
        product: str,
        quantity: int,
        transaction_type: str,
        order_reference_id: str | None = None,
        price: float | None = None,
        trigger_price: float | None = None,
    ):

        response = self.api.place_order(
            validity=validity,
            exchange=exchange,
            segment=segment,
            trading_symbol=trading_symbol,
            order_type=order_type,
            product=product,
            quantity=quantity,
            transaction_type=transaction_type,
            order_reference_id=order_reference_id,  # Optional
            price=price,  # Optional
            trigger_price=trigger_price,  # Optional
            timeout=self.timeout,
        )

        return response

    def modify_order(
        self,
        order_type: str,
        segment: str,
        groww_order_id: str,
        quantity: int,
        price: float | None = None,
        trigger_price: float | None = None,
    ):

        response = self.api.modify_order(
            order_type=order_type,
            segment=segment,
            groww_order_id=groww_order_id,
            quantity=quantity,
            price=price,  # Optional
            trigger_price=trigger_price,  # Optional
            timeout=self.timeout,
        )

        return response

    def cancel_order(
        self,
        groww_order_id: str,
        segment: str,
    ):

        response = self.api.cancel_order(
            groww_order_id=groww_order_id,
            segment=segment,
            timeout=self.timeout,
        )

        return response

    def get_trade_list_for_order(
        self,
        segment: str,
        groww_order_id: str,
    ):

        page = 0
        page_size = 50

        # repeat get_order_list until all pages are fetched
        list_trades = []
        while True:

            response = self.api.get_trade_list_for_order(
                groww_order_id=groww_order_id,
                segment=segment,
                page=page,
                page_size=page_size,
                timeout=self.timeout,
            )

            trades = response.get("trade_list", [])

            if len(trades) == 0:
                break  # exit loop if last page is reached

            list_trades.extend(trades)

            page += 1  # increment page number for next iteration

        all_trades = []

        for trade in list_trades:
            all_trades.append(models.GROWW_ORDER_DETAIL_TRADE(**trade))

        return response.get("trade_list")

    def get_order_status_by_order_id(
        self,
        segment: str,
        groww_order_id: str,
    ):

        response = self.api.get_order_status(
            segment=segment,
            groww_order_id=groww_order_id,
            timeout=self.timeout,
        )

        order_status = models.GROWW_ORDER_STATUS(**response)

        return order_status

    def get_order_status_by_reference_id(
        self,
        segment: str,
        order_reference_id: str,
    ):

        response = self.api.get_order_status_by_reference(
            segment=segment,
            order_reference_id=order_reference_id,
            timeout=self.timeout,
        )

        order_status = models.GROWW_ORDER_STATUS(**response)

        return order_status

    def get_orders_all(
        self,
        segment: str | None = None,
    ):

        page = 0
        page_size = 100

        # repeat get_order_list until all pages are fetched
        list_orders = []
        while True:
            response = self.api.get_order_list(
                segment=segment,
                page=page,
                page_size=page_size,
                timeout=self.timeout,
            )

            orders = response.get("order_list", [])

            if len(orders) == 0:
                break  # exit loop if last page is reached

            list_orders.extend(orders)

            page += 1  # increment page number for next iteration

        all_orders = []

        for order in list_orders:
            all_orders.append(models.GROWW_ORDER_DETAIL(**order))

        return all_orders

    def get_order_detail(self, segment: str, groww_order_id: str):

        response = self.api.get_order_detail(
            segment=segment,
            groww_order_id=groww_order_id,
            timeout=self.timeout,
        )

        order_detail = models.GROWW_ORDER_DETAIL(**response)

        return order_detail
