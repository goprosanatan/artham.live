#

from datetime import datetime, timedelta, timezone
from decouple import config
from growwapi import GrowwAPI, GrowwFeed
import pyotp
import os
from pydantic import BaseModel

from library import models


class GROWW_WEBSOCKET:

    def __init__(self):

        # ------------------------------------------------------------------
        # connect with api key, api secret and totp (new method)
        access_token = GrowwAPI.get_access_token(
            api_key=config("GROWW_API_KEY", cast=str),
            totp=pyotp.TOTP(config("GROWW_API_SECRET", cast=str)).now(),
        )
        self.api = GrowwAPI(access_token)

        try:
            self.websocket = GrowwFeed(groww_api=self.api)
            print("Websocket Initialized!")
        except Exception as e:
            self.websocket = None
            print("Websocket ERROR:", e)

        self.test_instrument_equity = [
            {"exchange": "NSE", "segment": "CASH", "exchange_token": "2885"},
            {"exchange": "NSE", "segment": "CASH", "exchange_token": "3812"},
        ]
        self.test_instrument_indices = [
            {"exchange": "NSE", "segment": "CASH", "exchange_token": "NIFTY"},
            {"exchange": "BSE", "segment": "CASH", "exchange_token": "1"},
        ]

    def arrange_feed(self, feed: dict):

        ts = datetime.now().timestamp() * 1000

        tick_data = {}
        order_data = {}

        for key in feed.keys():

            if key in ["ltp", "market_depth", "index_value"]:

                for exchange in feed[key].keys():
                    for segment in feed[key][exchange].keys():
                        for exchange_token in feed[key][exchange][segment].keys():

                            tick = feed[key][exchange][segment][exchange_token]
                            exchange_id = f"{exchange}:{segment}:{exchange_token}"

                            # skip if no update
                            if tick is not None:

                                tick["tsLocal"] = ts

                                # create a distinguish key for the combined tick data
                                if exchange_id not in tick_data:
                                    tick_data[exchange_id] = []

                                if key == "ltp":
                                    tick_data[exchange_id].append(
                                        models.GROWW_TICK_LTP(**tick)
                                    )
                                elif key == "market_depth":
                                    tick_data[exchange_id].append(
                                        models.GROWW_TICK_MARKET_DEPTH(**tick)
                                    )

                                elif key == "index_value":
                                    tick_data[exchange_id].append(
                                        models.GROWW_TICK_INDEX_VALUE(**tick)
                                    )

            elif key in ["order_updates", "position_updates"]:
                order_data[key] = {}

                for segment in feed[key].keys():
                    # skip if no update
                    if feed[key][segment] is not None:
                        order_data[key][segment] = feed[key][segment]

            else:
                raise Exception

        return (tick_data, order_data)


# groww_api = GROWW_API()
# instrument = groww_api.get_instrument_by_exchange_and_trading_symbol("NSE", "ZEEL")


# ws = GROWW_WEBSOCKET()


# def on_data_received(meta):

#     all_feed = ws.websocket.get_all_feed()
#     tick_data, order_data = ws.arrange_feed(feed=all_feed)
#     ticks = FROM_GROWW().ticks(data=tick_data)


# ws.websocket.subscribe_ltp(ws.test_instrument_equity, on_data_received=on_data_received)

# ws.websocket.unsubscribe_ltp(ws.test_instrument_equity)


# ws.websocket.subscribe_market_depth(
#     ws.test_instrument_equity, on_data_received=on_data_received
# )

# ws.websocket.unsubscribe_market_depth(ws.test_instrument_equity)


# ws.websocket.subscribe_index_value(
#     ws.test_instrument_indices, on_data_received=on_data_received
# )


# ws.websocket.unsubscribe_index_value(ws.test_instrument_indices)


# ws.websocket.subscribe_fno_position_updates(on_data_received=on_data_received)


# ws.websocket.subscribe_fno_order_updates(on_data_received=on_data_received)


# ws.websocket.subscribe_equity_order_updates(on_data_received=on_data_received)

# # ws.websocket.consume()
