# ===================================================================================================
# API for Zerodha Kite - For usage of the API and conversion of API data to GENERIC data
# ===================================================================================================

from decouple import config
import numpy as np
import pandas as pd
import re
from typing import Optional, Any, List, Dict, Literal
from datetime import datetime, timedelta, timezone, date
from kiteconnect import KiteConnect
from kiteconnect import KiteTicker

from library import models

# ===================================================================================================


class KITE_API:

    def __init__(
        self,
        access_token,
    ):
        self.api = KiteConnect(api_key=config("KITE_API_KEY", cast=str))
        self.api.set_access_token(access_token)

    def get_instruments(self) -> List[models.STANDARD_INSTRUMENT]:
        """
        Get transformed instruments master table with normalized columns.

        Returns:
            DataFrame with columns: instrument_id, underlying_instrument_id, exchange,
            segment, trading_symbol, description, instrument_type, strike, expiry,
            lot_size, tick_size
        """
        raw_instruments = self.api.instruments()
        df = self._transform_kite_instruments(raw_instruments)
        list_instruments = df.to_dict(orient="records")
        list_instruments = [
            models.STANDARD_INSTRUMENT(**item) for item in list_instruments
        ]
        return list_instruments

    @staticmethod
    def _format_description(
        name: str,
        tradingsymbol: str,
        instrument_type: str,
        strike: Optional[float] = None,
        expiry: Optional[date] = None,
    ) -> str:
        """Generate human-readable description."""
        if instrument_type not in ("CE", "PE", "FUT"):
            if name and str(name).strip():
                return name
            else:
                return tradingsymbol

        # Format expiry date
        expiry_str = "NA"
        if expiry:
            try:
                if isinstance(expiry, date):
                    dt = expiry
                else:
                    dt = datetime.strptime(expiry, "%Y-%m-%d")
                expiry_str = dt.strftime("%d %b")
            except Exception:
                expiry_str = str(expiry)

        if instrument_type in ("CE", "PE"):
            strike_str = f"{int(strike)}" if strike and strike > 0 else "NA"
            return f"{name} {expiry_str} {strike_str} {instrument_type}"

        if instrument_type == "FUT":
            return f"{name} {expiry_str} FUT"

        return name

    @staticmethod
    def _resolve_underlying_instrument_id(
        underlying_symbol: Optional[str],
        base_instruments_dict: Dict[str, int],
        alias_map: Optional[Dict[str, str]] = None,
    ) -> Optional[int]:
        """Resolve underlying instrument ID."""
        if not underlying_symbol:
            return None

        if underlying_symbol in base_instruments_dict:
            return base_instruments_dict[underlying_symbol]

        if alias_map and underlying_symbol in alias_map:
            aliased = alias_map[underlying_symbol]
            if aliased in base_instruments_dict:
                return base_instruments_dict[aliased]

        return None

    def _transform_kite_instruments(self, kite_instruments: List[Dict]) -> pd.DataFrame:
        """Transform raw Kite instruments into normalized master table."""
        df = pd.DataFrame(kite_instruments)

        # replace NaN with None
        df.replace({np.nan: None}, inplace=True)
        df.replace({"": None}, inplace=True)

        # Create base lookup
        base_instruments = df[
            df["instrument_type"].isin(["EQ", "INDICES", "ETF"])
        ].copy()
        base_lookup = dict(
            zip(base_instruments["tradingsymbol"], base_instruments["instrument_token"])
        )

        # Common aliases for indices
        alias_map = {
            "NIFTY": "NIFTY 50",
            "BANKNIFTY": "NIFTY BANK",
            "FINNIFTY": "NIFTY FIN SERVICE",
            "MIDCPNIFTY": "NIFTY MID SELECT",
            "SENSEX50": "SENSEX",
            "SENSEX": "SENSEX",
        }

        # Initialize new columns
        df["instrument_id"] = df["instrument_token"]
        df["trading_symbol"] = df["tradingsymbol"]
        df["underlying_instrument_id"] = None
        df["underlying_trading_symbol"] = None
        df["description"] = None
        df["timezone"] = "Asia/Kolkata"
        df["expired"] = False
        df["active"] = False
        df["isin"] = None
        df["display_order"] = None

        # Process each instrument
        for idx, row in df.iterrows():
            inst_type = row["instrument_type"].upper()
            exchange = (row.get("exchange") or "").upper()
            trading_symbol = (row.get("tradingsymbol") or "").upper()
            underlying_name = (row.get("name") or "").upper()

            # For derivatives, resolve underlying
            if inst_type in ("CE", "PE", "FUT"):
                underlying_id = self._resolve_underlying_instrument_id(
                    row["name"], base_lookup, alias_map
                )
                df.loc[idx, "underlying_instrument_id"] = underlying_id
                df.loc[idx, "underlying_trading_symbol"] = row["name"]

            # Mark only RELIANCE equity (NSE/BSE) and its derivatives (NFO/BFO) as active.
            is_reliance_equity = (
                inst_type == "EQ"
                and trading_symbol == "RELIANCE"
                and exchange in ("NSE", "BSE")
            )
            is_reliance_derivative = (
                inst_type in ("FUT", "CE", "PE")
                and underlying_name == "RELIANCE"
                and exchange in ("NFO", "BFO")
            )
            df.loc[idx, "active"] = is_reliance_equity or is_reliance_derivative

            # Generate description
            description = self._format_description(
                name=row["name"],
                tradingsymbol=row["tradingsymbol"],
                instrument_type=inst_type,
                strike=row.get("strike"),
                expiry=row.get("expiry"),
            )
            df.loc[idx, "description"] = description

            # Assign ordering: EQ -> 1, FUT -> 2, OPT (CE/PE) -> 3
            order_val = 1
            if inst_type == "EQ":
                order_val = 0
            elif inst_type == "FUT":
                order_val = 2
            elif inst_type in ("CE", "PE"):
                order_val = 3
            else:
                order_val = 1
            df.loc[idx, "display_order"] = order_val

        # Convert underlying_instrument_id to Int64
        df["underlying_instrument_id"] = df["underlying_instrument_id"].astype("Int64")
        df["display_order"] = df["display_order"].astype("Int64")

        # Select and reorder columns
        output_columns = [
            "instrument_id",
            "exchange",
            "segment",
            "trading_symbol",
            "underlying_instrument_id",
            "underlying_trading_symbol",
            "timezone",
            "instrument_type",
            "description",
            "isin",
            "strike",
            "expiry",
            "lot_size",
            "tick_size",
            "expired",
            "active",
            "display_order",
        ]

        return df[output_columns]

    def get_historical(
        self,
        instrument_id: int,
        from_date: datetime,
        to_date: datetime,
        interval: Literal["1m", "1D"],
    ) -> List[models.STANDARD_BAR]:

        # minute max 60 days
        # day max 2000 days

        INTERVAL_TO_TIMEFRAME = {
            "1m": "minute",
            "1D": "day",
        }

        records = self.api.historical_data(
            instrument_token=instrument_id,
            from_date=from_date,
            to_date=to_date,
            interval=INTERVAL_TO_TIMEFRAME.get(interval),
            continuous=False,
            oi=True,
        )

        data = []
        for r in records:
            payload = {**r}
            payload["instrument_id"] = instrument_id
            payload["timeframe"] = interval
            payload["bar_ts"] = r.get("date")
            data.append(models.STANDARD_BAR(**payload))

        return data

    def get_historical_max(
        self,
        instrument_id: int,
        interval: Literal["1m", "1D"],
        last_bar_ts: datetime = datetime(1970, 1, 1),
    ) -> List[models.STANDARD_BAR]:

        # --- API constraints ---
        max_days = 60 if interval == "1m" else 2000
        step = timedelta(days=max_days)

        # --- Start from today last second and go backwards ---
        today = datetime.combine(
            datetime.now().date() + timedelta(days=1), datetime.min.time()
        )

        all_bars: List[models.STANDARD_BAR] = []

        print(f"\n\n🔍 Fetching MAX historical data [{interval}] (latest → oldest)")
        print(f"   Chunk size: {max_days} days\n")
        chunk = 1

        # --- Loop through chunks (reverse: newest to oldest) ---
        current_to = today
        while current_to > last_bar_ts:
            current_from = max(current_to - step, last_bar_ts)

            # Adjust temp_to to be one second before current_to to avoid overlap
            temp_to = current_to - timedelta(seconds=1)

            print(
                f"📅 Chunk {chunk}: {current_from} → {temp_to} ...",
                end=" ",
                flush=True,
            )

            bars = self.get_historical(
                instrument_id=instrument_id,
                from_date=current_from,
                to_date=temp_to,
                interval=interval,
            )

            print(f"✅ Retrieved {len(bars)} bars.")

            if len(bars) == 0:
                print("⏹️  No more data available. Stopping.")
                break

            # Prepend chunk to maintain chronological order (oldest → newest)
            all_bars = bars + all_bars
            # Move the next window end just before this chunk's start to avoid overlap
            current_to = current_from  # avoid inclusive-boundary duplicate
            chunk += 1

        print(f"\n🏁 Completed fetching historical data. Total bars: {len(all_bars)}\n\n")
        return all_bars
