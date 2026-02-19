# ===================================================================================================
# ===================================================================================================

from zoneinfo import ZoneInfo
from decimal import Decimal
from datetime import datetime, timedelta, date
from math import log, sqrt, exp
from scipy.stats import norm

from library.core.calendar import CALENDAR_LOADER

# ===================================================================================================


class EXPIRY:
    def __init__(self):
        is_trading_day, start_dt, end_dt = CALENDAR_LOADER.session_window(
            exchange="NSE"
        )
        self.is_trading_day = is_trading_day
        self.start_dt = start_dt
        self.end_dt = end_dt
        self.time_fractions = self.get_time_fractions()

    def get_time_fractions(self):
        if not self.is_trading_day:
            return {}

        # create dict of datetime:time_fraction for each second between start_dt and end_dt
        time_fractions = {}
        total_seconds = int((self.end_dt - self.start_dt).total_seconds())
        total_seconds_decimal = Decimal(total_seconds)
        for sec in range(total_seconds + 1):
            current_dt = self.start_dt + timedelta(seconds=sec)
            current_dt = current_dt.isoformat()
            sec_decimal = Decimal(sec)
            time_fraction = Decimal("1") - (sec_decimal / total_seconds_decimal)
            time_fractions[current_dt] = time_fraction
        return time_fractions

    def get_days_to_expiry(self, expiry_date: date) -> float:
        dt_now = datetime.now().replace(microsecond=0, tzinfo=ZoneInfo("Asia/Kolkata"))
        today_date = dt_now.date()

        if expiry_date < today_date:
            raise ValueError("Expiry date cannot be in the past")

        delta = expiry_date - today_date

        if not self.is_trading_day or dt_now < self.start_dt:
            return delta.days
        elif dt_now >= self.end_dt:
            return delta.days - 1

        dt_now = dt_now.isoformat()
        day_fraction = self.time_fractions.get(dt_now, 0)
        days_to_expiry = delta.days + day_fraction
        return days_to_expiry


class BLACK_SCHOLES:
    """
    European option pricing, implied volatility and Greeks
    """

    def __init__(self, S, K, r, q, t_days):
        """
        S : Spot price
        K : Strike
        r : Risk-free rate (annual)
        q : Dividend yield / carry
        t_days : Time to expiry in days (can be float or Decimal)
        """
        self.S = S
        self.K = K
        self.r = r
        self.q = q
        self.t_days = t_days
        self.t = self.days_to_years(t_days)

    @staticmethod
    def days_to_years(days):
        """Convert days to years as float (days/365)"""
        return float(Decimal(days) / Decimal('365'))

    # ------------------------------------------------------------
    # d1 and d2
    # ------------------------------------------------------------
    def d1_d2(self, sigma):
        d1 = (log(self.S / self.K) + (self.r - self.q + 0.5 * sigma**2) * self.t) / (
            sigma * sqrt(self.t)
        )
        d2 = d1 - sigma * sqrt(self.t)
        return d1, d2

    # ------------------------------------------------------------
    # Prices
    # ------------------------------------------------------------
    def call_price(self, sigma):
        d1, d2 = self.d1_d2(sigma)
        return self.S * exp(-self.q * self.t) * norm.cdf(d1) - self.K * exp(
            -self.r * self.t
        ) * norm.cdf(d2)

    def put_price(self, sigma):
        d1, d2 = self.d1_d2(sigma)
        return self.K * exp(-self.r * self.t) * norm.cdf(-d2) - self.S * exp(
            -self.q * self.t
        ) * norm.cdf(-d1)

    # ------------------------------------------------------------
    # Vega (used by IV + Greeks)
    # ------------------------------------------------------------
    def vega(self, sigma):
        d1, _ = self.d1_d2(sigma)
        return self.S * exp(-self.q * self.t) * norm.pdf(d1) * sqrt(self.t)

    # ------------------------------------------------------------
    # Implied Volatility
    # ------------------------------------------------------------
    def implied_vol(self, market_price, option_type="call", tol=1e-3, max_iter=1000):

        sigma = 0.50

        for _ in range(max_iter):
            if option_type == "call":
                price = self.call_price(sigma)
            else:
                price = self.put_price(sigma)

            diff = price - market_price
            if abs(diff) < tol:
                return sigma

            vega = self.vega(sigma)
            if vega < 1e-8:
                break

            sigma -= diff / vega

        return sigma

    # ------------------------------------------------------------
    # Greeks
    # ------------------------------------------------------------
    def delta(self, sigma, option_type="call"):
        d1, _ = self.d1_d2(sigma)

        if option_type == "call":
            return exp(-self.q * self.t) * norm.cdf(d1)
        else:
            return -exp(-self.q * self.t) * norm.cdf(-d1)

    def gamma(self, sigma):
        d1, _ = self.d1_d2(sigma)
        return exp(-self.q * self.t) * norm.pdf(d1) / (self.S * sigma * sqrt(self.t))

    def theta(self, sigma, option_type="call"):
        d1, d2 = self.d1_d2(sigma)

        first_term = (
            -self.S * norm.pdf(d1) * sigma * exp(-self.q * self.t) / (2 * sqrt(self.t))
        )

        if option_type == "call":
            second = self.q * self.S * exp(-self.q * self.t) * norm.cdf(d1)
            third = self.r * self.K * exp(-self.r * self.t) * norm.cdf(d2)
            return first_term - second - third
        else:
            second = self.q * self.S * exp(-self.q * self.t) * norm.cdf(-d1)
            third = self.r * self.K * exp(-self.r * self.t) * norm.cdf(-d2)
            return first_term + second + third

    def rho(self, sigma, option_type="call"):
        _, d2 = self.d1_d2(sigma)

        if option_type == "call":
            return self.K * self.t * exp(-self.r * self.t) * norm.cdf(d2)
        else:
            return -self.K * self.t * exp(-self.r * self.t) * norm.cdf(-d2)

    # ------------------------------------------------------------
    # Convenience: All Greeks in one shot
    # ------------------------------------------------------------
    def greeks(self, sigma, option_type="call"):
        return {
            "delta": self.delta(sigma, option_type),
            "gamma": self.gamma(sigma),
            "vega": self.vega(sigma),
            "theta": self.theta(sigma, option_type),
            "rho": self.rho(sigma, option_type),
        }

    def greeks_scaled(self, sigma, option_type="call"):
        """Greeks in reporting units: vega per 1% vol, theta per day, rho per 1% rate."""
        g = self.greeks(sigma, option_type=option_type)
        if g.get("vega") is not None:
            g["vega"] = g["vega"] / 100.0
        if g.get("theta") is not None:
            g["theta"] = g["theta"] / 365.0
        if g.get("rho") is not None:
            g["rho"] = g["rho"] / 100.0
        return g


class BLACK_76:
    """
    Black-76 model for European options on futures / forwards
    """

    def __init__(self, F, K, r, t_days):
        """
        F : Forward / Futures price
        K : Strike
        r : Risk-free rate (annual)
        t_days : Time to expiry in days (can be float or Decimal)
        """
        self.F = F
        self.K = K
        self.r = r
        self.t_days = t_days
        self.t = self.days_to_years(t_days)

    @staticmethod
    def days_to_years(days):
        """Convert days to years as float (days/365)"""
        return float(Decimal(days) / Decimal('365'))

    # ------------------------------------------------------------
    # d1 and d2
    # ------------------------------------------------------------
    def d1_d2(self, sigma):
        d1 = (log(self.F / self.K) + 0.5 * sigma**2 * self.t) / (sigma * sqrt(self.t))
        d2 = d1 - sigma * sqrt(self.t)
        return d1, d2

    # ------------------------------------------------------------
    # Prices
    # ------------------------------------------------------------
    def call_price(self, sigma):
        d1, d2 = self.d1_d2(sigma)
        return exp(-self.r * self.t) * (self.F * norm.cdf(d1) - self.K * norm.cdf(d2))

    def put_price(self, sigma):
        d1, d2 = self.d1_d2(sigma)
        return exp(-self.r * self.t) * (self.K * norm.cdf(-d2) - self.F * norm.cdf(-d1))

    # ------------------------------------------------------------
    # Vega
    # ------------------------------------------------------------
    def vega(self, sigma):
        d1, _ = self.d1_d2(sigma)
        return exp(-self.r * self.t) * self.F * norm.pdf(d1) * sqrt(self.t)

    # ------------------------------------------------------------
    # Implied Volatility
    # ------------------------------------------------------------
    def implied_vol(
        self,
        market_price,
        option_type="call",
        tol=1e-3,
        max_iter=1000,
    ):
        sigma = 0.50

        for _ in range(max_iter):
            price = (
                self.call_price(sigma)
                if option_type == "call"
                else self.put_price(sigma)
            )

            diff = price - market_price
            if abs(diff) < tol:
                return sigma

            vega = self.vega(sigma)
            if vega < 1e-8:
                break

            sigma -= diff / vega

        return sigma

    # ------------------------------------------------------------
    # Greeks
    # ------------------------------------------------------------
    def delta(self, sigma, option_type="call"):
        d1, _ = self.d1_d2(sigma)
        df = exp(-self.r * self.t)

        if option_type == "call":
            return df * norm.cdf(d1)
        else:
            return -df * norm.cdf(-d1)

    def gamma(self, sigma):
        d1, _ = self.d1_d2(sigma)
        return exp(-self.r * self.t) * norm.pdf(d1) / (self.F * sigma * sqrt(self.t))

    def theta(self, sigma, option_type="call"):
        d1, d2 = self.d1_d2(sigma)
        df = exp(-self.r * self.t)

        first = -df * self.F * norm.pdf(d1) * sigma / (2 * sqrt(self.t))

        if option_type == "call":
            second = self.r * df * (self.F * norm.cdf(d1) - self.K * norm.cdf(d2))
            return first + second
        else:
            second = self.r * df * (self.K * norm.cdf(-d2) - self.F * norm.cdf(-d1))
            return first + second

    def rho(self, sigma, option_type="call"):
        _, d2 = self.d1_d2(sigma)
        df = exp(-self.r * self.t)

        if option_type == "call":
            return self.K * self.t * df * norm.cdf(d2)
        else:
            return -self.K * self.t * df * norm.cdf(-d2)

    # ------------------------------------------------------------
    # Convenience: All Greeks
    # ------------------------------------------------------------
    def greeks(self, sigma, option_type="call"):
        return {
            "delta": self.delta(sigma, option_type),
            "gamma": self.gamma(sigma),
            "vega": self.vega(sigma),
            "theta": self.theta(sigma, option_type),
            "rho": self.rho(sigma, option_type),
        }

    def greeks_scaled(self, sigma, option_type="call"):
        """Greeks in reporting units: vega per 1% vol, theta per day, rho per 1% rate."""
        g = self.greeks(sigma, option_type=option_type)
        if g.get("vega") is not None:
            g["vega"] = g["vega"] / 100.0
        if g.get("theta") is not None:
            g["theta"] = g["theta"] / 365.0
        if g.get("rho") is not None:
            g["rho"] = g["rho"] / 100.0
        return g
