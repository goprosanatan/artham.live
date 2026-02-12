# ===================================================================================================
# ===================================================================================================

import datetime
import json
import logging
from pathlib import Path
from typing import List, Optional

from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

# ===================================================================================================
#

class CALENDAR_SERVICE:
	"""Loads calendars and produces future bar timestamps."""

	def __init__(self, calendar_dir: Path):
		self.calendar_dir = calendar_dir
		self.calendars: dict = {}
		self._load_all()

	# ---- loading ---------------------------------------------------
	def _load_all(self):
		if not self.calendar_dir.exists():
			msg = f"Calendar path not found: {self.calendar_dir}"
			print(msg)
			logger.warning(msg)
			return

		loaded = 0
		for f in self.calendar_dir.glob("*.json"):
			# Skip template/example calendar files
			if f.name.upper() == "EXAMPLE.JSON":
				continue
			try:
				with open(f) as file:
					data = json.load(file)
					exchange = data.get("exchange")
					if exchange:
						self.calendars[exchange] = data
						loaded += 1
			except Exception as e:
				msg = f"Error loading calendar {f}: {e}"
				print(msg)
				logger.error(msg)
		print(f"Loaded {loaded} calendars from {self.calendar_dir}")
		logger.info(f"Loaded {loaded} calendars from {self.calendar_dir}")

	# ---- accessors -------------------------------------------------
	def get_calendar(self, exchange: Optional[str]) -> Optional[dict]:
		if not exchange:
			msg = "No exchange provided to get_calendar."
			print(msg)
			logger.warning(msg)
			return None
		cal = self.calendars.get(exchange)
		if cal is None:
			msg = f"Calendar not found for exchange: {exchange}"
			print(msg)
			logger.warning(msg)
		return cal

	# ---- public API ------------------------------------------------
	def session_slots_after(self, exchange: str, timeframe: str, now: datetime.datetime) -> List[datetime.datetime]:
		calendar = self.get_calendar(exchange)
		if not calendar or timeframe not in {"1m", "1D"}:
			msg = f"Invalid calendar or timeframe for session_slots_after: exchange={exchange}, timeframe={timeframe}"
			print(msg)
			logger.warning(msg)
			return []

		tz = ZoneInfo(calendar.get("timezone"))
		now = now.astimezone(tz) if now.tzinfo else now.replace(tzinfo=tz)

		if timeframe == "1m":
			slots: List[datetime.datetime] = []
			unique_days = set()

			# 1) Remaining minutes for today
			today_slots = self._future_minutes_for_day(calendar, tz, now)
			slots.extend(today_slots)
			if today_slots:
				unique_days.add(today_slots[0].date())

			# 2) Add future full days until we cover at least two trading days
			next_cursor = now.date()
			while len(unique_days) < 2:
				next_day = self._next_trading_day(calendar, tz, next_cursor)
				if not next_day:
					break
				day_slots = self._minutes_for_full_day(calendar, tz, next_day)
				if day_slots:
					slots.extend(day_slots)
					unique_days.add(day_slots[0].date())
				next_cursor = next_day

			return slots

		slots = self._next_trading_days(calendar, tz, now.date(), days=50)
		return slots

	def session_slots_between(self, exchange: str, timeframe: str, start_dt: datetime.datetime, end_dt: datetime.datetime) -> List[datetime.datetime]:
		"""Generate slots between two datetime objects, respecting trading sessions."""
		calendar = self.get_calendar(exchange)

		if not calendar:
			msg = f"Calendar not found for exchange: {exchange}"
			print(msg)
			logger.error(msg)
			raise ValueError(msg)

		tz = ZoneInfo(calendar.get("timezone"))
		start_dt = start_dt.astimezone(tz) if start_dt.tzinfo else start_dt.replace(tzinfo=tz)
		end_dt = end_dt.astimezone(tz) if end_dt.tzinfo else end_dt.replace(tzinfo=tz)

		if start_dt > end_dt:
			msg = f"Start datetime {start_dt} is after end datetime {end_dt} in session_slots_between."
			print(msg)
			logger.warning(msg)
			return []

		# For daily timeframes, collect trading day midnights
		if timeframe.endswith("D"):
			slots_dt: List[datetime.datetime] = []
			current_date = start_dt.date()

			while current_date <= end_dt.date():
				sessions = self._sessions_for_date(calendar, tz, current_date)
				if sessions:
					# Use midnight of the trading day, not session open
					day_midnight = datetime.datetime(current_date.year, current_date.month, current_date.day, 0, 0, 0, tzinfo=tz)
					if start_dt <= day_midnight <= end_dt:
						slots_dt.append(day_midnight)
				current_date += datetime.timedelta(days=1)

			return slots_dt

		# For intraday timeframes, generate slots within trading sessions
		delta = self._timeframe_to_timedelta(timeframe)
		if not delta:
			return []

		slots_dt: List[datetime.datetime] = []
		current_date = start_dt.date()

		while current_date <= end_dt.date():
			sessions = self._sessions_for_date(calendar, tz, current_date)

			for sess in sessions:
				sess_start = max(start_dt, sess["open_dt"])
				sess_end = min(end_dt, sess["close_dt"])

				if sess_start > sess_end:
					continue

				# Align to timeframe boundary
				if sess_start.second or sess_start.microsecond:
					sess_start = sess_start.replace(second=0, microsecond=0)
					if sess_start < start_dt:
						sess_start += delta

				current = sess_start
				while current <= sess_end:
					if start_dt <= current <= end_dt:
						slots_dt.append(current)
					current += delta

			current_date += datetime.timedelta(days=1)

		return slots_dt

	def session_window(self, exchange: str, dt_date: Optional[datetime.date] = None):
		"""Return (is_trading_day, start_dt, end_dt) for the given exchange/date."""
		calendar = self.get_calendar(exchange)
		if not calendar:
			msg = f"Calendar not found for exchange: {exchange} in session_window."
			print(msg)
			logger.warning(msg)
			return False, None, None

		tz = ZoneInfo(calendar.get("timezone"))
		target_date = dt_date or datetime.datetime.now(tz=tz).date()
		sessions = self._sessions_for_date(calendar, tz, target_date)

		if not sessions:
			return False, None, None

		start_dt = sessions[0]["open_dt"]
		end_dt = sessions[-1]["close_dt"]
		return True, start_dt, end_dt

	def _timeframe_to_timedelta(self, timeframe: str) -> Optional[datetime.timedelta]:
		if timeframe.endswith("m"):
			return datetime.timedelta(minutes=int(timeframe[:-1]))
		if timeframe.endswith("H"):
			return datetime.timedelta(hours=int(timeframe[:-1]))
		if timeframe.endswith("D"):
			return datetime.timedelta(days=int(timeframe[:-1]))
		return None

	def _sessions_for_date(self, calendar: dict, tz: ZoneInfo, dt_date: datetime.date) -> List[dict]:
		sessions_base = calendar.get("sessions", {})
		overrides = calendar.get("session_overrides", {})

		date_str = dt_date.isoformat()
		sessions = {}

		if date_str in overrides:
			override = overrides[date_str]
			for name, sess in sessions_base.items():
				if name in override:
					if override[name] == "closed":
						continue
					if isinstance(override[name], dict):
						if not self._session_enabled(override[name]):
							continue
						sessions[name] = override[name]
					else:
						if not self._session_enabled(sess):
							continue
						sessions[name] = sess
				else:
					if not self._session_enabled(sess):
						continue
					sessions[name] = sess
		else:
			weekday = dt_date.isoweekday()  # 1=Mon ... 7=Sun
			for name, sess in sessions_base.items():
				if weekday in sess.get("days", []) and self._session_enabled(sess):
					sessions[name] = sess

		normalized = []
		for name, sess in sessions.items():
			try:
				open_dt = self._combine(tz, dt_date, sess["open"])
				close_dt = self._combine(tz, dt_date, sess["close"])
				normalized.append({"name": name, "open_dt": open_dt, "close_dt": close_dt})
			except Exception:
				continue

		normalized.sort(key=lambda s: s["open_dt"])
		return normalized

	def _session_enabled(self, session: dict) -> bool:
		return bool(session.get("enabled", True))

	def _combine(self, tz: ZoneInfo, dt_date: datetime.date, hhmm: str) -> datetime.datetime:
		hour, minute = map(int, hhmm.split(":"))
		return datetime.datetime(dt_date.year, dt_date.month, dt_date.day, hour, minute, tzinfo=tz)

	def _future_minutes_for_day(self, calendar: dict, tz: ZoneInfo, now: datetime.datetime) -> List[datetime.datetime]:
		sessions = self._sessions_for_date(calendar, tz, now.date())
		future: List[datetime.datetime] = []

		for sess in sessions:
			# If we are past this session, skip
			if now >= sess["close_dt"]:
				continue

			# Determine the first minute strictly after `now`
			if now < sess["open_dt"]:
				start = sess["open_dt"]
			else:
				# Inside session window: move to the NEXT minute boundary (exclusive of `now`)
				if now.second or now.microsecond:
					start = now.replace(second=0, microsecond=0) + datetime.timedelta(minutes=1)
				else:
					start = now + datetime.timedelta(minutes=1)

			cur = start
			while cur <= sess["close_dt"]:
				future.append(cur)
				cur += datetime.timedelta(minutes=1)

		return future

	def _next_trading_days(self, calendar: dict, tz: ZoneInfo, from_date: datetime.date, days: int = 50) -> List[datetime.datetime]:
		results: List[datetime.datetime] = []
		cursor = from_date + datetime.timedelta(days=1)

		while len(results) < days and (cursor - from_date).days <= 366:
			sessions = self._sessions_for_date(calendar, tz, cursor)
			if sessions:
				# Return midnight of trading day, not session open
				day_midnight = datetime.datetime(cursor.year, cursor.month, cursor.day, 0, 0, 0, tzinfo=tz)
				results.append(day_midnight)
			cursor += datetime.timedelta(days=1)

		return results

	def _next_trading_day(self, calendar: dict, tz: ZoneInfo, from_date: datetime.date) -> Optional[datetime.date]:
		next_days = self._next_trading_days(calendar, tz, from_date, days=1)
		if not next_days:
			return None
		return next_days[0].date()

	def _minutes_for_full_day(self, calendar: dict, tz: ZoneInfo, dt_date: datetime.date) -> List[datetime.datetime]:
		sessions = self._sessions_for_date(calendar, tz, dt_date)
		future: List[datetime.datetime] = []

		for sess in sessions:
			cur = sess["open_dt"]
			while cur <= sess["close_dt"]:
				future.append(cur)
				cur += datetime.timedelta(minutes=1)

		return future


CALENDAR_LOADER = CALENDAR_SERVICE(Path(__file__).parent.parent / "calendars")

# -----------------------------------------------------------

