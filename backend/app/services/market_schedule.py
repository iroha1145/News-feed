from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo


EASTERN = ZoneInfo("America/New_York")


def _observed(day: date) -> date:
    if day.weekday() == 5:
        return day - timedelta(days=1)
    if day.weekday() == 6:
        return day + timedelta(days=1)
    return day


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    current = date(year, month, 1)
    shift = (weekday - current.weekday()) % 7
    return current + timedelta(days=shift + 7 * (n - 1))


def _last_weekday(year: int, month: int, weekday: int) -> date:
    current = date(year + (month == 12), 1 if month == 12 else month + 1, 1) - timedelta(days=1)
    return current - timedelta(days=(current.weekday() - weekday) % 7)


def _easter_sunday(year: int) -> date:
    # Anonymous Gregorian algorithm.
    a = year % 19
    b, c = divmod(year, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = (h + l - 7 * m + 114) % 31 + 1
    return date(year, month, day)


def nyse_holidays(year: int) -> set[date]:
    holidays = {
        _observed(date(year, 1, 1)),
        _nth_weekday(year, 1, 0, 3),
        _nth_weekday(year, 2, 0, 3),
        _easter_sunday(year) - timedelta(days=2),
        _last_weekday(year, 5, 0),
        _observed(date(year, 7, 4)),
        _nth_weekday(year, 9, 0, 1),
        _nth_weekday(year, 11, 3, 4),
        _observed(date(year, 12, 25)),
    }
    if year >= 2022:
        holidays.add(_observed(date(year, 6, 19)))
    next_new_year_observed = _observed(date(year + 1, 1, 1))
    if next_new_year_observed.year == year:
        holidays.add(next_new_year_observed)
    return holidays


def is_nyse_trading_day(day: date) -> bool:
    return day.weekday() < 5 and day not in nyse_holidays(day.year)


def _previous_trading_day(day: date) -> date:
    current = day - timedelta(days=1)
    while not is_nyse_trading_day(current):
        current -= timedelta(days=1)
    return current


def is_nyse_early_close(day: date) -> bool:
    if not is_nyse_trading_day(day):
        return False
    thanksgiving = _nth_weekday(day.year, 11, 3, 4)
    if day == thanksgiving + timedelta(days=1):
        return True
    if day == _previous_trading_day(date(day.year, 7, 4)):
        return True
    christmas_eve = date(day.year, 12, 24)
    return day == christmas_eve and is_nyse_trading_day(christmas_eve)


def nyse_close_time_et(day: date) -> time:
    return time(13, 0) if is_nyse_early_close(day) else time(16, 0)


def configured_cycle_times() -> tuple[time, ...]:
    from app.config import settings

    values: list[time] = []
    for raw in settings.hot_cycle_times_et.split(","):
        raw = raw.strip()
        if raw not in {"08:00", "12:00", "16:00"}:
            raise ValueError("HOT_CYCLE_TIMES_ET only supports 08:00,12:00,16:00")
        hour, minute = (int(value) for value in raw.split(":"))
        value = time(hour, minute)
        if value not in values:
            values.append(value)
    if settings.hot_cycle_optional_20_et:
        values.append(time(20, 0))
    return tuple(values)


def scheduled_slots_for_day(day: date) -> list[tuple[str, datetime]]:
    if not is_nyse_trading_day(day):
        return []
    slots = []
    for configured in configured_cycle_times():
        actual = nyse_close_time_et(day) if configured == time(16, 0) else configured
        trigger = f"scheduled_{configured.hour:02d}{configured.minute:02d}"
        slots.append((trigger, datetime.combine(day, actual, tzinfo=EASTERN)))
    return slots


def due_cycle_trigger(now: datetime | None = None, *, grace_minutes: int = 10) -> str | None:
    current = (now or datetime.now(timezone.utc)).astimezone(EASTERN)
    for trigger, scheduled in scheduled_slots_for_day(current.date()):
        if scheduled <= current < scheduled + timedelta(minutes=grace_minutes):
            return trigger
    return None


def next_cycle_at(now: datetime | None = None) -> datetime | None:
    current = (now or datetime.now(timezone.utc)).astimezone(EASTERN)
    for offset in range(0, 15):
        day = current.date() + timedelta(days=offset)
        for _, scheduled in scheduled_slots_for_day(day):
            if scheduled > current:
                return scheduled.astimezone(timezone.utc)
    return None
