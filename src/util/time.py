from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import re
from zoneinfo import ZoneInfo


UTC = timezone.utc


_DURATION_RE = re.compile(r"^\s*(\d+)\s*([smhdw])\s*$", re.IGNORECASE)


def now_utc() -> datetime:
    return datetime.now(tz=UTC)


def to_iso_utc(dt: datetime) -> str:
    if dt.tzinfo is None:
        raise ValueError("datetime must be timezone-aware")
    return dt.astimezone(UTC).replace(microsecond=0).isoformat()


def parse_duration(value: str) -> timedelta:
    """
    Parse simple durations like: 30m, 6h, 2d, 1w.
    """
    match = _DURATION_RE.match(value)
    if not match:
        raise ValueError(f"Invalid duration: {value!r} (expected e.g. 6h, 2d)")
    amount = int(match.group(1))
    unit = match.group(2).lower()
    if unit == "s":
        return timedelta(seconds=amount)
    if unit == "m":
        return timedelta(minutes=amount)
    if unit == "h":
        return timedelta(hours=amount)
    if unit == "d":
        return timedelta(days=amount)
    if unit == "w":
        return timedelta(weeks=amount)
    raise ValueError(f"Unsupported duration unit: {unit!r}")


@dataclass(frozen=True)
class DailyTime:
    hour: int
    minute: int

    @staticmethod
    def parse(value: str) -> "DailyTime":
        parts = value.strip().split(":")
        if len(parts) != 2:
            raise ValueError(f"Invalid DAILY_DIGEST_TIME: {value!r} (expected HH:MM)")
        hour = int(parts[0])
        minute = int(parts[1])
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            raise ValueError(f"Invalid DAILY_DIGEST_TIME: {value!r} (expected HH:MM)")
        return DailyTime(hour=hour, minute=minute)


def next_run_utc(*, tz_name: str, daily_time: DailyTime, now: datetime | None = None) -> datetime:
    tz = ZoneInfo(tz_name)
    now_local = (now or now_utc()).astimezone(tz)

    candidate = now_local.replace(
        hour=daily_time.hour, minute=daily_time.minute, second=0, microsecond=0
    )
    if candidate <= now_local:
        candidate = candidate + timedelta(days=1)
    return candidate.astimezone(UTC)

