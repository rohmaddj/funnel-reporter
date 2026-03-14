"""Date helpers for the weekly collector."""

from datetime import datetime, timedelta


def get_week_range() -> tuple[str, str]:
    """
    Returns (start, end) for the most recently completed Mon–Sun week.
    Run this on any day of the current week and it will return last week's range.

    Example: called on Wednesday Mar 18 → returns ("2026-03-09", "2026-03-15")
    """
    today = datetime.utcnow().date()
    # Monday of THIS week
    this_monday = today - timedelta(days=today.weekday())
    # Last Monday
    last_monday = this_monday - timedelta(weeks=1)
    last_sunday = last_monday + timedelta(days=6)

    return last_monday.isoformat(), last_sunday.isoformat()


def format_date(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")