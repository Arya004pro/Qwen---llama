"""Natural-language time parser for analytics queries."""

from __future__ import annotations

import calendar
import re
from datetime import date, timedelta

from utils.date_parser import parse_comparison_date_ranges

_MONTH_NAME_RE = re.compile(
    r"\b(january|february|march|april|may|june|july|august|september|october|november|december)\b",
    re.IGNORECASE,
)
_MONTH_MAP = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12,
}
_YEAR_RE = re.compile(r"\b(20\d{2})\b")
_QUARTER_RE = re.compile(r"\bq([1-4])(?:\s+(20\d{2}))?\b", re.IGNORECASE)
_LAST_N_DAYS_RE = re.compile(r"\blast\s+(\d{1,3})\s+days?\b", re.IGNORECASE)


def _month_range(year: int, month: int) -> tuple[date, date]:
    last_day = calendar.monthrange(year, month)[1]
    return date(year, month, 1), date(year, month, last_day)


def _quarter_range(year: int, q: int) -> tuple[date, date]:
    start_month = (q - 1) * 3 + 1
    end_month = start_month + 2
    return date(year, start_month, 1), date(year, end_month, calendar.monthrange(year, end_month)[1])


def _year_range(year: int) -> tuple[date, date]:
    return date(year, 1, 1), date(year, 12, 31)


def _label_for(s: date, e: date) -> str:
    if s.month == 1 and s.day == 1 and e.month == 12 and e.day == 31 and s.year == e.year:
        return str(s.year)
    if s.day == 1 and e.day >= 28 and s.year == e.year and s.month == e.month:
        return f"{calendar.month_name[s.month]} {s.year}"
    return f"{s.isoformat()} to {e.isoformat()}"


def _pack_range(s: date, e: date, label: str | None = None) -> dict:
    return {"start": s.isoformat(), "end": e.isoformat(), "label": label or _label_for(s, e)}


def parse_time_ranges_from_query(user_query: str, today: date | None = None) -> tuple[list[dict], str | None]:
    """
    Parse time ranges from natural language.

    Returns:
      (ranges, suggested_query_type)
    where suggested_query_type may be "comparison" for YoY / explicit comparisons.
    """
    q = (user_query or "").lower().strip()
    if not q:
        return [], None
    if today is None:
        today = date.today()

    is_yoy = any(k in q for k in ("yoy", "year over year", "year-on-year", "year on year"))

    # Explicit comparisons (vs / compare ... and ...)
    if (" vs " in q or " versus " in q or ("compare" in q and " and " in q)) and not is_yoy:
        try:
            (s1, e1), (s2, e2) = parse_comparison_date_ranges(user_query)
            return [_pack_range(s1, e1), _pack_range(s2, e2)], "comparison"
        except Exception:
            pass

    # last N days
    m_days = _LAST_N_DAYS_RE.search(q)
    if m_days:
        n = max(1, int(m_days.group(1)))
        end = today
        start = end - timedelta(days=n - 1)
        current = _pack_range(start, end, f"Last {n} days")
        if is_yoy:
            ly_start = date(start.year - 1, start.month, start.day)
            ly_end = date(end.year - 1, end.month, end.day)
            return [current, _pack_range(ly_start, ly_end, f"Last {n} days (LY)")], "comparison"
        return [current], None

    # this month / last month
    if "this month" in q:
        s = date(today.year, today.month, 1)
        e = today
        current = _pack_range(s, e, f"This month ({calendar.month_name[today.month]} {today.year})")
        if is_yoy:
            ly_s = date(today.year - 1, today.month, 1)
            ly_e = date(today.year - 1, today.month, min(today.day, calendar.monthrange(today.year - 1, today.month)[1]))
            return [current, _pack_range(ly_s, ly_e, f"This month LY ({calendar.month_name[today.month]} {today.year - 1})")], "comparison"
        return [current], None

    if "last month" in q:
        y = today.year if today.month > 1 else today.year - 1
        m = today.month - 1 if today.month > 1 else 12
        s, e = _month_range(y, m)
        current = _pack_range(s, e, f"Last month ({calendar.month_name[m]} {y})")
        if is_yoy:
            ly_s, ly_e = _month_range(y - 1, m)
            return [current, _pack_range(ly_s, ly_e, f"Last month LY ({calendar.month_name[m]} {y - 1})")], "comparison"
        return [current], None

    # this year / last year
    if "this year" in q:
        s = date(today.year, 1, 1)
        e = today
        current = _pack_range(s, e, f"This year ({today.year} YTD)")
        if is_yoy:
            ly_s = date(today.year - 1, 1, 1)
            ly_e = date(today.year - 1, today.month, min(today.day, calendar.monthrange(today.year - 1, today.month)[1]))
            return [current, _pack_range(ly_s, ly_e, f"This year LY ({today.year - 1} YTD)")], "comparison"
        return [current], None

    if "last year" in q:
        y = today.year - 1
        s, e = _year_range(y)
        current = _pack_range(s, e, f"Last year ({y})")
        if is_yoy:
            ly_s, ly_e = _year_range(y - 1)
            return [current, _pack_range(ly_s, ly_e, f"Last year LY ({y - 1})")], "comparison"
        return [current], None

    # YoY with explicit month / quarter / year, or generic YoY fallback
    if is_yoy:
        month_m = _MONTH_NAME_RE.search(q)
        year_m = _YEAR_RE.search(q)
        if month_m:
            month = _MONTH_MAP[month_m.group(1).lower()]
            year = int(year_m.group(1)) if year_m else today.year
            s1, e1 = _month_range(year, month)
            s2, e2 = _month_range(year - 1, month)
            return [_pack_range(s1, e1), _pack_range(s2, e2)], "comparison"

        q_m = _QUARTER_RE.search(q)
        if q_m:
            qn = int(q_m.group(1))
            year = int(q_m.group(2)) if q_m.group(2) else today.year
            s1, e1 = _quarter_range(year, qn)
            s2, e2 = _quarter_range(year - 1, qn)
            return [_pack_range(s1, e1), _pack_range(s2, e2)], "comparison"

        if year_m:
            year = int(year_m.group(1))
            s1, e1 = _year_range(year)
            s2, e2 = _year_range(year - 1)
            return [_pack_range(s1, e1), _pack_range(s2, e2)], "comparison"

        # Generic YoY comparison fallback: last full year vs previous full year.
        s1, e1 = _year_range(today.year - 1)
        s2, e2 = _year_range(today.year - 2)
        return [_pack_range(s1, e1), _pack_range(s2, e2)], "comparison"

    # ── Standalone quarter (no year) e.g. "Q1" ──────────────────────────────
    q_m = _QUARTER_RE.search(q)
    if q_m:
        qn   = int(q_m.group(1))
        year = int(q_m.group(2)) if q_m.group(2) else today.year
        s, e = _quarter_range(year, qn)
        return [_pack_range(s, e)], None

    # ── Specific month + year e.g. "January 2024" ────────────────────────────
    month_m = _MONTH_NAME_RE.search(q)
    year_m  = _YEAR_RE.search(q)
    if month_m and year_m:
        month = _MONTH_MAP[month_m.group(1).lower()]
        year  = int(year_m.group(1))
        s, e  = _month_range(year, month)
        return [_pack_range(s, e)], None

    # ── Standalone year e.g. "in 2024", "for 2023", "revenue by year" ────────
    # For year-bucket time_series with no specific year (e.g. "Revenue by year"),
    # we return a wide all-data range so the SQL builder can use an open range.
    # For queries with a specific year (e.g. "monthly trend in 2024"), we return
    # that year's full range.
    if year_m:
        year = int(year_m.group(1))
        s, e = _year_range(year)
        return [_pack_range(s, e)], None

    return [], None

