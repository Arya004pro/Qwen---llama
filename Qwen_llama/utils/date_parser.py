"""utils/date_parser.py

Parses natural-language date expressions into (start_date, end_date) tuples.

Single-period helpers
---------------------
  parse_date_range(time_range, raw_text)  → (start, end)

Comparison helpers
------------------
  parse_comparison_date_ranges(raw_text) → ((start1, end1), (start2, end2))

Supported comparison formats
-----------------------------
  Month vs Month       "compare revenue in March vs April 2024"
                       "compare revenue in January and March 2024"
  Month-range vs range "Jan to Mar vs Apr to Jun 2024"
  Quarter vs Quarter   "Q1 vs Q2 2024" / "Q1 2024 vs Q2 2024"
  Year vs Year         "compare revenue in 2023 vs 2024"
  Cross-year months    "March 2023 vs March 2024"
"""

from __future__ import annotations
import re
from datetime import date
from calendar import monthrange

# ─── lookup tables ────────────────────────────────────────────────────────────

MONTHS: dict[str, int] = {
    "jan": 1, "january": 1,
    "feb": 2, "february": 2,
    "mar": 3, "march": 3,
    "apr": 4, "april": 4,
    "may": 5,
    "jun": 6, "june": 6,
    "jul": 7, "july": 7,
    "aug": 8, "august": 8,
    "sep": 9, "september": 9,
    "oct": 10, "october": 10,
    "nov": 11, "november": 11,
    "dec": 12, "december": 12,
}

QUARTERS: dict[str, tuple[int, int]] = {
    "q1": (1, 3),
    "q2": (4, 6),
    "q3": (7, 9),
    "q4": (10, 12),
}

_MONTH_SET = set(MONTHS)
_QUARTER_SET = set(QUARTERS)


# ─── low-level helpers ────────────────────────────────────────────────────────

def _extract_year(text: str) -> int | None:
    """Return the first 4-digit year found, or None."""
    for token in text.split():
        c = token.strip(",.?:()")
        if c.isdigit() and len(c) == 4:
            return int(c)
    return None


def _extract_all_years(text: str) -> list[int]:
    """Return all 4-digit years found, in order."""
    return [int(t.strip(",.?:()")) for t in text.split()
            if t.strip(",.?:()").isdigit() and len(t.strip(",.?:()")) == 4]


def _extract_months_ordered(text: str) -> list[int]:
    """Month numbers in the order they appear."""
    found = []
    for word in text.split():
        w = word.strip(",.?:()")
        if w in MONTHS:
            found.append(MONTHS[w])
    return found


def _extract_quarter(text: str) -> tuple[int, int] | None:
    """Return (start_month, end_month) for the first quarter token found."""
    for token in text.lower().split():
        t = token.strip(",.?:()")
        if t in QUARTERS:
            return QUARTERS[t]
    return None


def _quarter_range(start_month: int, end_month: int, year: int):
    s = date(year, start_month, 1)
    e = date(year, end_month, monthrange(year, end_month)[1])
    return s, e


def _month_range(month: int, year: int):
    s = date(year, month, 1)
    e = date(year, month, monthrange(year, month)[1])
    return s, e


def _year_range(year: int):
    return date(year, 1, 1), date(year, 12, 31)


def _is_comparison_text(text: str) -> bool:
    return " vs " in text or "versus" in text or (
        "compare" in text and " and " in text
    )


# ─── period parser for one half of a comparison ──────────────────────────────

def _parse_half(half: str, fallback_year: int | None) -> tuple[date, date]:
    """
    Parse one side of a comparison expression.
    Supports: single month, month range ("jan to mar"), quarter ("q1"), year.
    """
    h = half.lower().strip()

    year = _extract_year(h) or fallback_year
    if year is None:
        raise ValueError(f"Cannot determine year from: '{half}'")

    # Quarter: "q1", "q2", …
    q = _extract_quarter(h)
    if q:
        return _quarter_range(q[0], q[1], year)

    # Month range: "jan to mar", "january - march"
    months = _extract_months_ordered(h)
    if len(months) >= 2:
        s, e = months[0], months[-1]
        return date(year, s, 1), date(year, e, monthrange(year, e)[1])

    # Single month
    if len(months) == 1:
        return _month_range(months[0], year)

    # Year only (no month found)
    return _year_range(year)


# ─── public API ───────────────────────────────────────────────────────────────

def parse_date_range(time_range: str, raw_text: str) -> tuple[date, date]:
    """Parse a single date range from raw query text."""
    text = raw_text.lower()

    year = _extract_year(text)
    if year is None:
        raise ValueError("Year not found")

    found_months = _extract_months_ordered(text)

    # Don't treat a comparison query as a multi-month range
    if _is_comparison_text(text):
        if found_months:
            return _month_range(found_months[0], year)
        return _year_range(year)

    # Month range "jan to jun"
    if len(found_months) >= 2:
        s, e = found_months[0], found_months[-1]
        return date(year, s, 1), date(year, e, monthrange(year, e)[1])

    # Single month
    if len(found_months) == 1:
        return _month_range(found_months[0], year)

    # Quarter
    q = _extract_quarter(text)
    if q:
        return _quarter_range(q[0], q[1], year)

    # Numeric fallback
    tokens = text.replace(",", "").split()
    nums   = [t for t in tokens if t.isdigit()]
    if len(nums) >= 3:
        return date(int(nums[1]), 1, int(nums[0])), date(int(nums[1]), 12, int(nums[2]))

    # Year only
    return _year_range(year)


def parse_comparison_date_ranges(
    raw_text: str,
) -> tuple[tuple[date, date], tuple[date, date]]:
    """
    Parse a comparison query into two (start, end) date range tuples.

    Supported formats
    -----------------
    Month vs Month         "March vs April 2024"
                           "March 2024 vs April 2024"
    Month-range vs range   "Jan to Mar vs Apr to Jun 2024"
    Quarter vs Quarter     "Q1 vs Q2 2024" / "Q1 2024 vs Q3 2024"
    Year vs Year           "2023 vs 2024"
                           "compare revenue in 2023 vs 2024"
    compare … and …        "compare revenue in January and March 2024"
    Cross-year months      "March 2023 vs March 2024"
    """
    text = raw_text.lower().replace("versus", "vs")

    # Resolve the "global" year (used when one side omits it)
    years = _extract_all_years(text)
    global_year = years[-1] if years else None   # prefer the last (rightmost) year

    # ── Split on separator ────────────────────────────────────────────────────
    if " vs " in text:
        left, right = text.split(" vs ", 1)

    elif " and " in text and "compare" in text:
        # "compare revenue in january AND march 2024"
        left, right = text.split(" and ", 1)
        # Strip the "compare … in" preamble from left half
        for kw in ["compare revenue in", "compare sales in", "compare in", "compare"]:
            if left.startswith(kw):
                left = left[len(kw):].strip()
                break
    else:
        raise ValueError(
            "No comparison separator found. "
            "Use 'vs', 'versus', or 'compare … and …'"
        )

    # ── Special case: pure year vs year ──────────────────────────────────────
    # e.g. "2023 vs 2024" where each side is just a number
    left_years  = _extract_all_years(left)
    right_years = _extract_all_years(right)

    left_has_month  = bool(_extract_months_ordered(left))
    right_has_month = bool(_extract_months_ordered(right))
    left_has_quarter  = bool(_extract_quarter(left))
    right_has_quarter = bool(_extract_quarter(right))

    is_year_vs_year = (
        left_years and not left_has_month and not left_has_quarter and
        right_years and not right_has_month and not right_has_quarter
    )
    if is_year_vs_year:
        y1 = left_years[0]
        y2 = right_years[0]
        return _year_range(y1), _year_range(y2)

    # ── General half-parser ───────────────────────────────────────────────────
    try:
        range1 = _parse_half(left, global_year)
    except ValueError as exc:
        raise ValueError(f"Could not parse left period '{left.strip()}': {exc}") from exc

    try:
        range2 = _parse_half(right, global_year)
    except ValueError as exc:
        raise ValueError(f"Could not parse right period '{right.strip()}': {exc}") from exc

    return range1, range2