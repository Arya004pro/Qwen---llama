"""utils/date_parser.py — generalised natural-language date parsing.

Public API
----------
  parse_date_range(time_range, raw_text)           → (start, end)
  parse_comparison_date_ranges(raw_text)           → ((s1,e1), (s2,e2))
  parse_both_date_ranges(raw_text)                 → ((s1,e1), (s2,e2))

parse_comparison_date_ranges supports:
  "March vs April 2024"
  "Jan to Mar vs Apr to Jun 2024"
  "Q1 vs Q2 2024"
  "compare revenue in 2023 vs 2024"
  "March 2023 vs March 2024"
  "revenue growth from Q1 to Q2 2024"   ← NEW
  "growth from January to March 2024"   ← NEW

parse_both_date_ranges supports:
  "both January and March 2024"         ← NEW
  "both Q1 and Q3 2024"                 ← NEW
"""

from __future__ import annotations
import re
from datetime import date
from calendar import monthrange

MONTHS: dict[str, int] = {
    "jan": 1, "january": 1, "feb": 2, "february": 2,
    "mar": 3, "march": 3,   "apr": 4, "april": 4,
    "may": 5,
    "jun": 6, "june": 6,   "jul": 7, "july": 7,
    "aug": 8, "august": 8, "sep": 9, "september": 9,
    "oct": 10, "october": 10, "nov": 11, "november": 11,
    "dec": 12, "december": 12,
}

QUARTERS: dict[str, tuple[int, int]] = {
    "q1": (1, 3), "q2": (4, 6), "q3": (7, 9), "q4": (10, 12),
}


# ── helpers ───────────────────────────────────────────────────────────────────

def _extract_year(text: str) -> int | None:
    for t in text.split():
        c = t.strip(",.?:()")
        if c.isdigit() and len(c) == 4:
            return int(c)
    return None


def _extract_all_years(text: str) -> list[int]:
    return [int(t.strip(",.?:()")) for t in text.split()
            if t.strip(",.?:()").isdigit() and len(t.strip(",.?:()")) == 4]


def _extract_months_ordered(text: str) -> list[int]:
    """Extract month numbers in order, with fuzzy matching for common typos."""
    import difflib as _dl
    found = []
    for w in text.split():
        k = w.strip(",.?:()")
        if k in MONTHS:
            found.append(MONTHS[k])
        elif len(k) >= 3:   # Only attempt fuzzy on words long enough to be a month
            matches = _dl.get_close_matches(k, MONTHS.keys(), n=1, cutoff=0.80)
            if matches:
                found.append(MONTHS[matches[0]])
    return found


def _extract_quarter(text: str) -> tuple[int, int] | None:
    for t in text.lower().split():
        k = t.strip(",.?:()")
        if k in QUARTERS:
            return QUARTERS[k]
    return None


def _extract_all_quarters(text: str) -> list[tuple[int, int]]:
    found = []
    for t in text.lower().split():
        k = t.strip(",.?:()")
        if k in QUARTERS:
            found.append(QUARTERS[k])
    return found


def _quarter_range(sm: int, em: int, year: int) -> tuple[date, date]:
    return date(year, sm, 1), date(year, em, monthrange(year, em)[1])


def _month_range(month: int, year: int) -> tuple[date, date]:
    return date(year, month, 1), date(year, month, monthrange(year, month)[1])


def _year_range(year: int) -> tuple[date, date]:
    return date(year, 1, 1), date(year, 12, 31)


def _is_comparison_text(text: str) -> bool:
    return " vs " in text or "versus" in text or (
        "compare" in text and " and " in text
    )


def _parse_half(half: str, fallback_year: int | None) -> tuple[date, date]:
    """Parse one side of a comparison expression."""
    h    = half.lower().strip()
    year = _extract_year(h) or fallback_year
    if year is None:
        raise ValueError(f"Cannot determine year from: '{half}'")

    q = _extract_quarter(h)
    if q:
        return _quarter_range(q[0], q[1], year)

    months = _extract_months_ordered(h)
    if len(months) >= 2:
        return date(year, months[0], 1), date(year, months[-1],
                                               monthrange(year, months[-1])[1])
    if len(months) == 1:
        return _month_range(months[0], year)

    return _year_range(year)


# ── public API ────────────────────────────────────────────────────────────────

def parse_date_range(time_range: str, raw_text: str) -> tuple[date, date]:
    """Parse a single date range from raw query text."""
    text = raw_text.lower()
    year = _extract_year(text)
    if year is None:
        raise ValueError("Year not found")

    found_months = _extract_months_ordered(text)

    if _is_comparison_text(text):
        return _month_range(found_months[0], year) if found_months else _year_range(year)

    if len(found_months) >= 2:
        s, e = found_months[0], found_months[-1]
        return date(year, s, 1), date(year, e, monthrange(year, e)[1])
    if len(found_months) == 1:
        return _month_range(found_months[0], year)

    q = _extract_quarter(text)
    if q:
        return _quarter_range(q[0], q[1], year)

    # Named half/period patterns — checked before falling back to full year
    # "first half", "H1", "jan to jun" style (no explicit month tokens)
    if any(p in text for p in ["first half", "h1", "jan-jun", "jan to jun"]):
        return date(year, 1, 1), date(year, 6, 30)
    if any(p in text for p in ["second half", "h2", "jul-dec", "jul to dec"]):
        return date(year, 7, 1), date(year, 12, 31)

    tokens = text.replace(",", "").split()
    nums   = [t for t in tokens if t.isdigit()]
    if len(nums) >= 3:
        return date(int(nums[1]), 1, int(nums[0])), date(int(nums[1]), 12, int(nums[2]))

    return _year_range(year)


def parse_comparison_date_ranges(
    raw_text: str,
) -> tuple[tuple[date, date], tuple[date, date]]:
    """
    Parse a comparison query into two date ranges.

    Handles: X vs Y  |  compare … and …  |  from X to Y  (generalised)
    """
    text = raw_text.lower().replace("versus", "vs")
    years       = _extract_all_years(text)
    global_year = years[-1] if years else None

    # ── Split on separator ─────────────────────────────────────────────────
    left: str
    right: str

    if " vs " in text:
        left, right = text.split(" vs ", 1)

    elif "compare" in text and " and " in text:
        left, right = text.split(" and ", 1)
        for kw in ["compare revenue in", "compare sales in", "compare in", "compare"]:
            if left.startswith(kw):
                left = left[len(kw):].strip()
                break

    else:
        # "from <period> to <period>" — lazy match so "to" doesn't consume
        # the year that belongs to the right-hand period
        from_to = re.search(r'\bfrom\b\s+(.+?)\s+\bto\b\s+(.+)', text)
        if from_to:
            left  = from_to.group(1).strip()
            right = from_to.group(2).strip()
        else:
            raise ValueError(
                "No comparison separator found. "
                "Use 'vs', 'versus', 'compare … and …', or 'from … to …'"
            )

    # ── Year-vs-year shortcut ──────────────────────────────────────────────
    ly, ry = _extract_all_years(left), _extract_all_years(right)
    if (ly and not _extract_months_ordered(left) and not _extract_quarter(left) and
            ry and not _extract_months_ordered(right) and not _extract_quarter(right)):
        return _year_range(ly[0]), _year_range(ry[0])

    try:
        r1 = _parse_half(left, global_year)
    except ValueError as exc:
        raise ValueError(f"Could not parse left period '{left.strip()}': {exc}") from exc

    try:
        r2 = _parse_half(right, global_year)
    except ValueError as exc:
        raise ValueError(f"Could not parse right period '{right.strip()}': {exc}") from exc

    return r1, r2


def parse_both_date_ranges(
    raw_text: str,
) -> tuple[tuple[date, date], tuple[date, date]]:
    """
    Parse a "both X and Y" query into two separate date ranges.
    E.g. "ordered in both January and March 2024"
         → (Jan range, Mar range)
    """
    text        = raw_text.lower()
    years       = _extract_all_years(text)
    global_year = years[-1] if years else None

    if global_year is None:
        raise ValueError(f"Year not found in: '{raw_text}'")

    both_m = re.search(
        r'\bboth\b\s+(.+?)\s+\band\b\s+(.+?)(?:\s+\d{4}|\s*[?.]?\s*$)',
        text,
    )
    if both_m:
        l, r = both_m.group(1).strip(), both_m.group(2).strip()
        try:
            return _parse_half(l, global_year), _parse_half(r, global_year)
        except ValueError:
            pass

    months = _extract_months_ordered(text)
    if len(months) >= 2:
        return _month_range(months[0], global_year), _month_range(months[1], global_year)

    quarters = _extract_all_quarters(text)
    if len(quarters) >= 2:
        return (
            _quarter_range(quarters[0][0], quarters[0][1], global_year),
            _quarter_range(quarters[1][0], quarters[1][1], global_year),
        )

    raise ValueError(
        f"Could not find two distinct time periods in: '{raw_text}'. "
        "Try: 'ordered in both January and March 2024'"
    )