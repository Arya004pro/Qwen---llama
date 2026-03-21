from datetime import date
from calendar import monthrange

MONTHS = {
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
    "dec": 12, "december": 12
}


def _extract_year(text):
    """Return the first 4-digit year found in text, or None."""
    for token in text.split():
        clean = token.strip(",.?:")
        if clean.isdigit() and len(clean) == 4:
            return int(clean)
    return None


def _extract_months_ordered(text):
    """Return list of month numbers in the order they appear in text."""
    found = []
    for word in text.split():
        word = word.strip(",.?:")
        if word in MONTHS:
            found.append(MONTHS[word])
    return found


def parse_date_range(time_range, raw_text):
    """Parse a single date range from raw query text."""
    text = raw_text.lower()

    year = _extract_year(text)
    if year is None:
        raise ValueError("Year not found")

    found_months = _extract_months_ordered(text)

    # CASE 1: Month range "jan to jun" – only when not a comparison query
    is_comparison = " vs " in text or "versus" in text or (
        "compare" in text and " and " in text
    )
    if len(found_months) >= 2 and not is_comparison:
        start_month = found_months[0]
        end_month   = found_months[-1]
        start_date  = date(year, start_month, 1)
        end_date    = date(year, end_month, monthrange(year, end_month)[1])
        return start_date, end_date

    # CASE 2: Single month "march 2024"
    if len(found_months) >= 1:
        month      = found_months[0]
        start_date = date(year, month, 1)
        end_date   = date(year, month, monthrange(year, month)[1])
        return start_date, end_date

    # CASE 3: Numeric fallback
    tokens = text.replace(",", "").split()
    nums   = [t for t in tokens if t.isdigit()]
    if len(nums) >= 3:
        d1, y1, d2 = int(nums[0]), int(nums[1]), int(nums[2])
        return date(y1, 1, d1), date(y1, 12, d2)

    # CASE 4: Year only
    if year and not found_months:
        return date(year, 1, 1), date(year, 12, 31)

    raise ValueError("Unable to parse date range")


def parse_comparison_date_ranges(raw_text):
    """
    Parse a comparison query into two (start, end) date range tuples.

    Supported formats:
      "compare revenue in January and March 2024"
      "March vs April 2024"
      "March 2024 vs April 2024"
      "revenue in jan vs mar 2024"

    Returns:
        ((start1, end1), (start2, end2))
    Raises:
        ValueError when two distinct periods cannot be extracted.
    """
    text = raw_text.lower()

    # Normalise "versus" → "vs" for consistent splitting
    text = text.replace("versus", "vs")

    year = _extract_year(text)
    if year is None:
        raise ValueError("Year not found in comparison query")

    # ── Try "vs" separator first ────────────────────────────────────────────
    if " vs " in text:
        left, right = text.split(" vs ", 1)
        m1_list = _extract_months_ordered(left)
        m2_list = _extract_months_ordered(right)
        m1 = m1_list[0] if m1_list else None
        m2 = m2_list[0] if m2_list else None
        y1 = _extract_year(left) or year
        y2 = _extract_year(right) or year

        if m1 and m2:
            s1 = date(y1, m1, 1);  e1 = date(y1, m1, monthrange(y1, m1)[1])
            s2 = date(y2, m2, 1);  e2 = date(y2, m2, monthrange(y2, m2)[1])
            return (s1, e1), (s2, e2)

    # ── Try "compare ... and ..." separator ─────────────────────────────────
    if " and " in text:
        # Split only on the first "and" that separates two month references
        parts = text.split(" and ", 1)
        m1_list = _extract_months_ordered(parts[0])
        m2_list = _extract_months_ordered(parts[1])
        m1 = m1_list[-1] if m1_list else None   # last month mentioned before "and"
        m2 = m2_list[0]  if m2_list else None   # first month mentioned after "and"
        y1 = _extract_year(parts[0]) or year
        y2 = _extract_year(parts[1]) or year

        if m1 and m2:
            s1 = date(y1, m1, 1);  e1 = date(y1, m1, monthrange(y1, m1)[1])
            s2 = date(y2, m2, 1);  e2 = date(y2, m2, monthrange(y2, m2)[1])
            return (s1, e1), (s2, e2)

    raise ValueError(
        "Could not find two distinct periods in comparison query. "
        "Try: 'compare revenue in January vs March 2024'"
    )