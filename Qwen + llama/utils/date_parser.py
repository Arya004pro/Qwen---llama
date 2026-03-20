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


def parse_date_range(time_range, raw_text):
    text = raw_text.lower()

    # -------------------------------
    # Extract year (mandatory)
    # -------------------------------
    year = None
    for token in text.split():
        if token.isdigit() and len(token) == 4:
            year = int(token)
            break

    if year is None:
        raise ValueError("Year not found")

    # -------------------------------
    # Extract months in order
    # -------------------------------
    found_months = []
    for word in text.split():
        word = word.strip(",.?")
        if word in MONTHS:
            found_months.append(MONTHS[word])

    # -------------------------------
    # CASE 1: Month range (jan to jun)
    # -------------------------------
    if len(found_months) >= 2:
        start_month = found_months[0]
        end_month = found_months[-1]

        start_date = date(year, start_month, 1)
        end_date = date(
            year,
            end_month,
            monthrange(year, end_month)[1]
        )
        return start_date, end_date

    # -------------------------------
    # CASE 2: Single month (march 2024)
    # -------------------------------
    if len(found_months) == 1:
        month = found_months[0]
        start_date = date(year, month, 1)
        end_date = date(year, month, monthrange(year, month)[1])
        return start_date, end_date

    # -------------------------------
    # CASE 3: Explicit numeric dates (fallback)
    # -------------------------------
    tokens = text.replace(",", "").split()
    nums = [t for t in tokens if t.isdigit()]

    if len(nums) >= 3:
        d1, y1, d2 = int(nums[0]), int(nums[1]), int(nums[2])
        return date(y1, 1, d1), date(y1, 12, d2)

    raise ValueError("Unable to parse date range")
