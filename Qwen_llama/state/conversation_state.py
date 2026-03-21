import re

_MONTH_WORDS = {
    "jan", "january", "feb", "february", "mar", "march",
    "apr", "april", "may", "jun", "june", "jul", "july",
    "aug", "august", "sep", "september", "oct", "october",
    "nov", "november", "dec", "december",
}

_QUARTER_WORDS = {"q1", "q2", "q3", "q4"}

_YEAR_RE = re.compile(r"\b(20\d{2})\b")


def _has_month(text):   return any(m in text for m in _MONTH_WORDS)
def _has_quarter(text): return any(q in text for q in _QUARTER_WORDS)
def _has_year(text):    return bool(_YEAR_RE.search(text))
def _count_years(text): return len(_YEAR_RE.findall(text))


class ConversationState:
    def __init__(self):
        self.entity        = None
        self.metric        = None
        self.time_range    = None
        self.raw_time_text = None
        self.ranking       = None
        self.top_n         = 5
        self.is_comparison = False

    def normalize(self, text):
        text = text.lower().strip()
        replacements = {
            "revnue":  "revenue",
            "prodcts": "products",
            "qty":     "quantity",
            "hw mch":  "how much",
            "versus":  "vs",
        }
        for k, v in replacements.items():
            text = text.replace(k, v)
        return text

    def _detect_comparison(self, t):
        """
        Detect cross-period comparisons for:
          • month vs month    "march vs april 2024"
          • quarter vs quarter "q1 vs q2 2024"
          • year vs year       "2023 vs 2024"
          • month-range vs month-range  "jan to mar vs apr to jun 2024"
          • compare … and …   "compare revenue in january and march 2024"
        """
        def _both_sides_have_period(sep):
            parts = t.split(sep, 1)
            if len(parts) < 2:
                return False
            left, right = parts
            left_ok  = _has_month(left)  or _has_quarter(left)  or _has_year(left)
            right_ok = _has_month(right) or _has_quarter(right) or _has_year(right)
            return left_ok and right_ok

        # "X vs Y"
        if " vs " in t and _both_sides_have_period(" vs "):
            return True

        # "compare ... and ..." with a period on each side of "and"
        if "compare" in t and " and " in t:
            parts = t.split(" and ", 1)
            left, right = parts
            left_ok  = _has_month(left)  or _has_quarter(left)
            right_ok = _has_month(right) or _has_quarter(right)
            # also catch "compare 2023 and 2024" (year-only on each side)
            if not left_ok and not right_ok:
                left_ok  = _has_year(left)
                right_ok = _has_year(right)
            if left_ok and right_ok:
                return True

        # "compare/comparison" keyword + two distinct time indicators anywhere
        if "compare" in t or "comparison" in t:
            months   = [m for m in _MONTH_WORDS  if m in t]
            quarters = [q for q in _QUARTER_WORDS if q in t]
            yr_count = _count_years(t)
            if len(months) >= 2 or len(quarters) >= 2 or yr_count >= 2:
                return True

        return False

    def update_from_user(self, text):
        self.top_n         = 5
        self.is_comparison = False

        t = self.normalize(text)

        # ENTITY
        if "product" in t:
            self.entity = "product"
        elif "category" in t:
            self.entity = "category"
        elif "customer" in t:
            self.entity = "customer"
        elif "city" in t:
            self.entity = "city"

        # METRIC
        if "revenue" in t or "sales" in t:
            self.metric = "revenue"
        elif "quantity" in t or "units" in t:
            self.metric = "quantity"

        # COMPARISON INTENT
        if self._detect_comparison(t):
            self.is_comparison = True
            self.raw_time_text = text
            if self.entity is None:
                self.entity = "product"
            if self.ranking is None:
                self.ranking = "aggregate"

        # AGGREGATE
        if any(x in t for x in ["how much", "total", "overall", "sum"]):
            self.ranking = "aggregate"

        # BOTTOM N
        if any(x in t for x in ["bottom", "worst", "lowest", "least",
                                  "low performing", "underperform"]):
            self.ranking = "bottom"
            m = re.search(r"bottom\s+(\d+)", t)
            if m:
                self.top_n = int(m.group(1))

        # TOP N
        if "top" in t and self.ranking != "bottom":
            self.ranking = "top"
            m = re.search(r"top\s+(\d+)", t)
            if m:
                self.top_n = int(m.group(1))

        # TIME
        if _has_month(t) or _has_quarter(t) or _has_year(t):
            self.time_range = "custom_range"
            if not self.raw_time_text:
                self.raw_time_text = text

    def is_complete(self):
        return bool(self.entity and self.metric and self.time_range)