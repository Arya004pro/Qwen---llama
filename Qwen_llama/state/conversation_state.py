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
        Detect cross-period comparisons including growth/change queries.

        Patterns caught:
          • X vs Y                 "march vs april 2024"
          • compare … and …       "compare revenue in jan and march 2024"
          • growth / change / diff "revenue growth from Q1 to Q2 2024"
          • from … to … (two periods) "from january to march vs april to june 2024"
          • 2+ distinct periods anywhere in the text
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
            if not left_ok and not right_ok:
                left_ok  = _has_year(left)
                right_ok = _has_year(right)
            if left_ok and right_ok:
                return True

        # NEW: growth / change / increase / decrease keywords + two time indicators
        # Catches: "growth from Q1 to Q2", "change between jan and march",
        #          "which city had the highest revenue growth from Q1 to Q2 2024"
        _growth_kw = {"growth", "grew", "increase", "increased", "decrease",
                      "decreased", "change", "changed", "differ", "difference",
                      "compare", "comparison", "trend"}
        if any(kw in t for kw in _growth_kw):
            months   = [m for m in _MONTH_WORDS  if m in t]
            quarters = [q for q in _QUARTER_WORDS if q in t]
            yr_count = _count_years(t)
            if len(months) >= 2 or len(quarters) >= 2 or yr_count >= 2:
                return True
            # "from Q1 to Q2" — two distinct quarter tokens (already caught above)
            # but also "from january to march" within a single year
            if len(months) >= 2 or len(quarters) >= 2:
                return True

        # NEW: "from <period> to <period>" structure (no explicit comparison keyword)
        # Catches: "from Q1 to Q2 2024", "from January to June 2024"
        from_to_re = re.compile(
            r"\bfrom\b.{1,30}?\bto\b",
            re.IGNORECASE,
        )
        if from_to_re.search(t):
            months   = [m for m in _MONTH_WORDS  if m in t]
            quarters = [q for q in _QUARTER_WORDS if q in t]
            yr_count = _count_years(t)
            if len(months) >= 2 or len(quarters) >= 2 or yr_count >= 2:
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
        elif "category" in t or "categor" in t:
            self.entity = "category"
        elif "customer" in t:
            self.entity = "customer"
        elif "city" in t or "cities" in t:
            self.entity = "city"

        # METRIC — revenue signals
        if "revenue" in t or "sales" in t or "growth" in t or "earning" in t:
            self.metric = "revenue"
        elif "quantity" in t or "units" in t or "sold" in t:
            self.metric = "quantity"

        # COMPARISON INTENT
        if self._detect_comparison(t):
            self.is_comparison = True
            self.raw_time_text = text
            if self.entity is None:
                self.entity = "product"
            if self.ranking is None:
                self.ranking = "aggregate"

        # THRESHOLD — filtered queries like "categories with more than 10% of revenue"
        # These need a HAVING clause, not a plain SUM.  Detected BEFORE aggregate
        # so they don't get collapsed into a featureless total.
        _threshold_kw = {"more than", "less than", "exceed", "exceeds",
                         "above", "below", "at least", "at most",
                         "greater than", "fewer than"}
        _has_threshold_kw = any(kw in t for kw in _threshold_kw)
        _has_pct = any(x in t for x in ["percent", "percentage", "%", "proportion"])
        _has_filter_num = bool(re.search(r"\d+\s*(%|percent|units|orders|times)", t))

        if _has_threshold_kw and (_has_pct or _has_filter_num):
            self.ranking = "threshold"

        # AGGREGATE — plain totals with no per-entity breakdown
        elif any(x in t for x in ["how much", "total", "overall", "sum"]):
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

        # Highest / lowest single result
        if any(x in t for x in ["highest", "most", "best", "largest", "biggest"]):
            if self.ranking is None:
                self.ranking = "top"
                self.top_n   = 1

        # TIME
        if _has_month(t) or _has_quarter(t) or _has_year(t):
            self.time_range = "custom_range"
            if not self.raw_time_text:
                self.raw_time_text = text

    def is_complete(self):
        return bool(self.entity and self.metric and self.time_range)

    def merge_clarification(self, clarification_text: str):
        """
        Apply a follow-up clarification answer on top of the existing state
        without resetting fields that are already known.
        Used by the Motia pipeline when routing clarification replies.
        """
        t = self.normalize(clarification_text)

        if self.entity is None:
            if "product" in t:
                self.entity = "product"
            elif "category" in t or "categor" in t:
                self.entity = "category"
            elif "customer" in t:
                self.entity = "customer"
            elif "city" in t or "cities" in t:
                self.entity = "city"

        if self.metric is None:
            if "revenue" in t or "sales" in t:
                self.metric = "revenue"
            elif "quantity" in t or "units" in t:
                self.metric = "quantity"

        if self.time_range is None:
            if _has_month(t) or _has_quarter(t) or _has_year(t):
                self.time_range    = "custom_range"
                self.raw_time_text = clarification_text