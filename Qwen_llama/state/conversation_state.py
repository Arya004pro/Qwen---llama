"""state/conversation_state.py

Parses a natural-language analytics query into structured intent fields.
All detection is pattern-based and fully generalised — no entity names,
date values, or thresholds are hardcoded.

Fields
------
entity            product | customer | city | category
metric            revenue | quantity
time_range        "custom_range" when a date token is found
raw_time_text     original text kept for date_parser
ranking           top | bottom | aggregate | threshold | zero_filter | top_growth
top_n             N for top/bottom/top_growth
is_comparison     two periods compared (vs / from…to / compare…and / growth)
is_intersection   "both X and Y" — entities present in BOTH periods
is_growth_ranking comparison + superlative → rank entities BY delta
threshold_value   numeric value for absolute threshold (e.g. 500)
threshold_type    "absolute" | "percentage" | None
"""

import re

# ─── token sets ──────────────────────────────────────────────────────────────

_MONTH_WORDS = {
    "jan", "january", "feb", "february", "mar", "march",
    "apr", "april", "may", "jun", "june", "jul", "july",
    "aug", "august", "sep", "september", "oct", "october",
    "nov", "november", "dec", "december",
}
_QUARTER_WORDS = {"q1", "q2", "q3", "q4"}
_YEAR_RE       = re.compile(r"\b(20\d{2})\b")

# Generalised numeric patterns — no hardcoded values
_ABS_QTY_RE = re.compile(
    r"\b(\d+(?:\.\d+)?)\s*(?:units?|orders?|times?|pieces?|items?)\b",
    re.IGNORECASE,
)
_PCT_RE = re.compile(
    r"\b(\d+(?:\.\d+)?)\s*(?:%|percent(?:age)?|proportion)(?:\b|\s|$|[,.])",
    re.IGNORECASE,
)
# Generic integer/float — used only when threshold keyword is nearby
_NUM_RE = re.compile(r"\b(\d[\d,]*(?:\.\d+)?)\b")


def _has_month(t):   return any(m in t for m in _MONTH_WORDS)
def _has_quarter(t): return any(q in t for q in _QUARTER_WORDS)
def _has_year(t):    return bool(_YEAR_RE.search(t))
def _count_years(t): return len(_YEAR_RE.findall(t))


# ─── state class ─────────────────────────────────────────────────────────────

class ConversationState:

    def __init__(self):
        self.entity            = None
        self.metric            = None
        self.time_range        = None
        self.raw_time_text     = None
        self.ranking           = None
        self.top_n             = 5
        self.is_comparison     = False
        self.is_intersection   = False
        self.is_growth_ranking = False
        self.threshold_value   = None   # e.g. 500.0 or 10.0 (pct)
        self.threshold_type    = None   # "absolute" | "percentage"

    # ── normalisation ─────────────────────────────────────────────────────────

    def normalize(self, text: str) -> str:
        t = text.lower().strip()
        for src, dst in {
            "revnue": "revenue", "prodcts": "products",
            "qty": "quantity",   "versus": "vs",
        }.items():
            t = t.replace(src, dst)
        return t

    # ── sub-detectors (all pattern-based, no hardcoding) ──────────────────────

    def _detect_comparison(self, t: str) -> bool:
        def _both(sep):
            parts = t.split(sep, 1)
            if len(parts) < 2:
                return False
            l, r = parts
            return ((_has_month(l) or _has_quarter(l) or _has_year(l)) and
                    (_has_month(r) or _has_quarter(r) or _has_year(r)))

        if " vs " in t and _both(" vs "):
            return True

        if "compare" in t and " and " in t:
            l, r = t.split(" and ", 1)
            if (_has_month(l) or _has_quarter(l)) and (_has_month(r) or _has_quarter(r)):
                return True
            if _has_year(l) and _has_year(r):
                return True

        _growth_kw = {
            "growth", "grew", "increase", "increased", "decrease", "decreased",
            "change", "changed", "differ", "difference", "compare", "comparison", "trend",
        }
        if any(kw in t for kw in _growth_kw):
            months   = [m for m in _MONTH_WORDS  if m in t]
            quarters = [q for q in _QUARTER_WORDS if q in t]
            if len(months) >= 2 or len(quarters) >= 2 or _count_years(t) >= 2:
                return True

        if re.search(r"\bfrom\b.{1,30}?\bto\b", t, re.IGNORECASE):
            months   = [m for m in _MONTH_WORDS  if m in t]
            quarters = [q for q in _QUARTER_WORDS if q in t]
            if len(months) >= 2 or len(quarters) >= 2 or _count_years(t) >= 2:
                return True

        if "compare" in t or "comparison" in t:
            months   = [m for m in _MONTH_WORDS  if m in t]
            quarters = [q for q in _QUARTER_WORDS if q in t]
            if len(months) >= 2 or len(quarters) >= 2 or _count_years(t) >= 2:
                return True

        return False

    def _detect_intersection(self, t: str) -> bool:
        """'both X and Y' — entities present in BOTH periods."""
        if "both" not in t or " and " not in t:
            return False
        months   = [m for m in _MONTH_WORDS  if m in t]
        quarters = [q for q in _QUARTER_WORDS if q in t]
        return len(months) >= 2 or len(quarters) >= 2

    def _detect_growth_ranking(self, t: str) -> bool:
        """
        Comparison query whose goal is to RANK entities by their growth delta.
        e.g. "Which city had the HIGHEST revenue growth from Q1 to Q2"
        """
        _sup = {"highest", "most", "best", "largest", "biggest",
                "lowest", "least", "worst", "smallest", "maximum", "minimum"}
        _growth = {"growth", "grew", "increase", "decrease", "change", "growth rate"}
        return any(s in t for s in _sup) and any(g in t for g in _growth)

    def _detect_zero_filter(self, t: str) -> bool:
        _kw = {
            "zero sales", "no sales", "zero quantity", "not sold",
            "zero revenue", "no orders", "never sold",
            "without sales", "without orders", "no transactions",
        }
        return any(kw in t for kw in _kw)

    def _parse_threshold(self, t: str):
        """
        Return (value, type) for the numeric threshold in the query.
        Generalised — works for any number the user specifies.
        """
        # Percentage first (most specific signal)
        m = _PCT_RE.search(t)
        if m:
            return float(m.group(1)), "percentage"

        # Explicit unit quantity (units, orders, pieces…)
        m = _ABS_QTY_RE.search(t)
        if m:
            return float(m.group(1)), "absolute"

        # Generic number next to threshold keyword (currency amounts etc.)
        _kw_pos = None
        for kw in ["exceed", "above", "more than", "greater than",
                   "at least", "over", "below", "fewer than", "under", "less than"]:
            idx = t.find(kw)
            if idx != -1:
                _kw_pos = idx
                break

        if _kw_pos is not None:
            # Scan for the first number after (or shortly before) the keyword
            segment = t[max(0, _kw_pos - 10): _kw_pos + 40]
            for m in _NUM_RE.finditer(segment):
                val = float(m.group(1).replace(",", ""))
                if val > 0:
                    return val, "absolute"

        return None, None

    # ── main entry point ──────────────────────────────────────────────────────

    def update_from_user(self, text: str) -> None:
        # Reset per-query flags
        self.top_n             = 5
        self.is_comparison     = False
        self.is_intersection   = False
        self.is_growth_ranking = False
        self.threshold_value   = None
        self.threshold_type    = None

        t = self.normalize(text)

        # ── entity ────────────────────────────────────────────────────────────
        if "product" in t:
            self.entity = "product"
        elif "category" in t or "categor" in t:
            self.entity = "category"
        elif "customer" in t:
            self.entity = "customer"
        elif "city" in t or "cities" in t:
            self.entity = "city"

        # ── metric ────────────────────────────────────────────────────────────
        if any(w in t for w in ["revenue", "sales", "growth", "earning", "amount",
                                   "spending", "spent", "purchase", "purchasing",
                                   "best selling", "top selling"]):
            self.metric = "revenue"
        elif any(w in t for w in ["quantity", "units", "pieces",
                                   "how many", "count of", "number of"]):
            self.metric = "quantity"

        # Smart default: if entity+ranking known but no metric signal,
        # default to revenue (the overwhelming majority of analytics queries).
        # Quantity is always explicit ("units sold", "quantity", "pieces").
        # "ordered", "placed orders", "purchases" without a metric word → revenue.
        if self.metric is None and self.entity is not None:
            self.metric = "revenue"

        # ── zero filter (early exit) ──────────────────────────────────────────
        if self._detect_zero_filter(t):
            self.ranking       = "zero_filter"
            self.time_range    = "custom_range"
            self.raw_time_text = text
            return   # no further ranking detection needed

        # ── threshold ─────────────────────────────────────────────────────────
        _threshold_kw = {
            "more than", "less than", "exceed", "exceeds", "exceeded",
            "above", "below", "at least", "at most",
            "greater than", "fewer than", "over", "under",
        }
        _has_threshold_kw = any(kw in t for kw in _threshold_kw)
        _has_pct          = bool(_PCT_RE.search(t))
        _has_qty_unit     = bool(_ABS_QTY_RE.search(t))
        _has_large_num    = False
        if _has_threshold_kw:
            for m in _NUM_RE.finditer(t):
                if float(m.group(1).replace(",", "")) > 99:
                    _has_large_num = True
                    break

        if _has_threshold_kw and (_has_pct or _has_qty_unit or _has_large_num):
            self.ranking = "threshold"
            self.threshold_value, self.threshold_type = self._parse_threshold(t)

        # ── comparison (before intersection) ──────────────────────────────────
        elif self._detect_comparison(t):
            self.is_comparison = True
            self.raw_time_text = text
            if self.entity is None:
                self.entity = "product"

            if self._detect_growth_ranking(t):
                self.is_growth_ranking = True
                self.ranking           = "top_growth"
            else:
                if self.ranking is None:
                    # "by category/product/city/customer" in the query means
                    # the user wants a per-entity breakdown, not a single total
                    _by_entity = any(p in t for p in [
                        "by category", "by product", "by city",
                        "by customer", "per category", "per product",
                        "per city", "per customer",
                    ])
                    self.ranking = "top" if _by_entity else "aggregate"

        # ── intersection ──────────────────────────────────────────────────────
        elif self._detect_intersection(t):
            self.is_intersection = True
            self.time_range      = "custom_range"
            self.raw_time_text   = text

        # ── aggregate ─────────────────────────────────────────────────────────
        if self.ranking not in ("threshold", "zero_filter") and not self.is_growth_ranking:
            if any(x in t for x in ["how much", "total", "overall", "sum"]):
                self.ranking = "aggregate"

        # ── bottom N ──────────────────────────────────────────────────────────
        if any(x in t for x in ["bottom", "worst", "lowest", "least",
                                  "low performing", "underperform"]):
            if not self.is_growth_ranking and self.ranking not in ("threshold", "zero_filter"):
                self.ranking = "bottom"
            m = re.search(r"bottom\s+(\d+)", t)
            if m:
                self.top_n = int(m.group(1))

        # ── top N ─────────────────────────────────────────────────────────────
        if "top" in t and self.ranking not in ("bottom", "threshold", "zero_filter"):
            if not self.is_growth_ranking:
                self.ranking = "top"
            m = re.search(r"top\s+(\d+)", t)
            if m:
                self.top_n = int(m.group(1))

        # ── superlatives (single-result) ──────────────────────────────────────
        if any(x in t for x in ["highest", "most", "best", "largest", "biggest",
                                  "maximum"]):
            if self.ranking is None:
                self.ranking = "top"
                self.top_n   = 1
            elif self.is_growth_ranking:
                self.top_n   = 1   # show the ONE entity with biggest growth

        if any(x in t for x in ["lowest", "worst", "smallest", "minimum"]):
            if self.ranking is None:
                self.ranking = "bottom"
                self.top_n   = 1
            elif self.is_growth_ranking:
                self.top_n   = 1

        # ── time ──────────────────────────────────────────────────────────────
        if _has_month(t) or _has_quarter(t) or _has_year(t):
            self.time_range = "custom_range"
            if not self.raw_time_text:
                self.raw_time_text = text

    def is_complete(self) -> bool:
        return bool(self.entity and self.metric and self.time_range)

    def merge_clarification(self, clarification_text: str) -> None:
        t = self.normalize(clarification_text)
        if self.entity is None:
            if "product"  in t:                    self.entity = "product"
            elif "category" in t or "categor" in t: self.entity = "category"
            elif "customer" in t:                   self.entity = "customer"
            elif "city" in t or "cities" in t:      self.entity = "city"
        if self.metric is None:
            if any(w in t for w in ["revenue", "sales"]):    self.metric = "revenue"
            elif any(w in t for w in ["quantity", "units"]): self.metric = "quantity"
        if self.time_range is None and (_has_month(t) or _has_quarter(t) or _has_year(t)):
            self.time_range    = "custom_range"
            self.raw_time_text = clarification_text