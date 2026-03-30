"""state/conversation_state.py

Parses a natural-language analytics query into structured intent fields.
All entity/metric detection is SCHEMA-DRIVEN — columns are read from
the live DuckDB database at runtime.  No table names, column names,
or domain-specific keywords are hardcoded.

Fields
------
entity            detected dimension column (from live schema)
metric            detected metric column (from live schema)
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
from functools import lru_cache

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


# ─── Schema-driven keyword detection ────────────────────────────────────────

_TEXT_TYPES = ("VARCHAR", "CHAR", "TEXT", "STRING")
_NUM_TYPES  = ("INT", "BIGINT", "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC", "REAL")
_DATE_HINTS = ("date", "time", "created", "updated", "timestamp", "at")

_MONETARY_KEYWORDS = (
    "revenue", "sales", "amount", "total", "fare", "earning", "price",
    "cost", "payment", "final", "profit", "income",
)
_QUANTITY_KEYWORDS = ("quantity", "qty", "units", "volume", "pieces", "items")
_COUNT_KEYWORDS    = ("count", "how many", "number of", "total number")


def _load_schema_maps() -> tuple[list[tuple[str, str]], list[tuple[str, str]], list[str]]:
    """
    Read live schema from DuckDB and return:
      entity_map  : [(keyword, column_name), ...]
      metric_map  : [(keyword, column_name), ...]
      entity_labels : [plural_display_name, ...]
    """
    entity_map: list[tuple[str, str]] = []
    metric_map: list[tuple[str, str]] = []
    entity_labels: list[str] = []

    try:
        from db.duckdb_connection import get_read_connection
        conn = get_read_connection()
        try:
            tables = [
                r[0] for r in conn.execute(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema='main' AND table_name NOT LIKE '_raw_%' "
                    "ORDER BY table_name"
                ).fetchall()
            ]
            for table in tables:
                cols = conn.execute(f'DESCRIBE "{table}"').fetchall()
                for col, dtype, *_ in cols:
                    d = dtype.upper() if isinstance(dtype, str) else str(dtype).upper()
                    cname = col.lower()

                    # Skip date columns
                    if any(k in cname for k in _DATE_HINTS):
                        continue

                    # Entity columns (text, non-ID)
                    if any(t in d for t in _TEXT_TYPES):
                        if cname.endswith("_id") or cname.endswith("_key") or cname.endswith("_uuid"):
                            continue
                        base = cname
                        for sfx in ("_name", "_title", "_label", "_type", "_mode"):
                            if base.endswith(sfx):
                                base = base[:-len(sfx)]
                                break
                        bw = base.replace("_", " ").strip()
                        if bw:
                            entity_map.append((bw, col))
                            # Also add plural
                            if not bw.endswith("s"):
                                entity_map.append((bw + "s", col))
                            entity_labels.append(bw + "s" if not bw.endswith("s") else bw)
                        if cname.replace("_", " ") != bw:
                            entity_map.append((cname.replace("_", " "), col))

                    # Metric columns (numeric, non-ID)
                    elif any(t in d for t in _NUM_TYPES):
                        if cname.endswith("_id") or cname.endswith("_key"):
                            continue
                        bw = cname.replace("_", " ")
                        metric_map.append((bw, col))
                        # Add revenue/sales aliases for monetary columns
                        if any(k in cname for k in ("price", "fare", "amount", "total", "final", "revenue", "earning", "cost", "profit")):
                            for alias in ("revenue", "sales", "earnings", "income", "money", "spending"):
                                metric_map.append((alias, col))
                        # Add quantity aliases
                        if any(k in cname for k in ("quantity", "qty", "units", "volume")):
                            for alias in ("quantity", "units", "items", "pieces", "volume"):
                                metric_map.append((alias, col))
        finally:
            conn.close()
    except Exception:
        pass

    # Deduplicate preserving order
    seen_e: set = set()
    seen_m: set = set()
    entity_map = [(k, v) for k, v in entity_map if (k, v) not in seen_e and not seen_e.add((k, v))]
    metric_map = [(k, v) for k, v in metric_map if (k, v) not in seen_m and not seen_m.add((k, v))]

    return entity_map, metric_map, list(dict.fromkeys(entity_labels))


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
            "revnue": "revenue", "qty": "quantity",
            "versus": "vs",
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
        _sup = {"highest", "most", "best", "largest", "biggest",
                "lowest", "least", "worst", "smallest", "maximum", "minimum"}
        _growth = {"growth", "grew", "increase", "decrease", "change", "growth rate"}
        return any(s in t for s in _sup) and any(g in t for g in _growth)

    def _detect_zero_filter(self, t: str) -> bool:
        _kw = {
            "zero sales", "no sales", "zero quantity", "not sold",
            "zero revenue", "no orders", "never sold",
            "without sales", "without orders", "no transactions",
            "zero activity", "no activity", "inactive",
        }
        return any(kw in t for kw in _kw)

    def _parse_threshold(self, t: str):
        # Percentage first (most specific signal)
        m = _PCT_RE.search(t)
        if m:
            return float(m.group(1)), "percentage"

        # Explicit unit quantity (units, orders, pieces…)
        m = _ABS_QTY_RE.search(t)
        if m:
            return float(m.group(1)), "absolute"

        # Generic number next to threshold keyword
        _kw_pos = None
        for kw in ["exceed", "above", "more than", "greater than",
                   "at least", "over", "below", "fewer than", "under", "less than"]:
            idx = t.find(kw)
            if idx != -1:
                _kw_pos = idx
                break

        if _kw_pos is not None:
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

        # ── entity (SCHEMA-DRIVEN) ────────────────────────────────────────────
        entity_map, metric_map, entity_labels = _load_schema_maps()

        # Sort by keyword length descending so longer matches win
        sorted_entities = sorted(entity_map, key=lambda x: len(x[0]), reverse=True)
        sorted_metrics  = sorted(metric_map, key=lambda x: len(x[0]), reverse=True)

        for kw, col in sorted_entities:
            if kw in t:
                self.entity = col
                break

        # ── metric (SCHEMA-DRIVEN) ────────────────────────────────────────────
        # First check for explicit revenue/sales/quantity/count keywords
        if any(w in t for w in _MONETARY_KEYWORDS):
            for kw, col in sorted_metrics:
                if kw in t:
                    self.metric = col
                    break
            if self.metric is None:
                # Use first monetary metric from schema
                for kw, col in sorted_metrics:
                    if any(k in kw for k in ("revenue", "sales", "amount", "price", "fare", "total")):
                        self.metric = col
                        break
        elif any(w in t for w in _QUANTITY_KEYWORDS):
            for kw, col in sorted_metrics:
                if any(k in kw for k in _QUANTITY_KEYWORDS):
                    self.metric = col
                    break
        elif any(w in t for w in _COUNT_KEYWORDS):
            self.metric = "count"
        else:
            # Try matching any metric keyword in the query
            for kw, col in sorted_metrics:
                if kw in t:
                    self.metric = col
                    break

        # Smart default: if entity+ranking known but no metric signal,
        # default to the first monetary metric found in schema
        if self.metric is None and self.entity is not None:
            for kw, col in sorted_metrics:
                if any(k in kw for k in ("revenue", "sales", "amount", "price", "fare", "total", "earning")):
                    self.metric = col
                    break
            # If still None, use first numeric metric
            if self.metric is None and sorted_metrics:
                self.metric = sorted_metrics[0][1]

        # ── zero filter (early exit) ──────────────────────────────────────────
        if self._detect_zero_filter(t):
            self.ranking       = "zero_filter"
            self.time_range    = "custom_range"
            self.raw_time_text = text
            return

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

            if self._detect_growth_ranking(t):
                self.is_growth_ranking = True
                self.ranking           = "top_growth"
            else:
                if self.ranking is None:
                    _by_entity = bool(re.search(r"\bby\s+\w+", t) or re.search(r"\bper\s+\w+", t))
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
                self.top_n   = 1

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

    def get_entity_display_label(self) -> str:
        """Return a human-readable plural label for the current entity."""
        if not self.entity:
            return "items"
        base = self.entity.lower()
        for sfx in ("_name", "_title", "_label"):
            if base.endswith(sfx):
                base = base[:-len(sfx)]
                break
        base = base.replace("_", " ")
        if not base.endswith("s"):
            base += "s"
        return base

    def merge_clarification(self, clarification_text: str) -> None:
        t = self.normalize(clarification_text)

        entity_map, metric_map, _ = _load_schema_maps()
        sorted_entities = sorted(entity_map, key=lambda x: len(x[0]), reverse=True)
        sorted_metrics  = sorted(metric_map, key=lambda x: len(x[0]), reverse=True)

        if self.entity is None:
            for kw, col in sorted_entities:
                if kw in t:
                    self.entity = col
                    break

        if self.metric is None:
            for kw, col in sorted_metrics:
                if kw in t:
                    self.metric = col
                    break

        if self.time_range is None and (_has_month(t) or _has_quarter(t) or _has_year(t)):
            self.time_range    = "custom_range"
            self.raw_time_text = clarification_text