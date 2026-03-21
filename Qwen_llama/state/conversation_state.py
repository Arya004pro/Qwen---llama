import re

# Month keyword set for quick lookup
_MONTH_WORDS = {
    "jan", "january", "feb", "february", "mar", "march",
    "apr", "april", "may", "jun", "june", "jul", "july",
    "aug", "august", "sep", "september", "oct", "october",
    "nov", "november", "dec", "december"
}

class ConversationState:
    def __init__(self):
        self.entity = None
        self.metric = None
        self.time_range = None
        self.raw_time_text = None
        self.ranking = None
        self.top_n = 5
        self.is_comparison = False

    def normalize(self, text):
        text = text.lower().strip()
        replacements = {
            "revnue": "revenue",
            "prodcts": "products",
            "qty": "quantity",
            "hw mch": "how much",
            "versus": "vs",
        }
        for k, v in replacements.items():
            text = text.replace(k, v)
        return text

    def _detect_comparison(self, t):
        """
        Return True when text looks like a cross-period comparison.
        Handles:
          * "compare revenue in January and March 2024"
          * "compare revenue in march vs april 2024"
          * "revenue in jan vs march 2024"
        """
        # "compare ... and ..." where both sides have a month
        if "compare" in t and " and " in t:
            parts = t.split(" and ", 1)
            left_has_month  = any(m in parts[0] for m in _MONTH_WORDS)
            right_has_month = any(m in parts[1] for m in _MONTH_WORDS)
            if left_has_month and right_has_month:
                return True

        # "vs" (already normalised from "versus")
        if " vs " in t:
            parts = t.split(" vs ", 1)
            left_has_month  = any(m in parts[0] for m in _MONTH_WORDS)
            right_has_month = any(m in parts[1] for m in _MONTH_WORDS)
            if left_has_month and right_has_month:
                return True

        # generic "compare" keyword with at least two distinct months anywhere
        if "compare" in t or "comparison" in t:
            months_found = [m for m in _MONTH_WORDS if m in t]
            if len(months_found) >= 2:
                return True

        return False

    def update_from_user(self, text):
        # Hard reset every turn
        self.top_n = 5
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

        # COMPARISON INTENT — detect before ranking so defaults are set correctly
        if self._detect_comparison(t):
            self.is_comparison = True
            self.raw_time_text = text
            # When no entity was stated, default to "product"
            if self.entity is None:
                self.entity = "product"
            # Aggregate is a clean default for comparisons (can still be overridden by "top N")
            if self.ranking is None:
                self.ranking = "aggregate"

        # AGGREGATE INTENT
        if any(x in t for x in ["how much", "total", "overall", "sum"]):
            self.ranking = "aggregate"

        # BOTTOM N
        if any(x in t for x in ["bottom", "worst", "lowest", "least", "low performing", "underperform"]):
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
        if any(m in t for m in _MONTH_WORDS):
            self.time_range = "custom_range"
            if not self.raw_time_text:
                self.raw_time_text = text

    def is_complete(self):
        return bool(self.entity and self.metric and self.time_range)