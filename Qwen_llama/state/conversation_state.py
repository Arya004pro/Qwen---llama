import re

class ConversationState:
    def __init__(self):
        self.entity = None
        self.metric = None
        self.time_range = None
        self.raw_time_text = None
        self.ranking = None
        self.top_n = 5

    def normalize(self, text):
        text = text.lower().strip()
        replacements = {
            "revnue": "revenue",
            "prodcts": "products",
            "qty": "quantity",
            "hw mch": "how much"
        }
        for k, v in replacements.items():
            text = text.replace(k, v)
        return text

    def update_from_user(self, text):
        # 🔥 HARD RESET EVERY TURN
        self.top_n = 5

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
        if "revenue" in t:
            self.metric = "revenue"
        elif "quantity" in t or "units" in t:
            self.metric = "quantity"

        # AGGREGATE INTENT
        if any(x in t for x in ["how much", "total", "overall", "sum"]):
            self.ranking = "aggregate"

        # TOP N
        if "top" in t:
            self.ranking = "top"
            m = re.search(r"top\s+(\d+)", t)
            if m:
                self.top_n = int(m.group(1))

        # TIME
        if any(m in t for m in [
            "jan", "feb", "mar", "apr", "may", "jun",
            "jul", "aug", "sep", "oct", "nov", "dec"
        ]):
            self.time_range = "custom_range"
            self.raw_time_text = text

    def is_complete(self):
        return self.entity and self.metric and self.time_range
