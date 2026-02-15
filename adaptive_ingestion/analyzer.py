from collections import defaultdict
import re

class FieldStats:
    def __init__(self):
        self.total_records = 0
        self.field_presence = defaultdict(int)
        self.type_counts = defaultdict(lambda: defaultdict(int))
        self.unique_values = defaultdict(set)

    def _infer_type(self, value):
        # Infer a semantic type (not just Python type).
        
        if value is None:
            return "null"

        if isinstance(value, bool):
            return "bool"

        if isinstance(value, int):
            return "int"

        if isinstance(value, float):
            return "float"

        if isinstance(value, list):
            return "list"

        if isinstance(value, dict):
            return "dict"

        if isinstance(value, str):
            # IP address detection
            ip_pattern = r"^(\d{1,3}\.){3}\d{1,3}$"
            if re.match(ip_pattern, value):
                return "ip_string"

            # Integer-like string
            if value.isdigit():
                return "numeric_string_int"

            # Float-like string
            try:
                float(value)
                return "numeric_string_float"
            except ValueError:
                return "string"

        return "unknown"


    def update(self, record: dict):
        # Update statistics using a normalized record.
        
        self.total_records += 1

        for field, value in record.items():
            self.field_presence[field] += 1

            inferred_type = self._infer_type(value)
            self.type_counts[field][inferred_type] += 1

            # Track uniqueness conservatively (limit growth)
            if isinstance(value, (int, float, str, bool)):
                if len(self.unique_values[field]) < 1000:
                    self.unique_values[field].add(value)

    def summary(self):
        
        # Return a readable summary (for debugging).
        result = {}
        for field in self.field_presence:
            result[field] = {
                "presence_ratio": self.field_presence[field] / self.total_records,
                "type_distribution": dict(self.type_counts[field]),
                "unique_count": len(self.unique_values[field])
            }
        return result
