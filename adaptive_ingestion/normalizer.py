import re

def camel_to_snake(name: str) -> str:
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    s2 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1)
    return s2.lower()

def normalize_segment(segment: str) -> str:
    """
    Normalize a single path segment (no dots here).
    """
    segment = camel_to_snake(segment)
    segment = re.sub(r'[^a-z0-9_]', '', segment)
    return segment

def flatten_dict(d: dict, parent_key: str = "", sep: str = ".") -> dict:
    items = {}
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.update(flatten_dict(v, new_key, sep=sep))
        else:
            items[new_key] = v
    return items

def normalize(record: dict) -> dict:
    normalized = {}

    flat = flatten_dict(record)

    for key, value in flat.items():
        segments = key.split(".")
        normalized_segments = [normalize_segment(seg) for seg in segments]
        norm_key = ".".join(normalized_segments)
        normalized[norm_key] = value

    return normalized
