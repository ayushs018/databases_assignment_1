def decide_backend(field_name: str, field_stats: dict) -> str:
    """
    Decide whether a field should go to SQL or MongoDB
    based on learned statistics.
    """

    presence = field_stats["presence_ratio"]
    type_dist = field_stats["type_distribution"]

    # Find dominant type
    total = sum(type_dist.values())
    dominant_type, dominant_count = max(
        type_dist.items(), key=lambda x: x[1]
    )
    dominant_ratio = dominant_count / total

    # Complex or nested types → MongoDB
    if dominant_type in {"list", "dict"}:
        return "mongo"

    # Low frequency → MongoDB
    if presence < 0.8:
        return "mongo"

    # Type instability → MongoDB
    if dominant_ratio < 0.9:
        return "mongo"

    # Otherwise → SQL
    return "sql"
