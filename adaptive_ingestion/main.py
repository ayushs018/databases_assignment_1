# main.py
import requests
import json
from sseclient import SSEClient
from datetime import datetime, timezone

from normalizer import normalize
from analyzer import FieldStats
from heuristics import decide_backend
from metadata_store import load_metadata, save_metadata
from sql_backend import insert_sql
from mongo_backend import insert_mongo



STREAM_URL = "http://127.0.0.1:8000/record/20"
metadata = load_metadata()


def main():
    print("Connecting to stream...")
    
    response = requests.get(STREAM_URL, stream=True)
    client = SSEClient(response)
    stats = FieldStats()
    buffered_records = []
    for event in client.events():
        if event.event == "record":
            record = json.loads(event.data)
            record["sys_ingested_at"] = datetime.now(timezone.utc).isoformat()
            normalized_record = normalize(record)
            stats.update(normalized_record)
            buffered_records.append(normalized_record)
            

    print("\n=== FIELD PLACEMENT DECISIONS ===")
    summary = stats.summary()
    # print (summary)

    for field, info in summary.items():
        # If decision already exists, reuse it
        print (info)  
        if field in metadata:
            backend = metadata[field]["backend"]
        else:
            backend = decide_backend(field, info)
            metadata[field] = {
                "backend": backend,
                "presence_ratio": info["presence_ratio"],
                "type_distribution": info["type_distribution"]
            }

        print(f"{field} -> {backend.upper()}")

    # Persist metadata
    save_metadata(metadata)
    for normalized_record in buffered_records: 
        sql_fields = []
        mongo_fields = []

        # route using metadata
        for field in normalized_record:
            backend = metadata.get(field, {}).get("backend", "mongo")

            if backend == "sql":
                sql_fields.append(field)
            else:
                mongo_fields.append(field)

        # ensure join keys in both
        for key in ("username", "sys_ingested_at"):
            if key not in sql_fields:
                sql_fields.append(key)
            if key not in mongo_fields:
                mongo_fields.append(key)

        # insert_sql(normalized_record, sql_fields)
        # insert_mongo(normalized_record, mongo_fields)

    with open("logs.json", "w") as f:
        json.dump(summary, f, indent=2)




if __name__ == "__main__":
    main()
