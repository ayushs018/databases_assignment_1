from pymongo import MongoClient
client = MongoClient("mongodb://localhost:27017/")
db = client["adaptive_db"]
collection = db["records"]

JOIN_KEYS = ("username", "sys_ingested_at")

def insert_mongo(record, mongo_fields):
    # ensure join keys always included
    fields = set(mongo_fields) | set(JOIN_KEYS)

    doc = {k: record.get(k) for k in fields if k in record}

    collection.insert_one(doc)
