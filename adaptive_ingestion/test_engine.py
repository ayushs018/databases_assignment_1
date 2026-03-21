import json
import sqlite3
from pymongo import MongoClient

from query_engine import HybridQueryEngine


# =========================
# LOAD METADATA
# =========================
with open("query_metadata.json") as f:
    metadata = json.load(f)


# =========================
# CONNECT DATABASES
# =========================

# SQLite
sql_conn = sqlite3.connect("hybrid.db")


# MongoDB
mongo_client = MongoClient("mongodb://localhost:27017/")
mongo_db = mongo_client["test_db"]


# =========================
# INIT ENGINE
# =========================
engine = HybridQueryEngine(metadata, sql_conn, mongo_db)


# =========================
# TEST CASES
# =========================

def test_insert():
    print("\n--- INSERT ---")

    query = {
        "operation": "insert",
        "filters": {
            "username": "ayush",
            "spo2": 97,
            "device_model": "iPhone",
            "timezone": "IST"
        }
    }

    print(engine.execute(query))


def test_read():
    print("\n--- READ ---")

    query = {
        "operation": "read",
        "fields": ["username", "spo2", "device_model", "timezone", "country", "cpu_usage"],
        "filters": {"subscription": "trial"}
    }

    result = engine.execute(query)
    print(result)


def test_update():
    print("\n--- UPDATE ---")
    query = {
        "operation": "update",
        "fields": ["spo2"],
        "filters": {
            "username": "linda37",
            "spo2": 100,
        },
        "data": {
            "username": "AYush",
            "timezone": "INDIA",
        }
    }
    print(engine.execute(query))


def test_delete_field():
    print("\n--- DELETE FIELD ---")

    query = {
        "operation": "delete",
        "fields": ["spo2"],
        "filters": {"username": "AYush"}
    }

    print(engine.execute(query))


def test_delete_row():
    print("\n--- DELETE ROW ---")

    query = {
        "operation": "delete",
        "filters": {"username": "nicholsonmatthew", "spo2": "96"}
    }

    print(engine.execute(query))


# =========================
# RUN TESTS
# =========================

if __name__ == "__main__":
    # test_insert()
    # test_read()
    # test_update()
    # test_read()
    # test_delete_field()
    # test_delete_row()
    test_read()