"""
UI-only Flask Application for SADF Logical Dashboard

This version removes undefined backend dependencies
and returns dummy data so UI can be viewed immediately.
"""

from flask import Flask, jsonify, request, render_template
from bson import ObjectId
from metadata_store import load_metadata
from sql_backend import get_connection
from mongo_backend import get_db
from query_engine import HybridQueryEngine
import sqlite3
from pymongo import MongoClient
import json


app = Flask(__name__)

# --- Backend initialization ---
# Keep Mongo + metadata global; create SQLite connections per-request (thread safety).
# metadata = load_metadata()  # from metadata.json
mongo_client = MongoClient("mongodb://localhost:27017/")
mongo_db = mongo_client["test_db"]

with open("query_metadata.json") as f:
    metadata = json.load(f)


def _new_engine():
    sql_conn = sqlite3.connect("hybrid.db")
    return HybridQueryEngine(metadata, sql_conn, mongo_db), sql_conn


# engine = _new_engine()

# ── Pages ─────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/docs")
def docs():
    return render_template("docs.html")


# ── Session & Metadata (dummy data) ───────────

@app.route("/api/session")
def api_session():

    return jsonify({
        "user": "demo_user",
        "database": "hybrid_demo",
        "status": "active"
    })


@app.route("/api/metadata")
def api_metadata():

    return jsonify(metadata)


@app.route("/api/entities")
def api_entities():

    entities = set()
    for info in metadata.values():
        table = info.get("table")
        collection = info.get("collection")
        if table:
            entities.add(table)
        if collection:
            entities.add(collection)
    return jsonify(sorted(entities))


# ── CRUD (dummy responses) ───────────────────
def _fields_for_entity(entity_name: str):
    """
    Helper to get all fields belonging to a given table/collection name.
    """
    fields = []
    for field, info in metadata.items():
        if info.get("table") == entity_name or info.get("collection") == entity_name:
            fields.append(field)
    return fields

@app.route("/api/data/<entity>", methods=["GET"])
def api_read(entity):

    filters = dict(request.args)
    fields = _fields_for_entity(entity)
    if not fields:
        # fall back to all known fields if entity doesn't match anything
        fields = list(metadata.keys())
    engine, sql_conn = _new_engine()
    try:
        result = engine.execute({
            "operation": "read",
            "fields": fields,
            "filters": filters,
        })
    finally:
        sql_conn.close()
    return jsonify({
        "status": "ok",
        "entity": entity,
        "count": len(result),
        "data": result,
    })


@app.route("/api/data/<entity>", methods=["POST"])
def api_create(entity):
    payload = request.get_json(force=True) or {}
    engine, sql_conn = _new_engine()
    try:
        engine.execute({
            "operation": "insert",
            "filters": payload,   # insert() currently reads `filters` as data
        })
    finally:
        sql_conn.close()
    # You can extend this to return created ID(s) if you change insert() to return them
    return jsonify({
        "status": "ok",
        "entity": entity,
    }), 201


@app.route("/api/data/<entity>/<record_id>", methods=["PUT"])
def api_update(entity, record_id):

    payload = request.get_json(force=True) or {}
    # Filters identify which row/doc to update; here we use sys_ingested_at
    filters = {"sys_ingested_at": record_id}
    engine, sql_conn = _new_engine()
    try:
        engine.execute({
            "operation": "update",
            "filters": filters,
            "data": payload,
        })
    finally:
        sql_conn.close()
    return jsonify({
        "status": "ok",
        "entity": entity,
        "record_id": record_id,
    })


@app.route("/api/data/<entity>/<record_id>", methods=["DELETE"])
def api_delete(entity, record_id):

    filters = {"sys_ingested_at": record_id}
    engine, sql_conn = _new_engine()
    try:
        engine.execute({
            "operation": "delete",
            "filters": filters,
        })
    finally:
        sql_conn.close()
    return jsonify({
        "status": "ok",
        "entity": entity,
        "record_id": record_id,
    })


# ── Logical Query (dummy) ────────────────────

def serialize_mongo(obj):

    if isinstance(obj, list):

        return [serialize_mongo(i) for i in obj]

    elif isinstance(obj, dict):

        new_dict = {}

        for k, v in obj.items():

            if isinstance(v, ObjectId):

                new_dict[k] = str(v)

            else:

                new_dict[k] = serialize_mongo(v)

        return new_dict

    else:

        return obj

@app.route("/api/query", methods=["POST"])
def api_query():

    query = request.json

    engine, sql_conn = _new_engine()

    try:

        result = engine.execute(query)

    finally:

        sql_conn.close()

    result = serialize_mongo(result)

    return jsonify(result)


@app.route("/api/search", methods=["GET"])
def api_search():

    q = request.args.get("q", "").strip()
    if not q:
        return jsonify({"status": "ok", "query": q, "count": 0, "data": []})
    filters = {}
    if "name" in metadata:
        filters["name"] = q
    engine, sql_conn = _new_engine()
    try:
        result = engine.execute({
            "operation": "read",
            "fields": list(metadata.keys()),
            "filters": filters,
        })
    finally:
        sql_conn.close()
    return jsonify({
        "status": "ok",
        "query": q,
        "count": len(result),
        "data": result,
    })


# ── ACID Tests (dummy results) ───────────────

@app.route("/api/acid-test/<test_name>", methods=["POST"])
def api_acid_test(test_name):

    results = {

        "atomicity":
        {
            "test": "atomicity",
            "status": "passed"
        },

        "consistency":
        {
            "test": "consistency",
            "status": "passed"
        },

        "isolation":
        {
            "test": "isolation",
            "status": "passed"
        },

        "durability":
        {
            "test": "durability",
            "status": "passed"
        },

        "all":
        [
            {"test": "atomicity", "status": "passed"},
            {"test": "consistency", "status": "passed"},
            {"test": "isolation", "status": "passed"},
            {"test": "durability", "status": "passed"}
        ]
    }

    return jsonify(results.get(test_name, {"error": "unknown test"}))


# ── Run ─────────────────────────────────────

if __name__ == "__main__":

    app.run(
        host="0.0.0.0",
        port=5000,
        debug=True
    )