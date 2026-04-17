from __future__ import annotations

from datetime import datetime, timezone
import uuid

from flask import Flask, jsonify, request, render_template

from bson import ObjectId

from metadata_store import load_query_metadata
from sql_backend import get_connection
from mongo_backend import get_db
from query_engine import HybridQueryEngine


app = Flask(__name__)

# --- Backend initialization ---
# Keep Mongo + metadata global; create SQLite connections per-request.
metadata = load_query_metadata()
mongo_db = get_db()

# --- Sessions + history (in-memory) ---
PROJECT_NAME = "SADF Dashboard"
_ACTIVE_SESSION_ID = str(uuid.uuid4())[:8]
_SESSION_STARTED_AT = datetime.now(timezone.utc).isoformat()
_QUERY_HISTORY: list[dict] = []


def _new_engine():
    sql_conn = get_connection()
    return HybridQueryEngine(metadata, sql_conn, mongo_db), sql_conn


def _serialize(obj):
    if isinstance(obj, ObjectId):
        return str(obj)
    if isinstance(obj, list):
        return [_serialize(i) for i in obj]
    if isinstance(obj, dict):
        return {k: _serialize(v) for k, v in obj.items()}
    return obj


def _logical_schema():
    """
    Return a logical-only schema view.
    Keeps SQL entities (e.g. USERS) and Mongo entities (e.g. users) separate
    so users can explore each backend's data independently.
    Hides all backend/table/collection implementation details.
    """
    entities: dict[str, dict] = {}
    for field, info in metadata.items():
        if field.startswith("_"):
            continue
        entity = info.get("table") or info.get("collection") or "unknown"
        ent = entities.setdefault(entity, {"description": "", "fields": []})
        if field not in ent["fields"]:
            ent["fields"].append(field)

    for ent in entities.values():
        ent["fields"].sort()

    return dict(sorted(entities.items(), key=lambda kv: kv[0].lower()))

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
        "project_name": PROJECT_NAME,
        "session_id": _ACTIVE_SESSION_ID,
        "status": "active",
        "started_at": _SESSION_STARTED_AT,
    })

@app.route("/api/sessions")
def api_sessions():
    return jsonify([{
        "session_id": _ACTIVE_SESSION_ID,
        "status": "active",
        "started_at": _SESSION_STARTED_AT,
        "last_active_at": datetime.now(timezone.utc).isoformat(),
    }])


@app.route("/api/schema")
def api_schema():
    return jsonify(_logical_schema())


@app.route("/api/entities")
def api_entities():
    return jsonify(_logical_schema())


# ── CRUD (dummy responses) ───────────────────
def _fields_for_entity(entity_name: str):
    """
    Return fields belonging to a logical entity, filtered by backend type.
    - SQL table entities (e.g. USERS): returns SQL-backed fields for that table.
    - Mongo collection entities (e.g. users): returns Mongo-backed fields for that collection.
    sys_ingested_at is always included as the merge/join key.
    """
    # Detect whether this entity is a SQL table or Mongo collection
    is_sql_entity = any(
        info.get("table") == entity_name and info.get("backend") == "sql"
        for info in metadata.values()
    )

    fields = []
    for field, info in metadata.items():
        if field.startswith("_"):
            continue
        table = info.get("table") or ""
        collection = info.get("collection") or ""
        backend = info.get("backend", "")

        if is_sql_entity:
            # SQL entity: include all fields mapped to this table
            if table == entity_name:
                fields.append(field)
        else:
            # Mongo entity: include only Mongo-backed fields for this collection
            if collection == entity_name and backend == "mongo":
                fields.append(field)

    # Always ensure merge key is present
    if "sys_ingested_at" not in fields:
        fields.append("sys_ingested_at")
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
        "data": _serialize(result),
    })


@app.route("/api/data/<entity>", methods=["POST"])
def api_create(entity):
    payload = request.get_json(force=True) or {}
    engine, sql_conn = _new_engine()
    try:
        res = engine.execute({
            "operation": "insert",
            "data": payload,
            "__trace": bool(request.args.get("trace") or payload.get("__trace"))
        })
    finally:
        sql_conn.close()

    if isinstance(res, dict) and "__trace" in res:
        out = {
            "status": "ok",
            "entity": entity,
            "record_id": res["result"].get("record_id"),
            "__trace": res["__trace"]
        }
        return jsonify(out), 201

    return jsonify({
        "status": "ok",
        "entity": entity,
        "record_id": res.get("record_id") if isinstance(res, dict) else None,
    }), 201


@app.route("/api/data/<entity>/<record_id>", methods=["PUT"])
def api_update(entity, record_id):

    payload = request.get_json(force=True) or {}
    # Filters identify which row/doc to update; here we use sys_ingested_at
    filters = {"sys_ingested_at": record_id}
    engine, sql_conn = _new_engine()
    try:
        res = engine.execute({
            "operation": "update",
            "filters": filters,
            "data": payload,
            "__trace": bool(request.args.get("trace") or payload.get("__trace"))
        })
    finally:
        sql_conn.close()

    if isinstance(res, dict) and "__trace" in res:
        return jsonify({
            "status": "ok",
            "entity": entity,
            "record_id": record_id,
            "__trace": res["__trace"]
        })

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


@app.route("/api/query", methods=["POST"])
def api_query():

    query = request.get_json(force=True) or {}
    started = datetime.now(timezone.utc)
    engine, sql_conn = _new_engine()

    try:
        result = engine.execute(query)
    except Exception as e:
        finished = datetime.now(timezone.utc)
        _QUERY_HISTORY.append({
            "ts": started.isoformat(),
            "duration_ms": int((finished - started).total_seconds() * 1000),
            "query": query,
            "error": str(e),
        })
        _QUERY_HISTORY[:] = _QUERY_HISTORY[-100:]
        return jsonify({"status": "error", "error": str(e)}), 500
    finally:
        sql_conn.close()

    finished = datetime.now(timezone.utc)
    
    if isinstance(result, dict) and "__trace" in result:
        serialized_res = _serialize(result["result"])
        trace = result["__trace"]
        _QUERY_HISTORY.append({
            "ts": started.isoformat(),
            "duration_ms": int((finished - started).total_seconds() * 1000),
            "query": query,
            "result_count": len(serialized_res) if isinstance(serialized_res, list) else None,
        })
        _QUERY_HISTORY[:] = _QUERY_HISTORY[-100:]
        return jsonify({
            "status": "ok",
            "result": serialized_res,
            "__trace": trace
        })

    serialized = _serialize(result)

    _QUERY_HISTORY.append({
        "ts": started.isoformat(),
        "duration_ms": int((finished - started).total_seconds() * 1000),
        "query": query,
        "result_count": len(serialized) if isinstance(serialized, list) else None,
    })
    _QUERY_HISTORY[:] = _QUERY_HISTORY[-100:]

    return jsonify({
        "status": "ok",
        "result": serialized,
    })


@app.route("/api/query/history", methods=["GET"])
def api_query_history():
    return jsonify({
        "status": "ok",
        "count": len(_QUERY_HISTORY),
        "items": list(reversed(_QUERY_HISTORY)),
    })


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


# ── ACID Tests (actual results) ───────────────

@app.route("/api/acid-test/<test_name>", methods=["POST"])
def api_acid_test(test_name):

    engine, sql_conn = _new_engine()
    try:
        def format_res(name, r):
            return {
                "test": name,
                "status": "passed" if r.get("passed") else "failed",
                "passed": bool(r.get("passed")),
                "reason": r.get("reason", "")
            }

        if test_name == "atomicity":
            return jsonify([format_res("atomicity", engine.test_atomicity())])
        elif test_name == "consistency":
            return jsonify([format_res("consistency", engine.test_consistency())])
        elif test_name == "isolation":
            return jsonify([format_res("isolation", engine.test_isolation())])
        elif test_name == "durability":
            return jsonify([format_res("durability", engine.test_durability())])
        elif test_name == "all":
            return jsonify([
                format_res("atomicity", engine.test_atomicity()),
                format_res("consistency", engine.test_consistency()),
                format_res("isolation", engine.test_isolation()),
                format_res("durability", engine.test_durability()),
            ])
        else:
            return jsonify({"error": "unknown test"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        sql_conn.close()


# ── Run ─────────────────────────────────────

if __name__ == "__main__":

    app.run(
        host="0.0.0.0",
        port=5000,
        debug=False,
        threaded=True,
        use_reloader=False
    )