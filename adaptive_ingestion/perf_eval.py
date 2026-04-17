from __future__ import annotations

import argparse
import csv
import json
import math
import os
import random
import statistics
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import requests

from metadata_store import load_query_metadata
from mongo_backend import get_db
from query_engine import HybridQueryEngine
from sql_backend import get_connection


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def percentile(sorted_vals: list[float], p: float) -> float:
    if not sorted_vals:
        return float("nan")
    if p <= 0:
        return sorted_vals[0]
    if p >= 100:
        return sorted_vals[-1]
    k = (len(sorted_vals) - 1) * (p / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return sorted_vals[int(k)]
    return sorted_vals[f] + (sorted_vals[c] - sorted_vals[f]) * (k - f)


def summarize_latencies_ms(values: list[float]) -> dict[str, Any]:
    s = sorted(values)
    return {
        "n": len(values),
        "avg_ms": statistics.fmean(values) if values else None,
        "p50_ms": percentile(s, 50),
        "p95_ms": percentile(s, 95),
        "p99_ms": percentile(s, 99),
        "min_ms": min(values) if values else None,
        "max_ms": max(values) if values else None,
    }


def ops_per_sec(n_ops: int, total_ms: float) -> float:
    if total_ms <= 0:
        return 0.0
    return n_ops / (total_ms / 1000.0)


def random_value_for_field(name: str, rng: random.Random) -> Any:
    n = name.lower()
    if "email" in n:
        return f"user{rng.randint(1, 10_000)}@example.com"
    if "user" in n and n.endswith("_id"):
        return str(rng.randint(1, 10_000))
    if n.endswith("_id") or "id" == n:
        return str(rng.randint(1, 10_000_000))
    if "temp" in n or "temperature" in n:
        return round(rng.uniform(10, 40), 2)
    if "humid" in n:
        return round(rng.uniform(20, 90), 2)
    if "timestamp" in n or n.endswith("_at"):
        return now_iso()
    if "status" in n:
        return rng.choice(["active", "inactive", "maintenance"])
    if "role" in n:
        return rng.choice(["student", "staff", "admin"])
    if "severity" in n:
        return rng.choice(["low", "medium", "high", "critical"])
    if "resolved" in n:
        return rng.choice([True, False])
    if "location" in n:
        return rng.choice(["lab", "library", "cafeteria", "parking", "hostel"])
    if "type" in n:
        return rng.choice(["temperature", "humidity", "motion", "co2", "light"])
    if "message" in n:
        return rng.choice(["ok", "warning", "threshold exceeded", "manual entry"])
    if "tags" in n:
        return rng.sample(["iot", "campus", "alert", "sensor", "edge"], k=rng.randint(0, 3))
    if "payload" in n or "context" in n or "details" in n:
        return {"k": rng.randint(1, 1000), "v": rng.choice(["a", "b", "c"])}
    # default: short string or number
    return rng.choice([rng.randint(0, 1000), f"v{rng.randint(0, 1000)}"])


def build_synthetic_record(fields: list[str], rng: random.Random, width: int) -> dict[str, Any]:
    chosen = fields[:]
    rng.shuffle(chosen)
    chosen = chosen[: max(1, min(width, len(chosen)))]
    rec = {}
    for f in chosen:
        rec[f] = random_value_for_field(f, rng)
    return rec


def logical_distribution(metadata: dict[str, dict]) -> dict[str, Any]:
    total = len(metadata)
    sql_fields = sum(1 for v in metadata.values() if v.get("backend") == "sql")
    mongo_fields = sum(1 for v in metadata.values() if v.get("backend") == "mongo")
    return {
        "total_fields": total,
        "sql_fields": sql_fields,
        "mongo_fields": mongo_fields,
        "sql_ratio": (sql_fields / total) if total else None,
        "mongo_ratio": (mongo_fields / total) if total else None,
    }


def make_sql_only_metadata(metadata: dict[str, dict]) -> dict[str, dict]:
    out = {}
    for field, info in metadata.items():
        ni = dict(info)
        # preserve table name if present; if not, provide a default logical table
        ni["backend"] = "sql"
        ni.setdefault("table", info.get("table") or "LOGICAL")
        out[field] = ni
    return out


def make_mongo_only_metadata(metadata: dict[str, dict]) -> dict[str, dict]:
    out = {}
    for field, info in metadata.items():
        ni = dict(info)
        ni["backend"] = "mongo"
        ni.setdefault("collection", info.get("collection") or "logical")
        out[field] = ni
    return out


@dataclass
class OpSample:
    kind: str
    op: str
    mode: str
    latency_ms: float
    trace: dict[str, Any] | None


def run_engine_insert(engine: HybridQueryEngine, record: dict[str, Any], trace: bool) -> tuple[dict, dict | None]:
    q = {"operation": "insert", "data": record}
    if trace:
        q["__trace"] = True
    res = engine.execute(q)
    if trace and isinstance(res, dict) and "__trace" in res:
        return res["result"], res["__trace"]
    return res, None


def run_engine_read(engine: HybridQueryEngine, fields: list[str], filters: dict[str, Any], trace: bool) -> tuple[Any, dict | None]:
    q = {"operation": "read", "fields": fields, "filters": filters}
    if trace:
        q["__trace"] = True
    res = engine.execute(q)
    if trace and isinstance(res, dict) and "__trace" in res:
        return res["result"], res["__trace"]
    return res, None


def measure_metadata_load(iterations: int) -> dict[str, Any]:
    times = []
    for _ in range(iterations):
        t0 = time.perf_counter()
        _ = load_query_metadata()
        times.append((time.perf_counter() - t0) * 1000)
    return summarize_latencies_ms(times)


def write_csv(path: str, rows: list[dict[str, Any]]):
    if not rows:
        return
    fieldnames = sorted({k for r in rows for k in r.keys()})
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def try_plot(out_dir: str, latency_rows: list[dict[str, Any]], throughput_rows: list[dict[str, Any]]):
    """
    Create required visualizations if matplotlib is available:
    - Bar chart comparing avg latency across scenarios
    - Line chart showing throughput under increasing workload
    """
    try:
        import matplotlib.pyplot as plt  # type: ignore
    except Exception:
        return

    os.makedirs(out_dir, exist_ok=True)

    # --- Bar chart: avg latency ---
    # Expect rows like: {scenario, impl, avg_ms}
    scenarios = sorted({r["scenario"] for r in latency_rows})
    impls = ["framework", "direct"]
    x = list(range(len(scenarios)))
    width = 0.38

    def avg_for(scn: str, impl: str):
        for r in latency_rows:
            if r["scenario"] == scn and r["impl"] == impl:
                return r.get("avg_ms") or 0.0
        return 0.0

    fw = [avg_for(s, "framework") for s in scenarios]
    dr = [avg_for(s, "direct") for s in scenarios]

    plt.figure(figsize=(10, 4))
    plt.bar([i - width / 2 for i in x], fw, width=width, label="Framework (logical)")
    plt.bar([i + width / 2 for i in x], dr, width=width, label="Direct DB")
    plt.xticks(x, scenarios, rotation=20, ha="right")
    plt.ylabel("Average latency (ms)")
    plt.title("Comparative latency: logical framework vs direct DB")
    plt.tight_layout()
    plt.legend()
    plt.savefig(os.path.join(out_dir, "compare_latency_bar.png"), dpi=160)
    plt.close()

    # --- Line chart: throughput vs workload ---
    # Expect rows like: {scenario, impl, workload, throughput_ops_sec}
    plt.figure(figsize=(10, 4))
    for scn in sorted({r["scenario"] for r in throughput_rows}):
        for impl in impls:
            pts = [r for r in throughput_rows if r["scenario"] == scn and r["impl"] == impl]
            pts.sort(key=lambda r: r["workload"])
            xs = [p["workload"] for p in pts]
            ys = [p.get("throughput_ops_sec") or 0.0 for p in pts]
            plt.plot(xs, ys, marker="o", label=f"{scn} · {impl}")
    plt.xlabel("Workload size (ops)")
    plt.ylabel("Throughput (ops/sec)")
    plt.title("Throughput under increasing workload")
    plt.tight_layout()
    plt.legend(fontsize=8, ncol=2)
    plt.savefig(os.path.join(out_dir, "compare_throughput_line.png"), dpi=160)
    plt.close()


def run_direct_engine(args) -> dict[str, Any]:
    rng = random.Random(args.seed)
    meta = load_query_metadata()
    fields_all = list(meta.keys())

    results_rows: list[dict[str, Any]] = []
    samples: list[OpSample] = []

    # Metadata load overhead (cold-ish repeated load)
    meta_load_stats = measure_metadata_load(iterations=max(10, args.meta_iters))

    # Distribution across backends
    dist = logical_distribution(meta)

    def new_engine_with_metadata(md: dict[str, dict]):
        sql_conn = get_connection()
        mongo_db = get_db()
        return HybridQueryEngine(md, sql_conn, mongo_db), sql_conn

    # --- Ingestion latency / throughput ---
    for mode, md in [
        ("hybrid", meta),
        ("sql_only", make_sql_only_metadata(meta)),
        ("mongo_only", make_mongo_only_metadata(meta)),
    ]:
        engine, conn = new_engine_with_metadata(md)
        try:
            # warmup
            for _ in range(args.warmup):
                rec = build_synthetic_record(fields_all, rng, width=args.width)
                _ = run_engine_insert(engine, rec, trace=False)

            latencies = []
            route_ms = []
            sql_ms = []
            mongo_ms = []
            commit_ms = []

            t_batch0 = time.perf_counter()
            for _ in range(args.n_inserts):
                rec = build_synthetic_record(fields_all, rng, width=args.width)
                t0 = time.perf_counter()
                res, tr = run_engine_insert(engine, rec, trace=True)
                dt = (time.perf_counter() - t0) * 1000
                latencies.append(dt)
                samples.append(OpSample("ingest", "insert", mode, dt, tr))
                if tr:
                    route_ms.append(tr.get("route_ms", 0.0))
                    sql_ms.append(tr.get("sql_ms", 0.0))
                    mongo_ms.append(tr.get("mongo_ms", 0.0))
                    commit_ms.append(tr.get("commit_ms", 0.0))
            t_batch_ms = (time.perf_counter() - t_batch0) * 1000

            ingest_stats = summarize_latencies_ms(latencies)
            results_rows.append({
                "experiment": "ingestion_latency",
                "mode": mode,
                **ingest_stats,
                "throughput_ops_sec": ops_per_sec(len(latencies), t_batch_ms),
                "avg_route_ms": statistics.fmean(route_ms) if route_ms else None,
                "avg_sql_ms": statistics.fmean(sql_ms) if sql_ms else None,
                "avg_mongo_ms": statistics.fmean(mongo_ms) if mongo_ms else None,
                "avg_commit_ms": statistics.fmean(commit_ms) if commit_ms else None,
                "coord_overhead_ms_est": (ingest_stats["avg_ms"] - (statistics.fmean(sql_ms) if sql_ms else 0.0) - (statistics.fmean(mongo_ms) if mongo_ms else 0.0)) if ingest_stats["avg_ms"] is not None else None,
            })
        finally:
            conn.close()

    # Transaction coordination overhead (compare hybrid vs max(single))
    ingest = {r["mode"]: r for r in results_rows if r["experiment"] == "ingestion_latency"}
    if "hybrid" in ingest and "sql_only" in ingest and "mongo_only" in ingest:
        hybrid_avg = ingest["hybrid"]["avg_ms"]
        best_single = max(ingest["sql_only"]["avg_ms"], ingest["mongo_only"]["avg_ms"])
        results_rows.append({
            "experiment": "txn_coordination_overhead",
            "mode": "hybrid_vs_single",
            "avg_ms": (hybrid_avg - best_single) if (hybrid_avg is not None and best_single is not None) else None,
            "notes": "avg(hybrid_insert_ms) - max(avg(sql_only_insert_ms), avg(mongo_only_insert_ms))",
        })

    # --- Logical query response time ---
    engine, conn = new_engine_with_metadata(meta)
    try:
        # ensure some data exists
        for _ in range(max(args.warmup, 50)):
            rec = build_synthetic_record(fields_all, rng, width=args.width)
            _ = run_engine_insert(engine, rec, trace=False)

        query_latencies = []
        route_ms = []
        sql_ms = []
        mongo_ms = []
        merge_ms = []

        for _ in range(args.n_queries):
            # pick a random field and query equality on its generated value
            chosen_fields = fields_all[:]
            rng.shuffle(chosen_fields)
            proj = chosen_fields[: max(3, min(10, len(chosen_fields)))]

            # use a simple filter on one likely-present field
            f = rng.choice(proj)
            filters = {f: random_value_for_field(f, rng)}

            t0 = time.perf_counter()
            res, tr = run_engine_read(engine, proj, filters, trace=True)
            dt = (time.perf_counter() - t0) * 1000
            query_latencies.append(dt)
            samples.append(OpSample("query", "read", "hybrid", dt, tr))
            if tr:
                route_ms.append(tr.get("route_ms", 0.0))
                sql_ms.append(tr.get("sql_ms", 0.0))
                mongo_ms.append(tr.get("mongo_ms", 0.0))
                merge_ms.append(tr.get("merge_ms", 0.0))

        q_stats = summarize_latencies_ms(query_latencies)
        results_rows.append({
            "experiment": "logical_query_latency",
            "mode": "hybrid",
            **q_stats,
            "avg_route_ms": statistics.fmean(route_ms) if route_ms else None,
            "avg_sql_ms": statistics.fmean(sql_ms) if sql_ms else None,
            "avg_mongo_ms": statistics.fmean(mongo_ms) if mongo_ms else None,
            "avg_merge_ms": statistics.fmean(merge_ms) if merge_ms else None,
            "metadata_load_avg_ms": meta_load_stats.get("avg_ms"),
        })
    finally:
        conn.close()

    # Build JSON output
    out = {
        "ts": now_iso(),
        "mode": "direct_engine",
        "config": {
            "seed": args.seed,
            "width": args.width,
            "warmup": args.warmup,
            "n_inserts": args.n_inserts,
            "n_queries": args.n_queries,
            "meta_iters": args.meta_iters,
        },
        "distribution": dist,
        "metadata_load_overhead": meta_load_stats,
        "results": results_rows,
    }

    # Save artifacts
    os.makedirs(args.out_dir, exist_ok=True)
    json_path = os.path.join(args.out_dir, "perf_results.json")
    csv_path = os.path.join(args.out_dir, "perf_results.csv")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)
    write_csv(csv_path, results_rows)

    return out


def run_http(args) -> dict[str, Any]:
    # This measures end-to-end logical query time including HTTP + JSON overhead.
    rng = random.Random(args.seed)
    meta = load_query_metadata()
    fields_all = list(meta.keys())
    merge_key = "sys_ingested_at"

    base = args.base_url.rstrip("/")
    results_rows: list[dict[str, Any]] = []

    # Metadata load overhead
    meta_load_stats = measure_metadata_load(iterations=max(10, args.meta_iters))

    # Insert via HTTP
    latencies = []
    route_ms = []
    sql_ms = []
    mongo_ms = []
    commit_ms = []
    inserted_ids: list[str] = []
    t_batch0 = time.perf_counter()
    for _ in range(args.n_inserts):
        rec = build_synthetic_record(fields_all, rng, width=args.width)
        # Add trace request
        rec["__trace"] = True
        t0 = time.perf_counter()
        r = requests.post(f"{base}/api/data/any", json=rec, timeout=30)
        dt = (time.perf_counter() - t0) * 1000
        if not r.ok:
            raise RuntimeError(f"Insert failed: HTTP {r.status_code} {r.text[:500]}")
        try:
            j = r.json()
            rid = j.get("record_id")
            if rid:
                inserted_ids.append(str(rid))
            tr = j.get("__trace")
            if tr:
                route_ms.append(tr.get("route_ms", 0.0))
                sql_ms.append(tr.get("sql_ms", 0.0))
                mongo_ms.append(tr.get("mongo_ms", 0.0))
                commit_ms.append(tr.get("commit_ms", 0.0))
        except Exception:
            pass
        latencies.append(dt)
    t_batch_ms = (time.perf_counter() - t_batch0) * 1000
    stats = summarize_latencies_ms(latencies)
    results_rows.append({
        "experiment": "ingestion_latency_http",
        "mode": "hybrid",
        **stats,
        "throughput_ops_sec": ops_per_sec(len(latencies), t_batch_ms),
        "avg_route_ms": statistics.fmean(route_ms) if route_ms else None,
        "avg_sql_ms": statistics.fmean(sql_ms) if sql_ms else None,
        "avg_mongo_ms": statistics.fmean(mongo_ms) if mongo_ms else None,
        "avg_commit_ms": statistics.fmean(commit_ms) if commit_ms else None,
        "coord_overhead_ms_est": (stats["avg_ms"] - (statistics.fmean(sql_ms) if sql_ms else 0.0) - (statistics.fmean(mongo_ms) if mongo_ms else 0.0)) if stats["avg_ms"] is not None else None,
        "metadata_load_avg_ms": meta_load_stats.get("avg_ms"),
    })

    # Query via HTTP
    q_lat = []
    for _ in range(args.n_queries):
        proj = fields_all[:]
        rng.shuffle(proj)
        proj = proj[: max(3, min(10, len(proj)))]
        # Always filter using the merge key for stability across backends.
        # This avoids accidental SQL errors when filtering on a field that isn't a real SQL column.
        if inserted_ids:
            rid = rng.choice(inserted_ids)
            filters = {merge_key: rid}
        else:
            filters = {}
        body = {"operation": "read", "fields": proj, "filters": filters, "__trace": True}
        t0 = time.perf_counter()
        r = requests.post(f"{base}/api/query", json=body, timeout=30)
        if not r.ok:
            raise RuntimeError(f"Query failed: HTTP {r.status_code} {r.text[:500]}")
        q_lat.append((time.perf_counter() - t0) * 1000)
    results_rows.append({
        "experiment": "logical_query_latency_http",
        "mode": "hybrid",
        **summarize_latencies_ms(q_lat),
        "metadata_load_avg_ms": meta_load_stats.get("avg_ms"),
    })

    out = {
        "ts": now_iso(),
        "mode": "http",
        "base_url": base,
        "config": {
            "seed": args.seed,
            "width": args.width,
            "n_inserts": args.n_inserts,
            "n_queries": args.n_queries,
        },
        "metadata_load_overhead": meta_load_stats,
        "results": results_rows,
    }

    os.makedirs(args.out_dir, exist_ok=True)
    json_path = os.path.join(args.out_dir, "perf_results_http.json")
    csv_path = os.path.join(args.out_dir, "perf_results_http.csv")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)
    write_csv(csv_path, results_rows)
    return out


def _pick_sql_field(meta: dict[str, dict], preferred: list[str]) -> tuple[str, str] | None:
    for f in preferred:
        info = meta.get(f)
        if info and info.get("backend") == "sql" and info.get("table"):
            return f, info["table"]
    for f, info in meta.items():
        if info.get("backend") == "sql" and info.get("table"):
            return f, info["table"]
    return None


def _pick_mongo_field(meta: dict[str, dict], preferred: list[str]) -> tuple[str, str] | None:
    for f in preferred:
        info = meta.get(f)
        if info and info.get("backend") == "mongo" and info.get("collection"):
            return f, info["collection"]
    for f, info in meta.items():
        if info.get("backend") == "mongo" and info.get("collection"):
            return f, info["collection"]
    return None


def run_compare_http(args) -> dict[str, Any]:
    """
    Comparative analysis: logical framework (HTTP) vs direct DB access.
    Produces:
    - latency comparisons (bar chart)
    - throughput vs workload (line chart)
    - CSV/JSON summaries
    """
    rng = random.Random(args.seed)
    meta = load_query_metadata()
    fields_all = list(meta.keys())
    merge_key = "sys_ingested_at"

    base = args.base_url.rstrip("/")
    out_dir = args.out_dir

    # Metadata load overhead
    meta_load_stats = measure_metadata_load(iterations=max(10, args.meta_iters))

    # Choose representative fields for scenarios
    sql_pick = _pick_sql_field(meta, ["username", "email", "user_id", "device_model"])
    mongo_pick = _pick_mongo_field(meta, ["payload", "context", "details", "tags", "weather"])
    if not sql_pick or not mongo_pick:
        raise RuntimeError("Could not infer representative SQL/Mongo fields from query_metadata.json")
    sql_field, sql_table = sql_pick
    mongo_field, mongo_collection = mongo_pick

    # Seed data via framework inserts and keep record_ids
    inserted_ids: list[str] = []
    for _ in range(max(50, args.warmup)):
        rec = build_synthetic_record(fields_all, rng, width=args.width)
        r = requests.post(f"{base}/api/data/any", json=rec, timeout=30)
        if not r.ok:
            raise RuntimeError(f"Warmup insert failed: HTTP {r.status_code} {r.text[:500]}")
        try:
            j = r.json()
            if j.get("record_id"):
                inserted_ids.append(str(j["record_id"]))
        except Exception:
            pass
    if not inserted_ids:
        raise RuntimeError("No record_ids collected from inserts; cannot run comparisons.")

    # Direct DB handles
    sql_conn = get_connection()
    mongo_db = get_db()

    latency_rows: list[dict[str, Any]] = []
    throughput_rows: list[dict[str, Any]] = []

    def framework_read_user(rid: str):
        body = {"operation": "read", "fields": [merge_key, sql_field], "filters": {merge_key: rid}}
        r = requests.post(f"{base}/api/query", json=body, timeout=30)
        if not r.ok:
            raise RuntimeError(f"Framework read failed: HTTP {r.status_code} {r.text[:200]}")
        return r.json()

    def direct_sql_read_user(rid: str):
        cur = sql_conn.cursor()
        q = f"SELECT {merge_key}, {sql_field} FROM {sql_table} WHERE {merge_key} = ? LIMIT 1"
        cur.execute(q, [rid])
        _ = cur.fetchone()
        cur.close()

    def framework_read_nested(rid: str):
        body = {"operation": "read", "fields": [merge_key, mongo_field], "filters": {merge_key: rid}}
        r = requests.post(f"{base}/api/query", json=body, timeout=30)
        if not r.ok:
            raise RuntimeError(f"Framework read failed: HTTP {r.status_code} {r.text[:200]}")
        return r.json()

    def direct_mongo_read_nested(rid: str):
        _ = mongo_db[mongo_collection].find_one({merge_key: rid}, {mongo_field: 1, merge_key: 1, "_id": 0})

    def framework_update_multi(rid: str):
        body = {
            "operation": "update",
            "filters": {merge_key: rid},
            "data": {sql_field: random_value_for_field(sql_field, rng), mongo_field: random_value_for_field(mongo_field, rng)},
        }
        r = requests.post(f"{base}/api/query", json=body, timeout=30)
        if not r.ok:
            raise RuntimeError(f"Framework update failed: HTTP {r.status_code} {r.text[:200]}")
        return r.json()

    def direct_update_multi(rid: str):
        # SQL update
        cur = sql_conn.cursor()
        q = f"UPDATE {sql_table} SET {sql_field} = ? WHERE {merge_key} = ?"
        cur.execute(q, [str(random_value_for_field(sql_field, rng)), rid])
        sql_conn.commit()
        cur.close()
        # Mongo update
        mongo_db[mongo_collection].update_many({merge_key: rid}, {"$set": {mongo_field: random_value_for_field(mongo_field, rng)}})

    def measure_latency(fn, n: int) -> dict[str, Any]:
        vals = []
        for _ in range(n):
            rid = rng.choice(inserted_ids)
            t0 = time.perf_counter()
            fn(rid)
            vals.append((time.perf_counter() - t0) * 1000)
        return summarize_latencies_ms(vals)

    # --- Latency comparisons (fixed n) ---
    n = args.n_queries
    for scenario, fw_fn, dr_fn in [
        ("retrieve_user", framework_read_user, direct_sql_read_user),
        ("access_nested", framework_read_nested, direct_mongo_read_nested),
        ("update_multi", framework_update_multi, direct_update_multi),
    ]:
        fw_stats = measure_latency(fw_fn, n)
        dr_stats = measure_latency(dr_fn, n)
        latency_rows.append({"scenario": scenario, "impl": "framework", **fw_stats})
        latency_rows.append({"scenario": scenario, "impl": "direct", **dr_stats})

    # --- Throughput under increasing workload (ops/sec) ---
    workloads = [50, 100, 200, 400]
    for scenario, fw_fn, dr_fn in [
        ("retrieve_user", framework_read_user, direct_sql_read_user),
        ("access_nested", framework_read_nested, direct_mongo_read_nested),
        ("update_multi", framework_update_multi, direct_update_multi),
    ]:
        for w in workloads:
            # framework
            t0 = time.perf_counter()
            for _ in range(w):
                fw_fn(rng.choice(inserted_ids))
            fw_ms = (time.perf_counter() - t0) * 1000
            throughput_rows.append({"scenario": scenario, "impl": "framework", "workload": w, "throughput_ops_sec": ops_per_sec(w, fw_ms)})

            # direct
            t1 = time.perf_counter()
            for _ in range(w):
                dr_fn(rng.choice(inserted_ids))
            dr_ms = (time.perf_counter() - t1) * 1000
            throughput_rows.append({"scenario": scenario, "impl": "direct", "workload": w, "throughput_ops_sec": ops_per_sec(w, dr_ms)})

    out = {
        "ts": now_iso(),
        "mode": "compare_http",
        "base_url": base,
        "config": {
            "seed": args.seed,
            "width": args.width,
            "n_ops": args.n_queries,
            "warmup": args.warmup,
        },
        "chosen_fields": {
            "merge_key": merge_key,
            "sql_field": sql_field,
            "sql_table": sql_table,
            "mongo_field": mongo_field,
            "mongo_collection": mongo_collection,
        },
        "latency": latency_rows,
        "throughput": throughput_rows,
        "metadata_load_overhead": meta_load_stats,
    }

    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, "compare_results.json"), "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)
    write_csv(os.path.join(out_dir, "compare_latency.csv"), latency_rows)
    write_csv(os.path.join(out_dir, "compare_throughput.csv"), throughput_rows)
    try_plot(out_dir, latency_rows, throughput_rows)

    try:
        sql_conn.close()
    except Exception:
        pass

    return out


def main():
    p = argparse.ArgumentParser(description="Performance evaluation for hybrid logical DB framework")
    p.add_argument("--mode", choices=["direct", "http", "compare"], default="direct")
    p.add_argument("--out-dir", default="perf_out")
    p.add_argument("--seed", type=int, default=7)
    p.add_argument("--width", type=int, default=25, help="How many logical fields per synthetic record")
    p.add_argument("--warmup", type=int, default=25)
    p.add_argument("--n-inserts", type=int, default=200)
    p.add_argument("--n-queries", type=int, default=200)
    p.add_argument("--meta-iters", type=int, default=50)
    p.add_argument("--base-url", default="http://127.0.0.1:5000", help="Used for --mode=http")
    args = p.parse_args()

    if args.mode == "direct":
        out = run_direct_engine(args)
        print("Wrote:", os.path.join(args.out_dir, "perf_results.json"))
        print("Wrote:", os.path.join(args.out_dir, "perf_results.csv"))
        print("Summary rows:", len(out.get("results", [])))
    elif args.mode == "http":
        out = run_http(args)
        print("Wrote:", os.path.join(args.out_dir, "perf_results_http.json"))
        print("Wrote:", os.path.join(args.out_dir, "perf_results_http.csv"))
        print("Summary rows:", len(out.get("results", [])))
    else:
        out = run_compare_http(args)
        print("Wrote:", os.path.join(args.out_dir, "compare_results.json"))
        print("Wrote:", os.path.join(args.out_dir, "compare_latency.csv"))
        print("Wrote:", os.path.join(args.out_dir, "compare_throughput.csv"))
        # Plots are optional if matplotlib is installed
        print("Summary rows:", len(out.get("latency", [])) + len(out.get("throughput", [])))


if __name__ == "__main__":
    main()

