from datetime import datetime, timezone
import json
import time
from sql_backend import create_table, add_missing_columns, get_existing_columns


class HybridQueryEngine:
    def __init__(self, metadata, sql_conn, mongo_db):
        self.metadata = metadata
        self.sql_conn = sql_conn
        self.sql_conn.isolation_level = None  # Prevent implicit commits on DDL
        self.mongo_db = mongo_db
        self.merge_key = "sys_ingested_at"

        self.op_map = {
            "eq": "=",
            "ne": "!=",
            "gt": ">",
            "gte": ">=",
            "lt": "<",
            "lte": "<=",
            "like": "LIKE",
            "in": "IN"
        }

    # =========================
    # MAIN EXECUTE
    # =========================
    def execute(self, query):

        op = query.get("operation")

        if op == "read":
            return self.read(query)

        elif op in ["create", "insert"]:
            return self.insert(query)

        elif op == "update":
            self.update(query)
            return {"status": "update successful"}

        elif op == "delete":
            self.delete(query)
            return {"status": "delete successful"}

        else:
            raise ValueError("Invalid operation")

    def _trace_enabled(self, query):
        return bool(query.get("__trace"))

    def _wrap_with_trace(self, query, result, trace):
        if not self._trace_enabled(query):
            return result
        return {"result": result, "__trace": trace}

    def _filter_conditions_for_sql(self, conditions, existing_cols):
        """
        Drop conditions referencing columns that don't exist in a given SQL table.
        This prevents sqlite3.OperationalError: no such column: <field>.
        """
        if not conditions:
            return []

        filtered = []
        for cond in conditions:
            if not isinstance(cond, dict):
                continue
            if "or" in cond and isinstance(cond["or"], list):
                inner = self._filter_conditions_for_sql(cond["or"], existing_cols)
                if inner:
                    filtered.append({"or": inner})
                continue
            field = cond.get("field")
            if field and field in existing_cols:
                filtered.append(cond)
        return filtered

    # =========================
    # CREATE / INSERT
    # =========================
  

    def insert(self, query):

        trace = {}
        t0 = time.perf_counter()
        # Accept either "data" (preferred) or "filters" (older UI payloads)
        data = query.get("data") or query.get("filters") or {}
        sys_ingested_at = datetime.now(timezone.utc).isoformat()
        data[self.merge_key] = sys_ingested_at
        sql_data = {}
        mongo_data = {}
        t_route0 = time.perf_counter()
        # Separate data per backend
        for field, val in data.items():
            info = self.metadata.get(field)
            if not info:
                continue
            if field == "sys_ingested_at":
                sql_data.setdefault(info["table"], {})[field] = sys_ingested_at
                mongo_data.setdefault(info["collection"], {})[field] = sys_ingested_at
                continue
            if info["backend"] == "sql":
                sql_data.setdefault(info["table"], {})[field] = val
            else:
                mongo_data.setdefault(info["collection"], {})[field] = val
        trace["route_ms"] = (time.perf_counter() - t_route0) * 1000

        inserted_mongo_ids = []
        try:
            # BEGIN SQL TRANSACTION
            self.sql_conn.execute("BEGIN IMMEDIATE")
            sql_cursor = self.sql_conn.cursor()
            # SQL inserts
            t_sql0 = time.perf_counter()
            for table, row in sql_data.items():
                # Ensure table exists and columns are present
                create_table(self.sql_conn, table, row.keys())
                add_missing_columns(self.sql_conn, table, row.keys())
                cols = ", ".join(row.keys())
                placeholders = ", ".join(["?"] * len(row))
                q = f"""
                INSERT INTO {table} ({cols})
                VALUES ({placeholders})
                """
                sql_cursor.execute(q, list(row.values()))
            trace["sql_ms"] = (time.perf_counter() - t_sql0) * 1000
            # Mongo inserts
            t_m0 = time.perf_counter()
            for collection, doc in mongo_data.items():
                result = self.mongo_db[collection].insert_one(doc)
                inserted_mongo_ids.append(
                    (collection, result.inserted_id)
                )
            if query.get("__fail_mongo"):
                raise Exception("Simulated Mongo failure")
            trace["mongo_ms"] = (time.perf_counter() - t_m0) * 1000
            # COMMIT SQL if all succeed
            t_commit0 = time.perf_counter()
            self.sql_conn.execute("COMMIT")
            trace["commit_ms"] = (time.perf_counter() - t_commit0) * 1000
            trace["total_ms"] = (time.perf_counter() - t0) * 1000
            trace["record_id"] = sys_ingested_at
            return {
                "status": "ok",
                "record_id": sys_ingested_at
            }
        except Exception as e:
            # rollback SQL
            try: self.sql_conn.execute("ROLLBACK")
            except: pass
            # rollback Mongo manually
            for collection, _id in inserted_mongo_ids:
                self.mongo_db[collection].delete_one(
                    {"_id": _id}
                )
            trace["total_ms"] = (time.perf_counter() - t0) * 1000
            return self._wrap_with_trace(query, {
                "status": "error",
                "message": str(e)
            }, trace)

    # =========================
    # READ
    # =========================
    def read(self, query):

        trace = {}
        t0 = time.perf_counter()
        fields = query.get("fields", [])
        filters = query.get("filters", {}) or {}
        conditions = query.get("conditions", [])
        order_by = query.get("order_by")
        order = query.get("order", "asc")
        limit = query.get("limit")

        # if fields missing → select all metadata fields
        if not fields:
            fields = list(self.metadata.keys())

        # ensure merge key present
        if self.merge_key not in fields:
            fields.append(self.merge_key)

        sql_plan = {}
        mongo_plan = {}

        t_route0 = time.perf_counter()
        for f in fields:
            info = self.metadata.get(f)
            if f == 'sys_ingested_at':
                sql_plan.setdefault(info["table"], []).append(f)
                mongo_plan.setdefault(info["collection"], []).append(f)
                continue
            if not info:
                continue
            if info["backend"] == "sql":
                sql_plan.setdefault(info["table"], []).append(f)
            else:
                
                mongo_plan.setdefault(info["collection"], []).append(f)
        trace["route_ms"] = (time.perf_counter() - t_route0) * 1000

        # Split filters so we don't apply Mongo-only fields to SQL (and vice-versa).
        # This prevents "no such column" errors during SQL query building.
        sql_filters = {}
        mongo_filters = {}
        for k, v in filters.items():
            info = self.metadata.get(k)
            if not info:
                continue
            if info.get("backend") == "sql":
                sql_filters[k] = v
            else:
                mongo_filters[k] = v
        # Always allow merge key filtering for both backends
        if self.merge_key in filters:
            sql_filters[self.merge_key] = filters[self.merge_key]
            mongo_filters[self.merge_key] = filters[self.merge_key]

        # Precisely determine if each backend is "filtered"
        # A backend is filtered if it has direct filters OR if it has conditions that apply to its fields
        sql_safe_conditions = []
        if sql_plan:
            # Get all fields that could exist in SQL
            sql_fields = [f for f, info in self.metadata.items() if info.get("backend") == "sql"]
            if self.merge_key not in sql_fields: sql_fields.append(self.merge_key)
            sql_safe_conditions = self._filter_conditions_for_sql(conditions, sql_fields)
        
        mongo_safe_conditions = []
        if mongo_plan:
            # Get all fields that could exist in Mongo
            mongo_fields = [f for f, info in self.metadata.items() if info.get("backend") == "mongo"]
            if self.merge_key not in mongo_fields: mongo_fields.append(self.merge_key)
            mongo_safe_conditions = self._filter_conditions_for_sql(conditions, mongo_fields)

        sql_filtered = bool(sql_filters) or bool(sql_safe_conditions)
        mongo_filtered = bool(mongo_filters) or bool(mongo_safe_conditions)

        # Decide execution order for optimization
        if mongo_filters and not sql_filters and not conditions:
            # Execute Mongo first
            t_m0 = time.perf_counter()
            mongo_results = self._execute_mongo_read(
                mongo_plan,
                mongo_filters,
                conditions,
                order_by,
                order,
                limit
            )
            trace["mongo_ms"] = (time.perf_counter() - t_m0) * 1000
            
            mongo_keys = [r[self.merge_key] for r in mongo_results if self.merge_key in r]
            if not mongo_keys:
                return self._wrap_with_trace(query, [], trace)
                
            # Pipe to SQL
            if not conditions:
                sql_filters[self.merge_key] = mongo_keys
                
            t_sql0 = time.perf_counter()
            sql_results = self._execute_sql_read(
                sql_plan,
                sql_filters,
                conditions,
                order_by,
                order,
                limit
            )
            trace["sql_ms"] = (time.perf_counter() - t_sql0) * 1000
        else:
            # Execute SQL first
            t_sql0 = time.perf_counter()
            sql_results = self._execute_sql_read(
                sql_plan,
                sql_filters,
                conditions,
                order_by,
                order,
                limit
            )
            trace["sql_ms"] = (time.perf_counter() - t_sql0) * 1000

            if sql_filtered and not mongo_filters:
                sql_keys = [r[self.merge_key] for r in sql_results if self.merge_key in r]
                if not sql_keys:
                    return self._wrap_with_trace(query, [], trace)
                mongo_filters[self.merge_key] = {"$in": sql_keys}

            t_m0 = time.perf_counter()
            mongo_results = self._execute_mongo_read(
                mongo_plan,
                mongo_filters,
                conditions,
                order_by,
                order,
                limit
            )
            trace["mongo_ms"] = (time.perf_counter() - t_m0) * 1000

        t_merge0 = time.perf_counter()
        merged = self._merge(sql_results, mongo_results, sql_filtered, mongo_filtered)
        trace["merge_ms"] = (time.perf_counter() - t_merge0) * 1000
        trace["total_ms"] = (time.perf_counter() - t0) * 1000
        trace["result_count"] = len(merged) if isinstance(merged, list) else None
        return self._wrap_with_trace(query, merged, trace)

    # =========================
    # SQL READ
    # =========================
    def _execute_sql_read(
            self,
            sql_plan,
            filters,
            conditions,
            order_by,
            order,
            limit
    ):

        results = []

        with self.sql_conn:

            cur = self.sql_conn.cursor()

            for table, fields in sql_plan.items():
                existing = set(get_existing_columns(self.sql_conn, table))
                safe_fields = [f for f in fields if f in existing]
                if not safe_fields:
                    continue

                safe_filters = {k: v for k, v in (filters or {}).items() if k in existing}
                safe_conditions = self._filter_conditions_for_sql(conditions, existing)
                safe_order_by = order_by if (order_by in existing) else None

                query, values = self._build_sql_query(
                    table,
                    safe_fields,
                    safe_filters,
                    safe_conditions,
                    safe_order_by,
                    order,
                    limit
                )
                cur.execute(query, values)

                cols = [d[0] for d in cur.description]

                rows = cur.fetchall()

                for r in rows:
                    results.append(dict(zip(cols, r)))

        return results

    # =========================
    # BUILD SQL QUERY
    # =========================
    def _build_sql_query(
            self,
            table,
            fields,
            filters,
            conditions,
            order_by,
            order,
            limit
    ):

        select_clause = ", ".join(fields)

        where_clause, values = self._build_where_clause(
            filters,
            conditions
        )

        q = f"SELECT {select_clause} FROM {table}"

        if where_clause:
            q += f" WHERE {where_clause}"

        if order_by:
            q += f" ORDER BY {order_by} {order.upper()}"

        if limit:
            q += f" LIMIT {limit}"

        return q, values

    # =========================
    # MONGO READ
    # =========================
    def _execute_mongo_read(
        self,
        mongo_plan,
        filters,
        conditions,
        order_by,
        order,
        limit
):

        results = []

        for col, fields in mongo_plan.items():
            if self.merge_key not in fields:
                fields.append(self.merge_key)

            # Filter conditions to only those that apply to this collection's fields
            col_fields = [f for f, info in self.metadata.items() if info.get("collection") == col]
            col_safe_conditions = self._filter_conditions_for_sql(conditions, col_fields)
            
            mongo_query = self._build_mongo_query(filters, col_safe_conditions)

            projection = {f: 1 for f in fields}
            projection["_id"] = 0

            cursor = self.mongo_db[col].find(
                mongo_query,
                projection
            )

            if order_by:
                direction = 1 if order == "asc" else -1
                cursor = cursor.sort(order_by, direction)

            if limit:
                cursor = cursor.limit(limit)

            docs = list(cursor)

            # print("MONGO RESULT:", docs)   # debug line

            results.extend(docs)

        return results

    # =========================
    # WHERE BUILDER
    # =========================
    def _build_where_clause(
            self,
            filters,
            conditions
    ):

        clauses = []
        values = []

        # simple equality filters
        for k, v in filters.items():

            clauses.append(f"{k} = ?")
            values.append(v)

        # complex conditions
        for cond in conditions:

            c, v = self._parse_condition_sql(cond)

            clauses.append(c)
            values.extend(v)

        return " AND ".join(clauses), values

    # =========================
    # SQL CONDITION PARSER
    # =========================
    def _parse_condition_sql(self, cond):

        if "or" in cond:

            parts = []
            values = []

            for c in cond["or"]:

                sql, val = self._parse_condition_sql(c)

                parts.append(sql)
                values.extend(val)

            return "(" + " OR ".join(parts) + ")", values

        field = cond["field"]
        op = cond["op"]
        value = cond["value"]

        sql_op = self.op_map[op]

        if op == "in":

            placeholders = ",".join(["?"] * len(value))

            return f"{field} IN ({placeholders})", value

        return f"{field} {sql_op} ?", [value]

    # =========================
    # MONGO CONDITION BUILDER
    # =========================
    def _build_mongo_query(self, filters, conditions):

        mongo_query = dict(filters)

        for cond in conditions:

            mongo_query.update(
                self._parse_condition_mongo(cond)
            )

        return mongo_query

    def _parse_condition_mongo(self, cond):

        if "or" in cond:

            return {
                "$or": [
                    self._parse_condition_mongo(c)
                    for c in cond["or"]
                ]
            }

        field = cond["field"]
        op = cond["op"]
        value = cond["value"]

        mongo_ops = {
            "eq": value,
            "ne": {"$ne": value},
            "gt": {"$gt": value},
            "gte": {"$gte": value},
            "lt": {"$lt": value},
            "lte": {"$lte": value},
            "like": {"$regex": value},
            "in": {"$in": value}
        }

        return {field: mongo_ops[op]}

    # =========================
    # MERGE RESULTS
    # =========================
    def _merge(self, sql_res, mongo_res, sql_filtered=False, mongo_filtered=False):
        sql_keys = {r.get(self.merge_key) for r in sql_res} if sql_filtered else None
        mongo_keys = {r.get(self.merge_key) for r in mongo_res} if mongo_filtered else None
        
        valid_keys = None
        if sql_keys is not None and mongo_keys is not None:
            valid_keys = sql_keys.intersection(mongo_keys)
        elif sql_keys is not None:
            valid_keys = sql_keys
        elif mongo_keys is not None:
            valid_keys = mongo_keys
            
        merged = {}
        for r in sql_res:
            key = r.get(self.merge_key)
            if valid_keys is None or key in valid_keys:
                merged[key] = r

        for r in mongo_res:
            key = r.get(self.merge_key)
            if valid_keys is None or key in valid_keys:
                if key not in merged:
                    merged[key] = {}
                merged[key].update(r)

        # Drop skeleton-only rows: records that have only the merge key
        # and no other real data (these are SQL-only rows with no Mongo match)
        result = []
        for row in merged.values():
            non_key_fields = {k: v for k, v in row.items()
                              if k != self.merge_key and v is not None and v != ""}
            if non_key_fields:
                result.append(row)
        return result

    # =========================
    # UPDATE
    # =========================
    def update(self, query):

        filters = query.get("filters", {})
        conditions = query.get("conditions", [])
        data = query.get("data", {})

        where_clause, values = self._build_where_clause(
            filters,
            conditions
        )

        sql_data = {}
        mongo_data = {}

        for f, v in data.items():

            info = self.metadata.get(f)

            if not info:
                continue

            if info["backend"] == "sql":

                sql_data.setdefault(
                    info["table"],
                    {}
                )[f] = v

            else:

                mongo_data.setdefault(
                    info["collection"],
                    {}
                )[f] = v

        # Ensure Atomicity + Isolation via BEGIN IMMEDIATE
        self.sql_conn.execute("BEGIN IMMEDIATE")
        try:
            cur = self.sql_conn.cursor()

            for table, row in sql_data.items():

                set_clause = ", ".join(
                    [f"{k} = ?" for k in row]
                )

                q = f"UPDATE {table} SET {set_clause}"

                if where_clause:
                    q += f" WHERE {where_clause}"

                cur.execute(
                    q,
                    list(row.values()) + values
                )

            # Mongo update
            mongo_query = self._build_mongo_query(
                filters,
                conditions
            )

            for col, doc in mongo_data.items():
                self.mongo_db[col].update_many(
                    mongo_query,
                    {"$set": doc}
                )

            self.sql_conn.execute("COMMIT")
        except Exception as e:
            try: self.sql_conn.execute("ROLLBACK")
            except: pass
            raise e

    # =========================
    # DELETE
    # =========================
    def delete(self, query):

        filters = query.get("filters", {})
        conditions = query.get("conditions", [])

        where_clause, values = self._build_where_clause(
            filters,
            conditions
        )

        tables = {
            info["table"]
            for info in self.metadata.values()
            if info["backend"] == "sql"
        }

        # Ensure Atomicity + Isolation via BEGIN IMMEDIATE
        self.sql_conn.execute("BEGIN IMMEDIATE")
        try:
            cur = self.sql_conn.cursor()

            for t in tables:

                q = f"DELETE FROM {t}"

                if where_clause:
                    q += f" WHERE {where_clause}"

                cur.execute(q, values)

            # Mongo delete
            mongo_query = self._build_mongo_query(
                filters,
                conditions
            )

            cols = {
                info["collection"]
                for info in self.metadata.values()
                if info["backend"] == "mongo"
            }

            for c in cols:
                self.mongo_db[c].delete_many(
                    mongo_query
                )

            self.sql_conn.execute("COMMIT")
        except Exception as e:
            try: self.sql_conn.execute("ROLLBACK")
            except: pass
            raise e

    def test_durability(self):

        record_id = f"durable_{datetime.now().timestamp()}"
        unified_query = {
            "operation": "insert",
            "data": {
                "username": "ayush",
                "spo2": 97,
                "device_model": "iPhone",
                "timezone": "IST",
                
            }
        }

        insert_result = self.execute(unified_query)
        if insert_result.get("status") != "ok":
            return {
                "test": "Durability",
                "passed": False,
                "reason": "Insert failed, durability cannot be verified"
            }

        # perform a fresh read query
        query = {"operation": "read",
            "fields": ["username", "spo2", "device_model", "timezone"],
            "filters": {
                "username": "ayush",
                "spo2": 97,
                "device_model": "iPhone",
                "timezone": "IST",
            }}
        read_result = self.execute(query)
        if len(read_result) == 0:
            return {
                "test": "Durability",
                "passed": False,
                "reason": "Committed data not found after insert"
            }
            
        # To truly test durability, we simulate a crash by spinning up a brand new engine
        # and checking if the data is permanently physically persisted.
        from sql_backend import get_connection
        from mongo_backend import get_db
        fresh_engine = type(self)(self.metadata, get_connection(), get_db())
        fresh_read_result = fresh_engine.execute(query)
        
        if len(fresh_read_result) == 0:
            return {
                "test": "Durability",
                "passed": False,
                "reason": "Data was lost after restarting database connections"
            }
            
        return {
            "test": "Durability",
            "passed": True,
            "reason": "Data correctly persisted physically to disk across connections"
        }
    
    def test_isolation(self):
        import threading
        import sqlite3
        username = f"ayush_iso_{datetime.now().timestamp()}"
        # Step 1 — insert initial record
        insert_query = {
            "operation": "insert",
            "data": {
                "username": username,
                "spo2": 95,
                "device_model": "iPhone",
                "timezone": "IST"
            }
        }
        insert_result = self.execute(insert_query)
        if insert_result.get("status") != "ok":
            return {
                "test": "Isolation",
                "passed": False,
                "reason": "Initial insert failed"
            }
        # Step 2 — concurrent updates using separate DB connections
        def update_spo2(new_value):
            # create fresh engine instance to ensure new SQL connection and true concurrency
            from sql_backend import get_connection
            from mongo_backend import get_db
            local_engine = type(self)(self.metadata, get_connection(), get_db())
            update_query = {
                "operation": "update",
                "filters": {
                    "username": username
                },
                "data": {
                    "spo2": new_value
                }
            }
            local_engine.execute(update_query)
        t1 = threading.Thread(target=update_spo2, args=(96,))
        t2 = threading.Thread(target=update_spo2, args=(99,))
        t1.start()
        t2.start()
        t2.join()
        t1.join()
        # Step 3 — read final value
        read_query = {
            "operation": "read",
            "fields": [
                "username",
                "spo2",
                "device_model",
                "timezone"
            ],
            "filters": {
                "username": username
            }
        }
        result = self.execute(read_query)
        if len(result) == 0:
            return {
                "test": "Isolation",
                "passed": False,
                "reason": "Record missing after concurrent updates"
            }
        final_spo2 = result[0]["spo2"]
        if final_spo2 in ["96", "99"]:
            return {
                "test": "Isolation",
                "passed": True,
                "reason": f"Final consistent value after concurrent updates: {final_spo2}"
            }
        return {
            "test": "Isolation",
            "passed": False,
            "reason": f"Inconsistent value detected: {final_spo2}"
        }
    

    def test_consistency(self):
        """
        Consistency Test
        Ensure transaction preserves valid unified record structure
        across SQL and Mongo.
        Record must remain mergeable using sys_ingested_at.
        """
        username = f"ayush_consistency_{datetime.now().timestamp()}"
        # Step 1 — insert unified record
        insert_query = {
            "operation": "insert",
            "data": {
                "username": username,
                "spo2": 97,
                "device_model": "iPhone",
                "timezone": "IST"
            }
        }
        insert_result = self.execute(insert_query)
        if insert_result.get("status") != "ok":
            return {
                "test": "Consistency",
                "passed": False,
                "reason": "Initial insert failed"
            }
        # Step 2 — update SQL field
        update_query = {
            "operation": "update",
            "filters": {
                "username": username
            },
            "data": {
                "spo2": 98
            }
        }
        self.execute(update_query)
        # Step 3 — read merged record
        read_query = {
            "operation": "read",
            "fields": [
                "username",
                "spo2",
                "device_model",
                "timezone"
            ],
            "filters": {
                "username": username
            }
        }
        result = self.execute(read_query)
        if len(result) == 0:
            return {
                "test": "Consistency",
                "passed": False,
                "reason": "Record missing after update"
            }
        record = result[0]
        # check all required fields still present
        required_fields = [
            "username",
            "spo2",
            "device_model",
            "timezone"
        ]

        for field in required_fields:
            if field not in record:
                return {
                    "test": "Consistency",
                    "passed": False,
                    "reason": f"Missing field after transaction: {field}"
                }

        return {
            "test": "Consistency",
            "passed": True,
            "reason": "Record structure preserved across SQL and Mongo"
        }
    

    def test_atomicity(self):
        """
        Atomicity Test
        Artificial failure occurs during insert.
        SQL changes must rollback completely.
        """
        username = f"ayush_atomic_test_{datetime.now().timestamp()}"
        insert_query = {
            "operation": "insert",
            "__fail_mongo": True,
            "data": {
                "username": username,
                "spo2": 99,
            }
        }
        try:
            self.execute(insert_query)
        except:
            pass
        # verify rollback happened
        read_query = {
            "operation": "read",
            "fields": [
                "username",
                "spo2"
            ],
            "filters": {
                "username": username
            }
        }
        result = self.execute(read_query)
        if len(result) == 0:
            return {
                "test": "Atomicity",
                "passed": True,
                "reason": "Transaction rolled back successfully after failure"
            }
        return {
            "test": "Atomicity",
            "passed": False,
            "reason": "Partial data still present after failure"
        }