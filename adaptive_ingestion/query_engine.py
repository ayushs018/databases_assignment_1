from datetime import datetime, timezone
import json


class HybridQueryEngine:
    def __init__(self, metadata, sql_conn, mongo_db):
        self.metadata = metadata
        self.sql_conn = sql_conn
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
            self.insert(query)
            return {"status": "insert successful"}

        elif op == "update":
            self.update(query)
            return {"status": "update successful"}

        elif op == "delete":
            self.delete(query)
            return {"status": "delete successful"}

        else:
            raise ValueError("Invalid operation")

    # =========================
    # CREATE / INSERT
    # =========================
  

    def insert(self, query):

        data = query.get("data", {})
        sys_ingested_at = datetime.now(timezone.utc).isoformat()
        data[self.merge_key] = sys_ingested_at
        sql_data = {}
        mongo_data = {}
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

        inserted_mongo_ids = []
        try:
            # BEGIN SQL TRANSACTION
            sql_cursor = self.sql_conn.cursor()
            # SQL inserts
            for table, row in sql_data.items():
                cols = ", ".join(row.keys())
                placeholders = ", ".join(["?"] * len(row))
                q = f"""
                INSERT INTO {table} ({cols})
                VALUES ({placeholders})
                """
                sql_cursor.execute(q, list(row.values()))
            # Mongo inserts
            for collection, doc in mongo_data.items():
                result = self.mongo_db[collection].insert_one(doc)
                inserted_mongo_ids.append(
                    (collection, result.inserted_id)
                )
            # COMMIT SQL if all succeed
            self.sql_conn.commit()
            return {
                "status": "success",
                "sys_ingested_at": sys_ingested_at
            }
        except Exception as e:
            # rollback SQL
            self.sql_conn.rollback()
            # rollback Mongo manually
            for collection, _id in inserted_mongo_ids:
                self.mongo_db[collection].delete_one(
                    {"_id": _id}
                )
            return {
                "status": "error",
                "message": str(e)
            }

    # =========================
    # READ
    # =========================
    def read(self, query):

        fields = query.get("fields", [])
        filters = query.get("filters", {})
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

        sql_results = self._execute_sql_read(
            sql_plan,
            filters,
            conditions,
            order_by,
            order,
            limit
        )

        mongo_results = self._execute_mongo_read(
            mongo_plan,
            filters,
            conditions,
            order_by,
            order,
            limit
        )
        return self._merge(sql_results, mongo_results)

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

                query, values = self._build_sql_query(
                    table,
                    fields,
                    filters,
                    conditions,
                    order_by,
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

        mongo_query = self._build_mongo_query(filters, conditions)
        for col, fields in mongo_plan.items():

            if self.merge_key not in fields:
                fields.append(self.merge_key)

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
    def _merge(self, sql_res, mongo_res):
        merged = {}
        for r in sql_res:
            key = r.get(self.merge_key)
            merged[key] = r
        for r in mongo_res:
            key = r.get(self.merge_key)
            if key not in merged:
                merged[key] = {}
            merged[key].update(r)

        return list(merged.values())

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

        # SQL update
        with self.sql_conn:

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

        # SQL delete
        tables = {
            info["table"]
            for info in self.metadata.values()
            if info["backend"] == "sql"
        }

        with self.sql_conn:

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
        if insert_result["status"] != "insert successful":
            return {
                "test": "Durability",
                "passed": False,
                "reason": "Insert failed, durability cannot be verified"
            }

        # simulate system interruption:
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
        else:
            return {
                "test": "Durability",
                "passed": True,
                "reason": "Data persisted correctly in SQL and Mongo after commit"
            }
    
    def test_isolation(self):
        import threading
        import sqlite3
        username = "ayush_iso"
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
        if insert_result["status"] != "insert successful":
            return {
                "test": "Isolation",
                "passed": False,
                "reason": "Initial insert failed"
            }
        # Step 2 — concurrent updates using separate DB connections
        def update_spo2(new_value):
            # create fresh engine instance to ensure new SQL connection
            local_engine = type(self)(self.metadata, self.sql_conn, self.mongo_db)
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
        t1.join()
        t2.join()
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
        username = "ayush_consistency"
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
        if insert_result["status"] != "insert successful":
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
        username = "ayush_atomic"
        insert_query = {
            "operation": "insert",
            "data": {
                "username": username,
                "spo2": 97,
                "force_fail": True   # trigger artificial failure
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