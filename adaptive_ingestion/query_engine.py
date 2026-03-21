from datetime import datetime, timezone

class HybridQueryEngine:
    def __init__(self, metadata, sql_conn, mongo_db):
        self.metadata = metadata
        self.sql_conn = sql_conn
        self.mongo_db = mongo_db
        self.merge_key = "sys_ingested_at"

    # EXECUTE
    def execute(self, query):
        op = query.get("operation")

        if op == "read":
            return self.read(query)
        elif op == "insert":
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

    # INSERT
    def insert(self, query):
        data = query.get("filters", {})

        # Generate ingestion time (merge key)
        sys_ingested_at = datetime.now(timezone.utc).isoformat()
        data[self.merge_key] = sys_ingested_at

        sql_data = {}
        mongo_data = {}

        for field, val in data.items():
            info = self.metadata.get(field)

            if not info:
                continue  # ignore unknown fields

            if info["backend"] == "sql":
                sql_data.setdefault(info["table"], {})[field] = val
            else:
                mongo_data.setdefault(info["collection"], {})[field] = val

        # SQL insert
        for table, row in sql_data.items():
            cols = ", ".join(row.keys())
            placeholders = ", ".join(["%s"] * len(row))

            q = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"
            self.sql_conn.cursor().execute(q, list(row.values()))

        # Mongo insert
        for col, doc in mongo_data.items():
            self.mongo_db[col].insert_one(doc)

        self.sql_conn.commit()

    # READ
    def read(self, query):
        fields = query.get("fields", [])
        filters = query.get("filters", {})

        # Always include merge key
        if self.merge_key not in fields:
            fields.append(self.merge_key)

        return self.find_records(fields, filters)

    # FIND
    def find_records(self, fields, filters):
        sql_plan = {}
        mongo_plan = {}
        
        if "sys_ingested_at" not in fields:
            fields.append("sys_ingested_at")

        for field in fields:
            info = self.metadata.get(field)
            if not info:
                continue
            if info["backend"] == "sql":
                sql_plan.setdefault(info["table"], []).append(field)
            else:
                if field == 'sys_ingested_at':
                    sql_plan.setdefault(info["table"], []).append(field)
                mongo_plan.setdefault(info["collection"], []).append(field)

        # SQL
        sql_results = []
        with self.sql_conn:                        # ← wrap in context manager
            cur = self.sql_conn.cursor()
            for table, flds in sql_plan.items():
                q, vals = self._build_sql_select(table, flds, filters)
                cur.execute(q, vals)
                cols = [d[0] for d in cur.description]
                rows = cur.fetchall()
                for row in rows:
                    sql_results.append(dict(zip(cols, row)))

        # Mongo
        mongo_results = []
        for col, flds in mongo_plan.items():
            docs = list(self.mongo_db[col].find(
                filters,
                {f: 1 for f in flds}
            ))
            mongo_results.extend(docs)

        return self._merge(sql_results, mongo_results)


    def _build_sql_select(self, table, fields, filters):
        fields_str = ", ".join(fields)

        if filters:
            cond = " AND ".join([f"{k} = ?" for k in filters])
            return f"SELECT {fields_str} FROM {table} WHERE {cond}", list(filters.values())

        return f"SELECT {fields_str} FROM {table}", []

    # MERGE (USING sys_ingested_at)
    def _merge(self, sql_res, mongo_res):
        merged = {}

        for row in sql_res:
            key = row.get(self.merge_key)
            merged[key] = row

        for doc in mongo_res:
            key = doc.get(self.merge_key)

            if key not in merged:
                merged[key] = {}
            merged[key].update(doc)

        return list(merged.values())

    # UPDATE
    def update(self, query):
        filters = query.get("filters", {})
        data = query.get("data", {})

        if not filters:
            raise ValueError("Update requires filters")

        if not data:
            raise ValueError("Update requires data")

        sql_data = {}
        mongo_data = {}

        # Split data
        for field, value in data.items():
            info = self.metadata.get(field)

            if not info:
                continue

            if info["backend"] == "sql":
                table = info["table"]
                sql_data.setdefault(table, {})[field] = value
            else:
                collection = info["collection"]
                mongo_data.setdefault(collection, {})[field] = value


        # SQL UPDATE
        with self.sql_conn:                          # ← context manager auto-commits
            cur = self.sql_conn.cursor()
            for table, row in sql_data.items():
                set_clause = ", ".join([f"{k} = ?" for k in row])
                cond = " AND ".join([f"{k} = ?" for k in filters])
                query_sql = f"UPDATE {table} SET {set_clause} WHERE {cond}"
                values = list(row.values()) + list(filters.values())
                cur.execute(query_sql, values)

            self.sql_conn.commit()  # ← explicit commit
            cur.close()
        # MONGO UPDATE
        for collection, doc in mongo_data.items():
            if self.debug:
                print("[MONGO UPDATE]:", collection, filters, doc)

            self.mongo_db[collection].update_many(
                filters,
                {"$set": doc}
            )

    # DELETE
    def delete(self, query):
        fields = query.get("fields")
        filters = query.get("filters", {})

        if not filters:
            raise ValueError("Delete requires filters")

        # FIELD DELETE
        if fields:
            sql_plan = {}
            mongo_plan = {}

            for f in fields:
                info = self.metadata[f]

                if info["backend"] == "sql":
                    sql_plan.setdefault(info["table"], []).append(f)
                else:
                    mongo_plan.setdefault(info["collection"], []).append(f)

            # SQL → NULL
            for table, flds in sql_plan.items():
                set_clause = ", ".join([f"{f}=NULL" for f in flds])
                cond = " AND ".join([f"{k}=?" for k in filters])

                q = f"UPDATE {table} SET {set_clause} WHERE {cond}"
                self.sql_conn.cursor().execute(q, list(filters.values()))

            # Mongo → unset
            for col, flds in mongo_plan.items():
                self.mongo_db[col].update_many(
                    filters,
                    {"$unset": {f: "" for f in flds}}
                )

        # ROW DELETE
        else:
            tables = {info["table"] for info in self.metadata.values() if info["backend"] == "sql"}
            for t in tables:
                cond = " AND ".join([f"{k}=?" for k in filters])
                q = f"DELETE FROM {t} WHERE {cond}"
                self.sql_conn.cursor().execute(q, list(filters.values()))

            cols = {info["collection"] for info in self.metadata.values() if info["backend"] == "mongo"}
            for c in cols:
                self.mongo_db[c].delete_many(filters)

        self.sql_conn.commit()