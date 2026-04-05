import sqlite3
from metadata_store import update_sql_metadata

DB_NAME = "hybrid.db"


# SINGLE CONNECTION CREATOR
def get_connection():
    # Flask's dev server may handle requests on different threads.
    # Allow using the connection across threads (we still create/close per request in app.py).
    conn = sqlite3.connect(DB_NAME, timeout=10, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn


# GET EXISTING COLUMNS
def get_existing_columns(conn, table):
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    cols = [row[1] for row in cur.fetchall()]
    cur.close()
    return cols


# ADD MISSING COLUMNS
def add_missing_columns(conn, table, new_columns):
    cur = conn.cursor()

    cur.execute(f"PRAGMA table_info({table})")
    existing_cols = [row[1] for row in cur.fetchall()]

    for col in new_columns:
        if col not in existing_cols:
            cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} TEXT")

    cur.close()


# CREATE TABLE
def create_table(conn, table, columns, parent=None):
    cur = conn.cursor()

    col_defs = []

    for col in columns:
        if col != "sys_ingested_at":
            col_defs.append(f"{col} TEXT")

    col_defs.append("sys_ingested_at TEXT")

    if parent:
        col_defs.append(f"{parent.lower()}_id INTEGER")

    col_defs_str = ", ".join(col_defs)

    if parent:
        query = f"""
        CREATE TABLE IF NOT EXISTS {table} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            {col_defs_str},
            FOREIGN KEY({parent.lower()}_id) REFERENCES {parent}(id)
        );
        """
    else:
        query = f"""
        CREATE TABLE IF NOT EXISTS {table} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            {col_defs_str}
        );
        """

    cur.execute(query)
    cur.close()


# INSERT ROW
def insert_row(conn, table, data):
    cur = conn.cursor()

    add_missing_columns(conn, table, data.keys())

    keys = list(data.keys())
    values = [str(v) for v in data.values()]

    placeholders = ",".join(["?"] * len(keys))
    columns = ",".join(keys)

    query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"

    cur.execute(query, values)

    row_id = cur.lastrowid
    cur.close()

    return row_id


# NORMALIZATION ENGINE
def normalize_sql(conn, data, table_name, parent=None, parent_id=None):

    base_data = {}
    child_entities = []

    for key, value in data.items():

        if key == "id":
            continue

        if isinstance(value, (int, float, str, bool)):
            base_data[key] = value

        elif isinstance(value, dict):
            child_entities.append((key.upper(), value))

        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    child_entities.append((key.upper(), item))

    # Add ingestion time
    base_data["sys_ingested_at"] = data.get("sys_ingested_at")

    # Foreign key
    if parent:
        base_data[f"{parent.lower()}_id"] = parent_id

    # Create table
    create_table(conn, table_name, base_data.keys(), parent)

    # Update metadata
    update_sql_metadata(base_data.keys(), table_name, parent)

    # Insert row
    current_id = insert_row(conn, table_name, base_data)

    # Process children
    for child_table, child_data in child_entities:
        normalize_sql(conn, child_data, child_table, table_name, current_id)


# INSERT ENTRY POINT
def insert_sql(conn, record, sql_fields):

    filtered = {k: v for k, v in record.items() if k in sql_fields}

    root_table = "USERS"

    update_sql_metadata(sql_fields, "USERS", None)

    normalize_sql(conn, filtered, root_table)