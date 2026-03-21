import json
import os

METADATA_FILE = "metadata.json"

def load_metadata():
    """
    Load metadata from disk if it exists.
    """
    if os.path.exists(METADATA_FILE):
        with open(METADATA_FILE, "r") as f:
            return json.load(f)
    return {}

def save_metadata(metadata: dict):
    """
    Persist metadata to disk.
    """
    with open(METADATA_FILE, "w") as f:
        json.dump(metadata, f, indent=2)



QUERY_META_FILE = "query_metadata.json"


def load_query_metadata():
    if os.path.exists(QUERY_META_FILE):
        with open(QUERY_META_FILE, "r") as f:
            return json.load(f)
    return {}


def save_query_metadata(metadata):
    with open(QUERY_META_FILE, "w") as f:
        json.dump(metadata, f, indent=2)


# SQL
def update_sql_metadata(fields, table, parent):

    meta = load_query_metadata()

    for field in fields:
        meta.setdefault(field, {})
        meta[field]["backend"] = "sql"
        meta[field]["table"] = table
        meta[field]["parent"] = parent

    save_query_metadata(meta)


# MONGO 
def update_mongo_metadata(fields, collection, parent_collection):

    meta = load_query_metadata()

    for field in fields:
        meta.setdefault(field, {})
        meta[field]["backend"] = "mongo"
        meta[field]["collection"] = collection
        meta[field]["parent"] = parent_collection

    save_query_metadata(meta)
