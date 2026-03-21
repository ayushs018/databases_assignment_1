# mongo_backend.py
from pymongo import MongoClient
from datetime import datetime, timezone
from metadata_store import update_mongo_metadata
# CONFIG 
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME ="hybrid_db"
ROOT_COLLECTION  = "users"

# Thresholds for embed vs reference decision
MAX_EMBED_ITEMS = 5       # if array has more than this -> reference
MAX_EMBED_FIELDS = 10     # if nested object has more fields than this -> reference

# CONNECTION 
def get_db():
    client = MongoClient(MONGO_URI)
    return client[DB_NAME]


# EMBEDDING vs REFERENCING DECISION 
def should_embed(value):
    """
    Automatically decides if a nested value should be embedded or referenced.

    EMBED when:
    - It's a dict with few fields (small, self-contained object)
    - It's a list with few items

    REFERENCE when:
    - List is long (repeating group, one-to-many)
    - Dict is large (could be its own entity)
    """

    if isinstance(value, dict):
        # Large nested object -> reference (separate collection)
        if len(value.keys()) > MAX_EMBED_FIELDS:
            return False
        return True

    if isinstance(value, list):
        # Empty or small list -> embed
        if len(value) == 0:
            return True
        # Long list -> reference
        if len(value) > MAX_EMBED_ITEMS:
            return False
        # List of primitives -> always embed
        if all(isinstance(i, (str, int, float, bool)) for i in value):
            return True
        # List of objects -> reference (repeating group = separate collection)
        if all(isinstance(i, dict) for i in value):
            return False
        return True

    return True  # primitives always embed


# DOCUMENT DECOMPOSITION 
def decompose_document(record, mongo_fields):
    """
    Splits a record into:
    - root_doc: fields to embed in the root document
    - references: {collection_name: [list of documents]} to store separately
    """

    root_doc = {}
    references = {}  # collection_name -> list of sub-documents

    for key, value in record.items():

        # only process 5 routed to mongo
        if key not in mongo_fields:
            continue

        # ---------- CASE 1: Primitive -> always embed ----------
        if isinstance(value, (str, int, float, bool)) or value is None:
            root_doc[key] = value

        # ---------- CASE 2: Nested dict ----------
        elif isinstance(value, dict):
            if should_embed(value):
                # Embed directly inside root document
                root_doc[key] = value
            else:
                # Store as separate collection, keep reference _id
                references[key] = [value]

        # ---------- CASE 3: Array ----------
        elif isinstance(value, list):
            if should_embed(value):
                # Small list or list of primitives -> embed
                root_doc[key] = value
            else:
                # Repeating group of objects -> separate collection
                references[key] = value

    return root_doc, references


# INSERT REFERENCES 
def insert_references(db, references, parent_id, parent_collection):
    """
    Inserts referenced sub-documents into their own collections.
    Stores parent_id as foreign reference.
    Returns a dict of {field_name: [inserted_ids]} for root doc linkage.
    """

    ref_links = {}

    for collection_name, docs in references.items():
        full_collection_name = f"{parent_collection}_{collection_name}"  # e.g. users_comments
        collection = db[full_collection_name]

        inserted_ids = []
        for doc in docs:
            sub_root, sub_refs = decompose_document(doc, doc.keys())

            sub_root[f"{parent_collection}_id"] = parent_id
            sub_root["sys_ingested_at"] = datetime.now(timezone.utc).isoformat()
            result = collection.insert_one(sub_root)
            new_id = result.inserted_id
            inserted_ids.append(new_id)
            if sub_refs:
                insert_references(db, sub_refs, new_id, full_collection_name)

        # Store reference IDs in root doc
        ref_links[f"{collection_name}_refs"] = [str(i) for i in inserted_ids]
        update_mongo_metadata([collection_name], full_collection_name, parent_collection)
        print(f"  [MongoDB] Referenced {len(docs)} doc(s) -> collection: '{full_collection_name}'")

    return ref_links


# MAIN ENTRY POINT 
def insert_mongo(record, mongo_fields):
    """
    Entry point called from main.py.

    Strategy:
    1. Decompose record into root doc + references
    2. Insert root doc into ROOT_COLLECTION
    3. Insert references into sub-collections
    4. Update root doc with reference IDs
    """

    db = get_db()

    # Step 1: Decompose
    root_doc, references = decompose_document(record, mongo_fields)

    # Step 2: Insert root doc first (to get its _id)
    root_doc["sys_ingested_at"] = record.get("sys_ingested_at")
    result = db[ROOT_COLLECTION].insert_one(root_doc)
    parent_id = result.inserted_id
    update_mongo_metadata(root_doc.keys(), ROOT_COLLECTION, None)

    print(f"  [MongoDB] Inserted root doc into '{ROOT_COLLECTION}' -> _id: {parent_id}")

    # Step 3: Insert references, get back reference IDs
    if references:
        ref_links = insert_references(db, references, parent_id, ROOT_COLLECTION)

        # Step 4: Update root doc with reference links
        db[ROOT_COLLECTION].update_one(
            {"_id": parent_id},
            {"$set": ref_links}
        )
        print(f"  [MongoDB] Updated root doc with reference links: {list(ref_links.keys())}")