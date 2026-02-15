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
