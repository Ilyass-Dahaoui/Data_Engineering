"""
Load module: Save processed data to the processed directory.
Handles writing data in various formats.
"""
import json
import os
from typing import List, Dict, Any
from config import (
    APPS_METADATA_PROCESSED,
    APPS_REVIEWS_PROCESSED,
    APPS_WITH_METRICS,
    PROCESSED_DATA_DIR
)


def save_json(data: List[Dict[str, Any]], filepath: str, indent: int = 2) -> None:
    """
    Save data to a JSON file.
    
    Args:
        data: Data to save
        filepath: Output file path
        indent: JSON indentation level
    """
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=indent)
    
    print(f"Saved {len(data)} records to {os.path.basename(filepath)}")


def load_processed_apps():
    """Load cleaned apps metadata."""
    print(f"Loading from {APPS_METADATA_PROCESSED}")
    with open(APPS_METADATA_PROCESSED, 'r', encoding='utf-8') as f:
        return json.load(f)


def load_processed_reviews():
    """Load cleaned apps reviews."""
    print(f"Loading from {APPS_REVIEWS_PROCESSED}")
    with open(APPS_REVIEWS_PROCESSED, 'r', encoding='utf-8') as f:
        return json.load(f)


def load_analytics_data():
    """Load analytics-ready data."""
    print(f"Loading from {APPS_WITH_METRICS}")
    with open(APPS_WITH_METRICS, 'r', encoding='utf-8') as f:
        return json.load(f)


if __name__ == "__main__":
    print("Testing load module...")
    print(f"Processed data directory: {PROCESSED_DATA_DIR}")
    print(f"Apps metadata: {APPS_METADATA_PROCESSED}")
    print(f"Apps reviews: {APPS_REVIEWS_PROCESSED}")
    print(f"Analytics data: {APPS_WITH_METRICS}")
