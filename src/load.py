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
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=indent)
    
    print(f"Saved {len(data)} records to {os.path.basename(filepath)}")


def load_processed_apps():
    print(f"Loading from {APPS_METADATA_PROCESSED}")
    with open(APPS_METADATA_PROCESSED, 'r', encoding='utf-8') as f:
        return json.load(f)


def load_processed_reviews():
    print(f"Loading from {APPS_REVIEWS_PROCESSED}")
    with open(APPS_REVIEWS_PROCESSED, 'r', encoding='utf-8') as f:
        return json.load(f)


def load_analytics_data():
    print(f"Loading from {APPS_WITH_METRICS}")
    with open(APPS_WITH_METRICS, 'r', encoding='utf-8') as f:
        return json.load(f)
