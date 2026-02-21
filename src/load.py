import json
import os
from typing import List, Dict, Any
import config


def save_json(data: List[Dict[str, Any]], filepath: str, indent: int = 2) -> None:
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=indent)
    
    print(f"Saved {len(data)} records to {os.path.basename(filepath)}")


def load_processed_apps():
    path = config.APPS_METADATA_PROCESSED
    print(f"Loading from {path}")
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def load_processed_apps_scd2():
    # history table may not exist on first run
    path = config.APPS_METADATA_SCD2
    if not os.path.exists(path):
        return []
    print(f"Loading SCD2 history from {path}")
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def load_processed_reviews():
    path = config.APPS_REVIEWS_PROCESSED
    print(f"Loading from {path}")
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def load_analytics_data():
    path = config.APPS_WITH_METRICS
    print(f"Loading from {path}")
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)
