import json
import os
from typing import List, Dict, Any
import config


def load_json_file(filepath: str) -> List[Dict[str, Any]]:
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"File not found: {filepath}")
    
    with open(filepath, 'r', encoding='utf-8') as f:
        try:
            data = json.load(f)
            if isinstance(data, list):
                return data
            elif isinstance(data, dict):
                return [data]
        except json.JSONDecodeError:
            f.seek(0)
            data = []
            for line in f:
                line = line.strip()
                if line:
                    try:
                        data.append(json.loads(line))
                    except json.JSONDecodeError as e:
                        print(f"Warning: Skipping invalid JSON line: {e}")
            return data


def ingest_apps_metadata() -> List[Dict[str, Any]]:
    print("Ingesting apps metadata...")
    data = load_json_file(config.APPS_METADATA_RAW)
    print(f"Loaded {len(data)} app records")
    return data


def ingest_apps_reviews() -> List[Dict[str, Any]]:
    print("Ingesting apps reviews...")
    data = load_json_file(config.APPS_REVIEWS_RAW)
    print(f"Loaded {len(data)} review records")
    return data
