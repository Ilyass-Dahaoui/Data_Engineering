import json
import os
import csv
import re
from typing import List, Dict, Any, Union
import config

# regex patterns for numeric detection
type_patterns = {
    'int': re.compile(r"^-?\d+$"),
    'float': re.compile(r"^-?\d+\.?\d*$"),
}

def _parse_value(val: Any) -> Union[str, int, float, None]:
    if val is None:
        return None
    v = str(val).strip()
    if v == '' or v.upper() in ('NULL', 'NONE'):
        return None
    if type_patterns['int'].match(v):
        return int(v)
    if type_patterns['float'].match(v):
        return float(v)
    return v



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


def load_csv_file(filepath: str) -> List[Dict[str, Any]]:
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"CSV not found: {filepath}")
    rows: List[Dict[str, Any]] = []
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for r in reader:
            normalized = {k: _parse_value(v) for k, v in r.items()}
            rows.append(normalized)
    return rows


def ingest_apps_metadata() -> List[Dict[str, Any]]:
    """Read applications metadata from raw JSON and any supplemental CSVs."""
    print("Ingesting apps metadata...")
    data: List[Dict[str, Any]] = []
    try:
        data.extend(load_json_file(config.APPS_METADATA_RAW))
    except FileNotFoundError:
        print("No primary metadata JSON found")
    # extra CSV resources
    for fname in os.listdir(config.RAW_DATA_DIR):
        if fname.endswith('.csv') and 'apps' in fname:
            try:
                data.extend(load_csv_file(os.path.join(config.RAW_DATA_DIR, fname)))
            except Exception as e:
                print(f"Skipping CSV {fname}: {e}")
    print(f"Loaded {len(data)} app records")
    return data


def ingest_apps_reviews() -> List[Dict[str, Any]]:
    """Read application reviews from raw JSON and any supplemental CSVs/batches."""
    print("Ingesting apps reviews...")
    data: List[Dict[str, Any]] = []
    try:
        data.extend(load_json_file(config.APPS_REVIEWS_RAW))
    except FileNotFoundError:
        print("No primary reviews JSON found")
    # ingest CSV batches
    for fname in os.listdir(config.RAW_DATA_DIR):
        if fname.endswith('.csv') and 'note_taking' in fname:
            try:
                data.extend(load_csv_file(os.path.join(config.RAW_DATA_DIR, fname)))
            except Exception as e:
                print(f"Skipping CSV {fname}: {e}")
    print(f"Loaded {len(data)} review records")
    return data
