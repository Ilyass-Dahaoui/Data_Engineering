"""
Ingestion module: Read raw data files.
Handles loading JSON data with various formats (JSON array, JSONL).
"""
import json
import os
from typing import List, Dict, Any
from config import APPS_METADATA_RAW, APPS_REVIEWS_RAW


def load_json_file(filepath: str) -> List[Dict[str, Any]]:
    """
    Load JSON data from a file.
    Supports both JSON array format and JSONL (newline-delimited JSON).
    
    Args:
        filepath: Path to the JSON file
        
    Returns:
        List of dictionaries containing the data
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"File not found: {filepath}")
    
    with open(filepath, 'r', encoding='utf-8') as f:
        try:
            # Try to load as standard JSON array
            data = json.load(f)
            if isinstance(data, list):
                return data
            elif isinstance(data, dict):
                return [data]
        except json.JSONDecodeError:
            # If standard JSON fails, try JSONL format
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
    """
    Ingest apps metadata from raw data file.
    
    Returns:
        List of app metadata dictionaries
    """
    print("Ingesting apps metadata...")
    data = load_json_file(APPS_METADATA_RAW)
    print(f"Loaded {len(data)} app records")
    return data


def ingest_apps_reviews() -> List[Dict[str, Any]]:
    """
    Ingest apps reviews from raw data file.
    
    Returns:
        List of review dictionaries
    """
    print("Ingesting apps reviews...")
    data = load_json_file(APPS_REVIEWS_RAW)
    print(f"Loaded {len(data)} review records")
    return data


if __name__ == "__main__":
    # Test ingestion
    print("Testing data ingestion...")
    try:
        apps = ingest_apps_metadata()
        print(f"✓ Apps metadata: {len(apps)} records")
        
        reviews = ingest_apps_reviews()
        print(f"✓ Apps reviews: {len(reviews)} records")
        
        print("\nSample app record:")
        if apps:
            print(f"  App ID: {apps[0].get('appId')}")
            print(f"  Title: {apps[0].get('title')}")
            
        print("\nSample review record:")
        if reviews:
            print(f"  App ID: {reviews[0].get('app_id')}")
            print(f"  Score: {reviews[0].get('score')}")
            
    except Exception as e:
        print(f"✗ Error: {e}")
