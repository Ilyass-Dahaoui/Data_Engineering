"""
Configuration file for the data pipeline.
Contains paths and constants used across different pipeline stages.
"""
import os

# Project root directory
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Data directories
RAW_DATA_DIR = os.path.join(PROJECT_ROOT, "DATA", "raw")
PROCESSED_DATA_DIR = os.path.join(PROJECT_ROOT, "DATA", "processed")

# Raw data files
APPS_METADATA_RAW = os.path.join(RAW_DATA_DIR, "apps_metadata.json")
APPS_REVIEWS_RAW = os.path.join(RAW_DATA_DIR, "apps_reviews.json")

# Processed data files
APPS_METADATA_PROCESSED = os.path.join(PROCESSED_DATA_DIR, "apps_metadata_clean.json")
APPS_REVIEWS_PROCESSED = os.path.join(PROCESSED_DATA_DIR, "apps_reviews_clean.json")
REVIEWS_AGGREGATED = os.path.join(PROCESSED_DATA_DIR, "reviews_aggregated.json")
APPS_WITH_METRICS = os.path.join(PROCESSED_DATA_DIR, "apps_with_metrics.json")

# Ensure directories exist
os.makedirs(RAW_DATA_DIR, exist_ok=True)
os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)
