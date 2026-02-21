import os

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

RAW_DATA_DIR = os.path.join(PROJECT_ROOT, "DATA", "raw")
PROCESSED_DATA_DIR = os.path.join(PROJECT_ROOT, "DATA", "processed")

APPS_METADATA_RAW = os.path.join(RAW_DATA_DIR, "apps_metadata.json")
APPS_REVIEWS_RAW = os.path.join(RAW_DATA_DIR, "apps_reviews.json")

APPS_METADATA_PROCESSED = os.path.join(PROCESSED_DATA_DIR, "apps_metadata_clean.json")
APPS_METADATA_SCD2 = os.path.join(PROCESSED_DATA_DIR, "apps_metadata_scd2.json")  # history table for SCD2
APPS_REVIEWS_PROCESSED = os.path.join(PROCESSED_DATA_DIR, "apps_reviews_clean.json")
REVIEWS_AGGREGATED = os.path.join(PROCESSED_DATA_DIR, "reviews_aggregated.json")
APPS_WITH_METRICS = os.path.join(PROCESSED_DATA_DIR, "apps_with_metrics.json")

# star schema output paths
DIM_APPS = os.path.join(PROCESSED_DATA_DIR, "dim_apps.json")
DIM_CATEGORIES = os.path.join(PROCESSED_DATA_DIR, "dim_categories.json")
DIM_DEVELOPERS = os.path.join(PROCESSED_DATA_DIR, "dim_developers.json")
DIM_DATE = os.path.join(PROCESSED_DATA_DIR, "dim_date.json")
FACT_REVIEWS = os.path.join(PROCESSED_DATA_DIR, "fact_reviews.json")

os.makedirs(RAW_DATA_DIR, exist_ok=True)
os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)
