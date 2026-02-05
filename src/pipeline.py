"""
Main pipeline orchestrator.
Coordinates the end-to-end data pipeline: Ingest -> Transform -> Load.
"""
import sys
from datetime import datetime

from ingest import ingest_apps_metadata, ingest_apps_reviews
from transform import clean_apps_metadata, clean_apps_reviews, transform_for_analytics
from load import (
    save_json,
    APPS_METADATA_PROCESSED,
    APPS_REVIEWS_PROCESSED,
    APPS_WITH_METRICS
)


def run_pipeline():
    """
    Execute the complete data pipeline.
    
    Pipeline stages:
    1. Ingestion: Read raw data files
    2. Transformation: Clean and transform data
    3. Loading: Write processed data
    """
    start_time = datetime.now()
    print("=" * 60)
    print("STARTING DATA PIPELINE")
    print("=" * 60)
    print(f"Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    try:
        # STAGE 1: INGESTION
        print("\n" + "=" * 60)
        print("STAGE 1: DATA INGESTION")
        print("=" * 60)
        raw_apps = ingest_apps_metadata()
        raw_reviews = ingest_apps_reviews()
        
        # STAGE 2: TRANSFORMATION
        print("\n" + "=" * 60)
        print("STAGE 2: DATA TRANSFORMATION")
        print("=" * 60)
        clean_apps = clean_apps_metadata(raw_apps)
        clean_reviews = clean_apps_reviews(raw_reviews)
        
        print("\nAggregating data for analytics...")
        analytics_data = transform_for_analytics(clean_apps, clean_reviews)
        
        # STAGE 3: LOADING
        print("\n" + "=" * 60)
        print("STAGE 3: DATA LOADING")
        print("=" * 60)
        save_json(clean_apps, APPS_METADATA_PROCESSED)
        save_json(clean_reviews, APPS_REVIEWS_PROCESSED)
        save_json(analytics_data, APPS_WITH_METRICS)
        
        # SUMMARY
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print("\n" + "=" * 60)
        print("PIPELINE COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        print(f"Duration: {duration:.2f} seconds")
        print(f"\nData Summary:")
        print(f"  • Apps processed: {len(clean_apps)}")
        print(f"  • Reviews processed: {len(clean_reviews)}")
        print(f"  • Analytics records: {len(analytics_data)}")
        print(f"\nProcessed files saved to: DATA/processed/")
        print("=" * 60)
        
        return True
        
    except Exception as e:
        print("\n" + "=" * 60)
        print("PIPELINE FAILED!")
        print("=" * 60)
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = run_pipeline()
    sys.exit(0 if success else 1)
