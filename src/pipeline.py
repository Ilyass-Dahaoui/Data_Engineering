import sys
from datetime import datetime

from ingest import ingest_apps_metadata, ingest_apps_reviews
from transform import clean_apps_metadata, clean_apps_reviews, transform_for_analytics
from load import (
    save_json,
    load_processed_apps_scd2,
    load_processed_reviews
)
import config
from utils import merge_reviews, scd2_update
from quality import check_apps_metadata, check_reviews


def _print_quality_report(app_issues, review_issues):
    print("\n" + "=" * 60)
    print("DATA QUALITY CHECKS")
    print("=" * 60)
    if not app_issues and not review_issues:
        print("No obvious quality issues detected.")
    if app_issues:
        print(f"\nApps metadata issues ({len(app_issues)}):")
        for issue in app_issues[:10]:
            print("  -", issue)
    if review_issues:
        print(f"\nReview issues ({len(review_issues)}):")
        for issue in review_issues[:10]:
            print("  -", issue)



def run_pipeline():
    start_time = datetime.now()
    print("=" * 60)
    print("STARTING DATA PIPELINE")
    print("=" * 60)
    print(f"Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    try:
        print("\n" + "=" * 60)
        print("STAGE 1: DATA INGESTION")
        print("=" * 60)
        raw_apps = ingest_apps_metadata()
        raw_reviews = ingest_apps_reviews()
        
        print("\n" + "=" * 60)
        print("STAGE 2: DATA TRANSFORMATION")
        print("=" * 60)
        clean_apps = clean_apps_metadata(raw_apps)
        clean_reviews = clean_apps_reviews(raw_reviews)

        # data quality checks
        app_issues = check_apps_metadata(clean_apps)
        review_issues = check_reviews(clean_reviews)
        _print_quality_report(app_issues, review_issues)

        # SCD2 update for apps metadata
        existing_history = load_processed_apps_scd2()
        updated_history = scd2_update(existing_history, clean_apps)
        # derive current snapshot for analytics from history
        current_apps = [r for r in updated_history if r.get('current_flag')]

        # incremental merge for reviews
        existing_reviews = []
        try:
            existing_reviews = load_processed_reviews()
        except FileNotFoundError:
            existing_reviews = []
        merged_reviews = merge_reviews(existing_reviews, clean_reviews)

        print("\nAggregating data for analytics using current snapshot...")
        analytics_data = transform_for_analytics(current_apps, merged_reviews)

        # also build star schema tables (dim/fact) if caller wants them
        star = None
        try:
            from transform import build_star_schema
            star = build_star_schema(current_apps, merged_reviews)
        except ImportError:
            star = None

        print("\n" + "=" * 60)
        print("STAGE 3: DATA LOADING")
        print("=" * 60)
        # write metadata snapshot and history
        save_json(current_apps, config.APPS_METADATA_PROCESSED)
        save_json(updated_history, config.APPS_METADATA_SCD2)
        save_json(merged_reviews, config.APPS_REVIEWS_PROCESSED)
        save_json(analytics_data, config.APPS_WITH_METRICS)
        if star is not None:
            save_json(star['dim_apps'], config.DIM_APPS)
            save_json(star['dim_categories'], config.DIM_CATEGORIES)
            save_json(star['dim_developers'], config.DIM_DEVELOPERS)
            save_json(star['dim_date'], config.DIM_DATE)
            save_json(star['fact_reviews'], config.FACT_REVIEWS)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print("\n" + "=" * 60)
        print("PIPELINE COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        print(f"Duration: {duration:.2f} seconds")
        print(f"\nData Summary:")
        print(f"  • Apps current records: {len(current_apps)}")
        print(f"  • Total historical app rows: {len(updated_history)}")
        print(f"  • Reviews after merge: {len(merged_reviews)}")
        print(f"  • Analytics records: {len(analytics_data)}")
        print(f"\nProcessed files saved to: {config.PROCESSED_DATA_DIR}")
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
