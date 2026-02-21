import json
import sys
from datetime import datetime
from collections import Counter, defaultdict

sys.path.append('.')
from config import APPS_METADATA_RAW, APPS_REVIEWS_RAW, APPS_WITH_METRICS

print("="*60)
print("PART 1: DATA QUALITY & EXPLORATION ANALYSIS")
print("="*60)

with open(APPS_METADATA_RAW, 'r', encoding='utf-8') as f:
    apps = json.load(f)

print("\n1. FIVE DATA QUALITY ISSUES:")
print("-" * 60)

issues = []

print("\n   Issue 1: Missing/Null Fields")
missing_fields = defaultdict(int)
for app in apps:
    for key in ['developer', 'updated', 'version', 'released']:
        if not app.get(key):
            missing_fields[key] += 1
for field, count in missing_fields.items():
    print(f"   - {count} apps missing '{field}' field")
    issues.append(f"Missing {field} in {count} apps")

print("\n   Issue 2: Inconsistent Install Format")
print(f"   - Installs stored as strings: '500,000+', '1,000,000+'")
print(f"   - Makes numeric comparison difficult")
issues.append("Installs field is string, not numeric")

print("\n   Issue 3: Duplicate/Inconsistent Data Types")
print(f"   - Some scores may be None/null vs 0")
print(f"   - Ratings vs reviews count discrepancy")
for app in apps[:3]:
    print(f"   - {app['title']}: score={app.get('score')}, ratings={app.get('ratings')}, reviews={app.get('reviews')}")
issues.append("Mixed data types for scores/ratings")

print("\n   Issue 4: Reviews Format Issues")
print("   - Reviews stored as JSONL (one per line) not JSON array")
print("   - File size: 56.58 MB - difficult to load into memory")
print("   - No built-in validation or schema")
issues.append("Reviews in JSONL format, massive file size")

print("\n   Issue 5: Temporal Data Inconsistency")
print("   - Timestamps may be strings, datetime objects, or null")
print("   - No standard timezone information")
print("   - 'updated' field may be date string or null")
issues.append("Inconsistent timestamp formats")

print("\n\n2. APP PERFORMANCE ANALYSIS:")
print("-" * 60)

with open(APPS_WITH_METRICS, 'r', encoding='utf-8') as f:
    analytics = json.load(f)

sorted_by_score = sorted(analytics, key=lambda x: x['review_metrics']['avg_score'], reverse=True)

print("\n   BEST PERFORMING (by avg review score):")
for i, app in enumerate(sorted_by_score[:5], 1):
    metrics = app['review_metrics']
    print(f"   {i}. {app['title']}")
    print(f"      Avg Score: {metrics['avg_score']:.2f} | Total Reviews: {metrics['total_reviews']}")

print("\n   WORST PERFORMING (by avg review score):")
for i, app in enumerate(sorted_by_score[-5:], 1):
    metrics = app['review_metrics']
    print(f"   {i}. {app['title']}")
    print(f"      Avg Score: {metrics['avg_score']:.2f} | Total Reviews: {metrics['total_reviews']}")

print("\n\n3. REVIEW VOLUME DIFFERENCES:")
print("-" * 60)

sorted_by_volume = sorted(analytics, key=lambda x: x['review_metrics']['total_reviews'], reverse=True)
print("\n   Highest volume:")
for app in sorted_by_volume[:5]:
    print(f"   - {app['title']}: {app['review_metrics']['total_reviews']:,} reviews")

print("\n   Lowest volume:")
for app in sorted_by_volume[-5:]:
    print(f"   - {app['title']}: {app['review_metrics']['total_reviews']:,} reviews")

volumes = [app['review_metrics']['total_reviews'] for app in analytics]
print(f"\n   Volume statistics:")
print(f"   - Max: {max(volumes):,} | Min: {min(volumes):,}")
print(f"   - Avg: {sum(volumes)//len(volumes):,} | Range: {max(volumes)-min(volumes):,}")

print("\n\n4. RATING TRENDS OVER TIME:")
print("-" * 60)
print("   Note: Full temporal analysis requires parsing review timestamps")
print("   from JSONL file. Current pipeline doesn't aggregate by time period.")
print("   Limitation: No time-series analysis in current implementation")

print("\n" + "="*60)
print("PART 2: PIPELINE ROBUSTNESS ANALYSIS")
print("="*60)

print("\n1. HANDLING NEW BATCHES:")
print("-" * 60)
print("   - Changes required: 0 (pipeline is batch-agnostic)")
print("   - Full refresh: EXPLICIT (extract_data.py removes existing file)")
print("   - Code line: 'os.remove(reviews_file)' in extract_data.py")

print("\n2. DUPLICATE REVIEWS:")
print("-" * 60)
print("   - Current handling: NOT HANDLED")
print("   - Reviews appended without deduplication")
print("   - Risk: If extraction runs multiple times, duplicates accumulate")
print("   - reviewId field exists but not used for deduplication")

print("\n3. ORPHANED REVIEWS:")
print("-" * 60)
print("   - Reviews without matching apps: SILENTLY IGNORED")
print("   - transform.py line: 'if app_id in app_dict'")
print("   - Behavior: Orphaned reviews dropped during analytics creation")
print("   - No warning/logging when reviews are dropped")

print("\n4. HARD-CODED COLUMN NAMES:")
print("-" * 60)
print("   - transform.py: 'appId', 'title', 'score', 'ratings'")
print("   - ingest.py: 'app_id', 'content', 'reviewId'")
print("   - Widespread: 20+ hard-coded field names across 3 files")
print("   - Risk: Schema changes break entire pipeline")

print("\n5. FAILURE BEHAVIOR:")
print("-" * 60)
print("   - Missing fields: SILENT FAILURE (uses .get() with defaults)")
print("   - Invalid types: SILENT FAILURE (try/except in date parsing)")
print("   - Example: 'score' field missing → defaults to 0")
print("   - Incorrect results propagate without warnings")

print("\n6. INVALID DATA HANDLING:")
print("-" * 60)
print("   - Invalid ratings: Filtered (if score is None, skipped)")
print("   - Invalid timestamps: Transformed to None")
print("   - Location: clean_apps_reviews() in transform.py")
print("   - Problem: No logging of how many records were affected")

print("\n7. DATA QUALITY SURFACING:")
print("-" * 60)
print("   - Issues surface: LATE (during aggregation)")
print("   - Silent impact on metrics (e.g., avg_score calculation)")
print("   - No early validation stage")
print("   - No data quality report generated")

print("\n8. NEW BUSINESS LOGIC:")
print("-" * 60)
print("   - New metric location: transform_for_analytics() function")
print("   - Parts requiring changes: 1-2 (transform.py, possibly load.py)")
print("   - Logic reusability: LOW (tightly coupled to aggregation)")
print("   - Separation: POOR (preparation mixed with analytics)")

print("\n9. CODE CHANGE LOCALIZATION:")
print("-" * 60)
print("   - Schema changes: WIDESPREAD (3+ files)")
print("   - New metrics: LOCALIZED (1 function)")
print("   - Data validation: WOULD BE WIDESPREAD")

print("\n\n" + "="*60)
print("SUMMARY: PIPELINE PAIN POINTS")
print("="*60)
print("1. No duplicate detection mechanism")
print("2. Silent failures hide data quality issues")
print("3. Hard-coded field names make schema brittle")
print("4. No separation between data prep and analytics")
print("5. No validation or data quality reporting")
print("6. Large file handling inefficient (56MB JSONL)")
print("7. No incremental processing capability")
print("8. Orphaned data silently dropped")
print("9. No time-series analysis capability")
print("10. Poor observability (no logging/monitoring)")
