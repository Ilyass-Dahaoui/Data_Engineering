import json
import time
import os
from datetime import datetime
from google_play_scraper import search, app, reviews, Sort

RAW_DATA_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "DATA", "raw")
LANG = "en"
COUNTRY = "us"

SEARCH_QUERY = "note taking"
MAX_APPS = 50
REVIEWS_PER_APP = 200
MAX_REVIEWS_PER_APP = 5000
SLEEP_SECONDS = 1

def json_serializer(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")

def extract_apps_metadata():
    print("Searching for apps...")
    search_results = search(
        SEARCH_QUERY,
        lang=LANG,
        country=COUNTRY,
        n_hits=MAX_APPS
    )

    apps_metadata = []

    for result in search_results:
        app_id = result["appId"]
        print(f"Fetching metadata for {app_id}")

        try:
            metadata = app(
                app_id,
                lang=LANG,
                country=COUNTRY
            )
            apps_metadata.append(metadata)
            time.sleep(SLEEP_SECONDS)
        except Exception as e:
            print(f"Failed to fetch app {app_id}: {e}")

    return apps_metadata

def extract_apps_reviews(apps_metadata):
    reviews_file = f"{RAW_DATA_PATH}/apps_reviews.json"
    
    if os.path.exists(reviews_file):
        os.remove(reviews_file)
    
    total_reviews = 0

    for app_meta in apps_metadata:
        app_id = app_meta.get("appId")
        app_name = app_meta.get("title")
        print(f"Fetching reviews for {app_name} ({app_id})")

        try:
            continuation_token = None
            app_review_count = 0
            
            while True:
                result, continuation_token = reviews(
                    app_id,
                    lang=LANG,
                    country=COUNTRY,
                    sort=Sort.NEWEST,
                    count=REVIEWS_PER_APP,
                    continuation_token=continuation_token
                )

                for r in result:
                    r["app_id"] = app_id
                    r["app_name"] = app_name
                
                with open(reviews_file, "a", encoding="utf-8") as f:
                    for r in result:
                        json.dump(r, f, ensure_ascii=False, default=json_serializer)
                        f.write("\n")
                
                app_review_count += len(result)
                total_reviews += len(result)
                print(f"  Fetched {len(result)} reviews (total for this app: {app_review_count})")
                
                if not continuation_token or len(result) == 0 or app_review_count >= MAX_REVIEWS_PER_APP:
                    if app_review_count >= MAX_REVIEWS_PER_APP:
                        print(f"  Reached maximum limit of {MAX_REVIEWS_PER_APP} reviews for this app")
                    break
                
                time.sleep(SLEEP_SECONDS)

            print(f"Completed {app_name}: {app_review_count} reviews")
            
        except Exception as e:
            print(f"Failed to fetch reviews for {app_id}: {e}")

    return total_reviews

def main():
    print("Starting Google Play extraction pipeline...")
    print(f"Raw data will be saved to: {RAW_DATA_PATH}\n")

    apps_metadata = extract_apps_metadata()

    with open(f"{RAW_DATA_PATH}/apps_metadata.json", "w", encoding="utf-8") as f:
        json.dump(apps_metadata, f, ensure_ascii=False, indent=2, default=json_serializer)

    print(f"\nSaved {len(apps_metadata)} apps metadata records")

    total_reviews = extract_apps_reviews(apps_metadata)

    print(f"\nTotal reviews saved: {total_reviews}")
    print("Extraction completed successfully")

if __name__ == "__main__":
    main()
