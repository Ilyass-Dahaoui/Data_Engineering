import json
import time
from datetime import datetime
from google_play_scraper import search, app, reviews, Sort

RAW_DATA_PATH = r"C:\Users\asus\Downloads\App Market Reseaarch\DATA\raw"
LANG = "en"
COUNTRY = "us"

SEARCH_QUERY = "AI note taking"
MAX_APPS = 20
REVIEWS_PER_APP = 300
SLEEP_SECONDS = 2

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
    all_reviews = []

    for app_meta in apps_metadata:
        app_id = app_meta.get("appId")
        app_name = app_meta.get("title")
        print(f"Fetching reviews for {app_name}")

        try:
            result, _ = reviews(
                app_id,
                lang=LANG,
                country=COUNTRY,
                sort=Sort.NEWEST,
                count=REVIEWS_PER_APP
            )

            for r in result:
                r["app_id"] = app_id
                r["app_name"] = app_name
                all_reviews.append(r)

            time.sleep(SLEEP_SECONDS)
        except Exception as e:
            print(f"Failed to fetch reviews for {app_id}: {e}")

    return all_reviews

def main():
    print("Starting Google Play extraction pipeline...")

    apps_metadata = extract_apps_metadata()

    with open(f"{RAW_DATA_PATH}/apps_metadata.json", "w", encoding="utf-8") as f:
        json.dump(apps_metadata, f, ensure_ascii=False, indent=2, default=json_serializer)

    print(f"Saved {len(apps_metadata)} apps metadata records")

    apps_reviews = extract_apps_reviews(apps_metadata)

    with open(f"{RAW_DATA_PATH}/apps_reviews.json", "w", encoding="utf-8") as f:
        json.dump(apps_reviews, f, ensure_ascii=False, indent=2, default=json_serializer)

    print(f"Saved {len(apps_reviews)} reviews records")
    print("Extraction completed successfully")

if __name__ == "__main__":
    main()
