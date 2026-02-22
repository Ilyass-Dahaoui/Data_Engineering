import json
import os
import sys
from pathlib import Path

import pytest

# make sure the src directory is on path so imports inside code resolve correctly
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import pipeline
import config


def test_pipeline_incremental(tmp_path):
    # set up temporary raw and processed directories
    raw_dir = tmp_path / "DATA" / "raw"
    proc_dir = tmp_path / "DATA" / "processed"
    raw_dir.mkdir(parents=True)
    proc_dir.mkdir(parents=True)

    apps = [{'appId': 'a1', 'title': 'App1', 'score': 4}]
    reviews = [{'reviewId': 'r1', 'app_id': 'a1', 'content': 'ok', 'score': 5}]

    with open(raw_dir / "apps_metadata.json", 'w', encoding='utf-8') as f:
        json.dump(apps, f)
    with open(raw_dir / "apps_reviews.json", 'w', encoding='utf-8') as f:
        # write as JSON array for simplicity
        json.dump(reviews, f)

    # override config paths to point at tmp files (assign directly)
    config.RAW_DATA_DIR = str(raw_dir)
    config.PROCESSED_DATA_DIR = str(proc_dir)
    config.APPS_METADATA_RAW = str(raw_dir / "apps_metadata.json")
    config.APPS_REVIEWS_RAW = str(raw_dir / "apps_reviews.json")
    config.APPS_METADATA_PROCESSED = str(proc_dir / "apps_metadata_clean.json")
    config.APPS_METADATA_SCD2 = str(proc_dir / "apps_metadata_scd2.json")
    config.APPS_REVIEWS_PROCESSED = str(proc_dir / "apps_reviews_clean.json")
    config.APPS_WITH_METRICS = str(proc_dir / "apps_with_metrics.json")
    config.DIM_APPS = str(proc_dir / "dim_apps.json")
    config.DIM_CATEGORIES = str(proc_dir / "dim_categories.json")
    config.DIM_DEVELOPERS = str(proc_dir / "dim_developers.json")
    config.DIM_DATE = str(proc_dir / "dim_date.json")
    config.FACT_REVIEWS = str(proc_dir / "fact_reviews.json")

    # first run should succeed and create output
    assert pipeline.run_pipeline()
    rev_file = proc_dir / "apps_reviews_clean.json"
    assert rev_file.exists()
    data = json.loads(rev_file.read_text(encoding='utf-8'))
    assert len(data) == 1

    # second run should not duplicate the review
    assert pipeline.run_pipeline()
    data2 = json.loads(rev_file.read_text(encoding='utf-8'))
    assert len(data2) == 1
    # also ensure history file created
    hist = proc_dir / "apps_metadata_scd2.json"
    assert hist.exists()
    hist_data = json.loads(hist.read_text(encoding='utf-8'))
    assert len(hist_data) >= 1

    # verify star schema outputs exist and have minimal structure
    for fname in ["dim_apps.json", "dim_categories.json", "dim_developers.json", "dim_date.json", "fact_reviews.json"]:
        fpath = proc_dir / fname
        assert fpath.exists(), f"{fname} missing"
        contents = json.loads(fpath.read_text(encoding='utf-8'))
        assert isinstance(contents, list)
        # only one app so expect 1 or more rows depending on dimension
    # check dim_apps row has expected keys
    apps_dim = json.loads((proc_dir / "dim_apps.json").read_text(encoding='utf-8'))
    assert apps_dim[0]['app_id'] == 'a1'


def test_ingest_csv_and_clean_transform(tmp_path):
    # create small CSV files that mimic schema drift scenarios
    raw_dir = tmp_path / "DATA" / "raw"
    raw_dir.mkdir(parents=True)
    # apps_metadata with year and rating column drift
    csv_path = raw_dir / "apps_updated.csv"
    with open(csv_path, 'w', encoding='utf-8') as f:
        f.write("appId,title,year,rating\n")
        f.write("a2,AppTwo,2021,4.5\n")
    # reviews batch file with score->rating and comments->content
    review_csv = raw_dir / "note_taking_batch.csv"
    with open(review_csv, 'w', encoding='utf-8') as f:
        f.write("reviewId,app_id,rating,comments,at\n")
        f.write("r2,a2,3,Great!,2022-01-01T00:00:00Z\n")

    # adjust config paths
    config.RAW_DATA_DIR = str(raw_dir)
    config.APPS_METADATA_RAW = str(raw_dir / "nonexistent.json")
    config.APPS_REVIEWS_RAW = str(raw_dir / "missing.json")

    # call ingest functions
    from src import ingest, transform
    apps = ingest.ingest_apps_metadata()
    reviews = ingest.ingest_apps_reviews()
    assert len(apps) == 1
    assert apps[0]['appId'] == 'a2'
    assert len(reviews) == 1

    # clean
    cleaned_apps = transform.clean_apps_metadata(apps)
    cleaned_reviews = transform.clean_apps_reviews(reviews)
    assert cleaned_apps[0]['released'] == '2021'
    assert cleaned_apps[0]['rating'] == 4.5
    assert cleaned_reviews[0]['score'] == 3
    assert cleaned_reviews[0]['content'] == 'Great!'
