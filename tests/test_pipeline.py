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
