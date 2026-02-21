import pytest

from src.quality import check_apps_metadata, check_reviews


def test_check_apps_metadata_empty():
    assert check_apps_metadata([]) == []


def test_check_apps_metadata_missing_fields():
    bad = [{'app_id': None, 'title': 'foo'}, {'app_id': '1'}]
    issues = check_apps_metadata(bad)
    assert len(issues) >= 1


def test_check_reviews_types():
    bad = [{'review_id': 'r1', 'app_id': 'a1', 'score': 'notint'}]
    issues = check_reviews(bad)
    assert len(issues) == 1
