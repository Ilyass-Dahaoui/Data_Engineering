import pytest
from datetime import datetime, timedelta

from src.utils import merge_reviews, scd2_update


def test_merge_reviews_basic():
    existing = [
        {'review_id': '1', 'score': 5},
        {'review_id': '2', 'score': 3}
    ]
    new = [
        {'review_id': '2', 'score': 4},
        {'review_id': '3', 'score': 1}
    ]
    merged = merge_reviews(existing, new)
    # review 2 should be updated, review 3 added
    ids = {r['review_id'] for r in merged}
    assert ids == {'1', '2', '3'}
    updated = next(r for r in merged if r['review_id'] == '2')
    assert updated['score'] == 4


def make_record(app_id, title):
    return {'app_id': app_id, 'title': title}


def test_scd2_update_insert_and_change():
    now = datetime.utcnow().isoformat()
    existing = [
        {'app_id': 'a1', 'title': 'Old', 'start_date': now, 'end_date': None, 'current_flag': True}
    ]
    incoming = [make_record('a1', 'New'), make_record('a2', 'Another')]

    updated = scd2_update(existing, incoming, timestamp=now)
    # Expect old record closed (end_date not None, current_flag False), plus two current rows
    assert len(updated) == 3
    # check state of a1
    hist_a1 = [r for r in updated if r['app_id'] == 'a1']
    assert len(hist_a1) == 2
    assert any(not r['current_flag'] for r in hist_a1)
    assert any(r['current_flag'] for r in hist_a1)
    # check new row attributes
    current = next(r for r in updated if r['app_id'] == 'a1' and r['current_flag'])
    assert current['title'] == 'New'


def test_scd2_update_deletion():
    now = datetime.utcnow().isoformat()
    existing = [
        {'app_id': 'x', 'title': 'Keep', 'start_date': now, 'end_date': None, 'current_flag': True},
        {'app_id': 'y', 'title': 'Gone', 'start_date': now, 'end_date': None, 'current_flag': True}
    ]
    incoming = [
        {'app_id': 'x', 'title': 'Keep'}
    ]
    updated = scd2_update(existing, incoming, timestamp=now)
    # y should be closed
    y_records = [r for r in updated if r['app_id'] == 'y']
    assert len(y_records) == 1
    assert y_records[0]['current_flag'] is False
    assert y_records[0]['end_date'] == now
