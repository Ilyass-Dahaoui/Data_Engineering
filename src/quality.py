from typing import List, Dict, Any


# simple data quality checks that return a list of issues or a metrics dict


def check_apps_metadata(apps: List[Dict[str, Any]]) -> List[str]:
    issues = []
    for i, app in enumerate(apps):
        if not app.get('app_id') or not app.get('title'):
            issues.append(f"Row {i}: missing primary fields")
        # check types
        if 'rating' in app and app['rating'] is not None:
            if not isinstance(app['rating'], (int, float)):
                issues.append(f"Row {i}: rating not numeric")
    return issues


def check_reviews(reviews: List[Dict[str, Any]]) -> List[str]:
    issues = []
    for i, r in enumerate(reviews):
        if not r.get('review_id') or not r.get('app_id'):
            issues.append(f"Row {i}: missing ids")
        if 'score' in r and r['score'] is not None:
            if not isinstance(r['score'], int):
                issues.append(f"Row {i}: score not integer")
    return issues
