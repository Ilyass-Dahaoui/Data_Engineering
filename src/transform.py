from typing import List, Dict, Any
from datetime import datetime


def clean_apps_metadata(apps: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    print("Cleaning apps metadata...")
    cleaned_apps = []
    
    for app in apps:
        if not app.get('appId') or not app.get('title'):
            continue
            
        cleaned_app = {
            'app_id': app.get('appId'),
            'title': app.get('title'),
            'developer': app.get('developer', 'Unknown'),
            'developer_id': app.get('developerId'),
            'category': app.get('genre'),
            'rating': float(app.get('score', 0)) if app.get('score') else None,
            'ratings_count': int(app.get('ratings', 0)) if app.get('ratings') else 0,
            'installs': app.get('installs', '0'),
            'price': float(app.get('price', 0)) if app.get('price') else 0.0,
            'free': app.get('free', True),
            'content_rating': app.get('contentRating'),
            'released': app.get('released'),
            'updated': app.get('updated'),
            'version': app.get('version'),
            'description': app.get('description', ''),
            'summary': app.get('summary', '')
        }
        
        cleaned_apps.append(cleaned_app)
    
    print(f"Cleaned {len(cleaned_apps)} app records")
    return cleaned_apps


def clean_apps_reviews(reviews: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    print("Cleaning apps reviews...")
    cleaned_reviews = []
    
    for review in reviews:
        if not review.get('app_id') or review.get('content') is None:
            continue
        
        review_date = review.get('at')
        if isinstance(review_date, str):
            try:
                review_date = datetime.fromisoformat(review_date.replace('Z', '+00:00'))
            except:
                review_date = None
        
        cleaned_review = {
            'review_id': review.get('reviewId'),
            'app_id': review.get('app_id'),
            'app_name': review.get('app_name'),
            'user_name': review.get('userName', 'Anonymous'),
            'content': review.get('content', ''),
            'score': int(review.get('score', 0)) if review.get('score') else None,
            'thumbs_up_count': int(review.get('thumbsUpCount', 0)),
            'review_created_version': review.get('reviewCreatedVersion'),
            'at': review_date.isoformat() if review_date else None,
            'reply_content': review.get('replyContent'),
            'replied_at': review.get('repliedAt')
        }
        
        cleaned_reviews.append(cleaned_review)
    
    print(f"Cleaned {len(cleaned_reviews)} review records")
    return cleaned_reviews



def build_star_schema(apps: List[Dict[str, Any]],
                      reviews: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """Return star schema tables derived from clean app and review lists.

    The return value is a dict with keys ``dim_apps``, ``dim_categories``,
    ``dim_developers``, ``dim_date`` and ``fact_reviews`` matching the schema
    provided by the user.  Surrogate keys are generated as consecutive
    integers starting at 1 within each dimension.
    """
    # build category and developer dimensions
    categories = {}
    developers = {}
    dim_apps = []

    for app in apps:
        cat = app.get('category') or 'Unknown'
        if cat not in categories:
            categories[cat] = len(categories) + 1
        dev_name = app.get('developer') or 'Unknown'
        if dev_name not in developers:
            developers[dev_name] = len(developers) + 1

    # build dim_apps rows
    for app in apps:
        dim_apps.append({
            'app_key': len(dim_apps) + 1,
            'app_id': app['app_id'],
            'app_name': app.get('title'),
            'developer_key': developers.get(app.get('developer') or 'Unknown'),
            'category_key': categories.get(app.get('category') or 'Unknown'),
            'price': app.get('price'),
            'is_paid': not app.get('free', True),
            'installs': app.get('installs'),
            'catalog_rating': app.get('rating'),
            'ratings_count': app.get('ratings_count')
        })

    # prepare dim_categories and dim_developers lists
    dim_categories = [
        {'category_key': k, 'category_name': name}
        for name, k in categories.items()
    ]
    dim_developers = [
        {'developer_key': k, 'developer_name': name, 'developer_website': None, 'developer_email': None}
        for name, k in developers.items()
    ]

    # date dimension and fact_reviews
    dim_date = {}
    fact_reviews = []

    for rev in reviews:
        # convert review date to date_key
        dt_str = rev.get('at')
        if dt_str:
            try:
                dt = datetime.fromisoformat(dt_str)
            except ValueError:
                dt = None
        else:
            dt = None
        if dt:
            date_only = dt.date()
            if date_only not in dim_date:
                key = len(dim_date) + 1
                dim_date[date_only] = key
        else:
            key = None
        fact_reviews.append({
            'review_id': rev.get('review_id'),
            'app_key': next((r['app_key'] for r in dim_apps if r['app_id'] == rev.get('app_id')), None),
            'developer_key': next((r['developer_key'] for r in dim_apps if r['app_id'] == rev.get('app_id')), None),
            'date_key': dim_date.get(date_only) if dt else None,
            'rating': rev.get('score'),
            'thumbs_up_count': rev.get('thumbs_up_count'),
            'review_text': rev.get('content'),
            'review_version': rev.get('review_created_version')
        })

    dim_date_rows = []
    for date_val, key in dim_date.items():
        dim_date_rows.append({
            'date_key': key,
            'date': date_val.isoformat(),
            'year': date_val.year,
            'month': date_val.month,
            'quarter': (date_val.month - 1) // 3 + 1,
            'day_of_week': date_val.isoweekday(),
            'is_weekend': date_val.weekday() >= 5
        })

    return {
        'dim_apps': dim_apps,
        'dim_categories': dim_categories,
        'dim_developers': dim_developers,
        'dim_date': dim_date_rows,
        'fact_reviews': fact_reviews
    }


def transform_for_analytics(apps: List[Dict[str, Any]], 
                            reviews: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Keep existing metrics aggregation for backwards compatibility.

    This function continues to produce the simple analytics records used in Lab
   1; it is not aware of the full star schema.  The new :func:`build_star_schema`
    function should be used when schema generation is required.
    """
    print("Transforming data for analytics...")
    
    app_dict = {app['app_id']: app for app in apps}
    
    review_aggregates = {}
    for review in reviews:
        app_id = review['app_id']
        
        if app_id not in review_aggregates:
            review_aggregates[app_id] = {
                'total_reviews': 0,
                'avg_score': 0,
                'score_sum': 0,
                'total_thumbs_up': 0,
                '5_star': 0,
                '4_star': 0,
                '3_star': 0,
                '2_star': 0,
                '1_star': 0,
                'reviews_with_reply': 0
            }
        
        agg = review_aggregates[app_id]
        agg['total_reviews'] += 1
        
        if review['score']:
            agg['score_sum'] += review['score']
            agg[f"{review['score']}_star"] += 1
        
        agg['total_thumbs_up'] += review['thumbs_up_count']
        
        if review['reply_content']:
            agg['reviews_with_reply'] += 1
    
    analytics_ready = []
    for app_id, agg in review_aggregates.items():
        if app_id in app_dict:
            app_data = app_dict[app_id].copy()
            
            agg['avg_score'] = agg['score_sum'] / agg['total_reviews'] if agg['total_reviews'] > 0 else 0
            agg['reply_rate'] = agg['reviews_with_reply'] / agg['total_reviews'] if agg['total_reviews'] > 0 else 0
            
            del agg['score_sum']
            
            app_data['review_metrics'] = agg
            analytics_ready.append(app_data)
    
    print(f"Created {len(analytics_ready)} analytics-ready records")
    return analytics_ready
