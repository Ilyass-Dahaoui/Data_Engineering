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


def transform_for_analytics(apps: List[Dict[str, Any]], 
                            reviews: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
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
