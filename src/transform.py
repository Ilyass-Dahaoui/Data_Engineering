"""
Transform module: Clean and transform raw data.
Handles data quality issues, missing values, and standardization.
"""
from typing import List, Dict, Any
from datetime import datetime


def clean_apps_metadata(apps: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Clean and standardize apps metadata.
    
    Args:
        apps: Raw list of app metadata
        
    Returns:
        Cleaned list of app metadata
    """
    print("Cleaning apps metadata...")
    cleaned_apps = []
    
    for app in apps:
        # Skip if critical fields are missing
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
    """
    Clean and standardize apps reviews.
    
    Args:
        reviews: Raw list of reviews
        
    Returns:
        Cleaned list of reviews
    """
    print("Cleaning apps reviews...")
    cleaned_reviews = []
    
    for review in reviews:
        # Skip if critical fields are missing
        if not review.get('app_id') or review.get('content') is None:
            continue
        
        # Parse date if it's a string
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
    """
    Aggregate reviews and join with app metadata for analytics.
    
    Args:
        apps: Cleaned app metadata
        reviews: Cleaned reviews
        
    Returns:
        List of apps with aggregated review metrics
    """
    print("Transforming data for analytics...")
    
    # Create app lookup dictionary
    app_dict = {app['app_id']: app for app in apps}
    
    # Aggregate reviews by app
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
    
    # Calculate averages and combine with app data
    analytics_ready = []
    for app_id, agg in review_aggregates.items():
        if app_id in app_dict:
            app_data = app_dict[app_id].copy()
            
            # Calculate metrics
            agg['avg_score'] = agg['score_sum'] / agg['total_reviews'] if agg['total_reviews'] > 0 else 0
            agg['reply_rate'] = agg['reviews_with_reply'] / agg['total_reviews'] if agg['total_reviews'] > 0 else 0
            
            # Remove intermediate calculation fields
            del agg['score_sum']
            
            # Merge app data with aggregates
            app_data['review_metrics'] = agg
            analytics_ready.append(app_data)
    
    print(f"Created {len(analytics_ready)} analytics-ready records")
    return analytics_ready


if __name__ == "__main__":
    # Test transformation
    print("Testing data transformation...")
    from ingest import ingest_apps_metadata, ingest_apps_reviews
    
    try:
        # Ingest
        raw_apps = ingest_apps_metadata()
        raw_reviews = ingest_apps_reviews()
        
        # Clean
        clean_apps = clean_apps_metadata(raw_apps)
        clean_reviews = clean_apps_reviews(raw_reviews)
        
        # Transform
        analytics_data = transform_for_analytics(clean_apps, clean_reviews)
        
        print("\n✓ Transformation successful!")
        print(f"  Apps: {len(clean_apps)}")
        print(f"  Reviews: {len(clean_reviews)}")
        print(f"  Analytics records: {len(analytics_data)}")
        
        if analytics_data:
            print("\nSample analytics record:")
            sample = analytics_data[0]
            print(f"  App: {sample['title']}")
            print(f"  Total reviews: {sample['review_metrics']['total_reviews']}")
            print(f"  Avg score: {sample['review_metrics']['avg_score']:.2f}")
            
    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()
