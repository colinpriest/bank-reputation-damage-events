#!/usr/bin/env python3
"""
Debug script to test MediaStack API date range functionality
"""

import asyncio
import httpx
import os
from datetime import date
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

async def test_mediastack_date_ranges():
    """Test different date ranges to see what MediaStack returns"""
    
    api_key = os.getenv('MEDIASTACK_API_KEY')
    if not api_key:
        print("❌ MEDIASTACK_API_KEY not found in environment")
        return
    
    base_url = "http://api.mediastack.com/v1/news"
    
    # Test different date ranges
    test_ranges = [
        ("Full year 2023", date(2023, 1, 1), date(2023, 12, 31)),
        ("First half 2023", date(2023, 1, 1), date(2023, 6, 30)),
        ("Second half 2023", date(2023, 7, 1), date(2023, 12, 31)),
        ("Q1 2023", date(2023, 1, 1), date(2023, 3, 31)),
        ("Q2 2023", date(2023, 4, 1), date(2023, 6, 30)),
        ("Q3 2023", date(2023, 7, 1), date(2023, 9, 30)),
        ("Q4 2023", date(2023, 10, 1), date(2023, 12, 31)),
    ]
    
    async with httpx.AsyncClient() as client:
        for range_name, start_date, end_date in test_ranges:
            print(f"\n🔍 Testing {range_name}: {start_date} to {end_date}")
            
            params = {
                'access_key': api_key,
                'keywords': 'Bank of America',
                'date': f"{start_date.strftime('%Y-%m-%d')},{end_date.strftime('%Y-%m-%d')}",
                'languages': 'en',
                'countries': 'us',
                'categories': 'business',
                'limit': 10,  # Just get first 10 for testing
                'offset': 0
            }
            
            try:
                response = await client.get(base_url, params=params, timeout=30.0)
                
                if response.status_code == 200:
                    data = response.json()
                    articles = data.get('data', [])
                    
                    print(f"   ✅ Status: {response.status_code}")
                    print(f"   📊 Articles found: {len(articles)}")
                    
                    if articles:
                        # Show date range of returned articles
                        dates = [article.get('published_at', '') for article in articles]
                        dates = [d for d in dates if d]  # Filter out empty dates
                        
                        if dates:
                            dates.sort()
                            print(f"   📅 Date range: {dates[0]} to {dates[-1]}")
                            
                            # Count articles by month
                            month_counts = {}
                            for article in articles:
                                pub_date = article.get('published_at', '')
                                if pub_date:
                                    month = pub_date[:7]  # YYYY-MM
                                    month_counts[month] = month_counts.get(month, 0) + 1
                            
                            print(f"   📈 Articles by month: {month_counts}")
                    else:
                        print("   ❌ No articles found")
                        
                else:
                    print(f"   ❌ Error: {response.status_code}")
                    print(f"   📄 Response: {response.text}")
                    
            except Exception as e:
                print(f"   ❌ Exception: {e}")
            
            # Small delay between requests
            await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(test_mediastack_date_ranges()) 