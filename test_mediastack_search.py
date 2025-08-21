#!/usr/bin/env python3
"""
Test script for MediaStackSearch functionality
"""

import asyncio
from datetime import date, timedelta
from mediastack_search import MediaStackSearch

async def test_mediastack_search():
    """Test the MediaStackSearch functionality"""
    print("Testing MediaStackSearch Class")
    print("=" * 50)
    
    try:
        # Initialize search
        searcher = MediaStackSearch()
        
        # Search for articles about a specific bank in the last 7 days
        end_date = date.today()
        start_date = end_date - timedelta(days=7)
        
        print(f"🔍 Searching for 'Wells Fargo' from {start_date} to {end_date}")
        
        # Search for articles
        success = await searcher.search(
            search_phrase="Wells Fargo",
            start_date=start_date,
            end_date=end_date,
            exclude_keywords=["earnings", "quarterly", "profit", "revenue"]  # Exclude routine earnings news
        )
        
        if success:
            print(f"✅ Search completed successfully")
            print(f"📊 Found {searcher.article_count()} articles")
            
            # Display first few articles
            for i in range(min(3, searcher.article_count())):
                article = searcher.get_article(i)
                if article:
                    print(f"\n📰 Article {i+1}:")
                    print(f"   Title: {article.title}")
                    print(f"   Source: {article.source}")
                    print(f"   Published: {article.published_at}")
                    print(f"   URL: {article.url}")
            
            # Test scraping (just first article for demo)
            if searcher.article_count() > 0:
                print(f"\n🔄 Testing scraping for first article...")
                first_article = searcher.get_article(0)
                if first_article:
                    scrape_success = await first_article.scrape()
                    if scrape_success:
                        print(f"✅ Scraping successful")
                        print(f"   Text length: {len(first_article.scraped_text or '')} characters")
                        print(f"   PDF created: {first_article.pdf_path}")
                    else:
                        print(f"❌ Scraping failed")
            
            # Test analysis (just first article for demo)
            if searcher.article_count() > 0:
                print(f"\n🧠 Testing analysis for first article...")
                first_article = searcher.get_article(0)
                if first_article:
                    # Test with specific event
                    analysis = await first_article.analyse("Wells Fargo", "regulatory fine")
                    if "error" not in analysis:
                        print(f"✅ Analysis successful")
                        print(f"   About banking entity: {analysis.get('about_banking_entity', 'Unknown')}")
                        print(f"   About specific event: {analysis.get('about_specific_event', 'N/A')}")
                        print(f"   Sentiment (wording): {analysis.get('sentiment_wording', 'Unknown')}")
                        print(f"   Sentiment (facts): {analysis.get('sentiment_facts', 'Unknown')}")
                        print(f"   Category: {analysis.get('category', 'Unknown')}")
                    else:
                        print(f"❌ Analysis failed: {analysis.get('error', 'Unknown error')}")
            
            # Export results
            print(f"\n💾 Exporting results...")
            export_success = searcher.export_results("wells_fargo_test_results.json")
            if export_success:
                print(f"✅ Results exported successfully")
            else:
                print(f"❌ Export failed")
                
        else:
            print(f"❌ Search failed")
            
    except Exception as e:
        print(f"❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_mediastack_search())
