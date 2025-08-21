import asyncio
from mediastack_search import MediaStackSearch
from datetime import date, timedelta
import json

async def collect_news_articles_monthly():
    """Collect and analyze news articles for Bank of America using monthly searches"""
    
    # Initialize searcher
    searcher = MediaStackSearch()
    all_articles = []
    
    # Search month by month for 2023
    for month in range(1, 13):
        start_date = date(2023, month, 1)
        if month == 12:
            end_date = date(2023, 12, 31)
        else:
            end_date = date(2023, month + 1, 1) - timedelta(days=1)
        
        print(f"\n🔍 Searching {start_date.strftime('%B %Y')}: {start_date} to {end_date}")
        
        # Create a new searcher instance for each month
        monthly_searcher = MediaStackSearch()
        success = await monthly_searcher.search(
            search_phrase="Bank of America",
            start_date=start_date,
            end_date=end_date,
            exclude_keywords=["earnings", "quarterly", "profit"]
        )
        
        if success:
            article_count = monthly_searcher.article_count()
            print(f"✅ Found {article_count} articles for {start_date.strftime('%B %Y')}")
            
            if article_count > 0:
                # Process results for this month
                await monthly_searcher.scrape_all_articles()
                await monthly_searcher.analyse_all_articles("Bank of America", "junk fees and fake accounts")
                
                # Add articles to the main collection
                all_articles.extend(monthly_searcher.articles)
        else:
            print(f"❌ Search failed for {start_date.strftime('%B %Y')}")
    
    # Export combined results
    if all_articles:
        print(f"\n📊 Total articles collected: {len(all_articles)}")
        
        # Export to JSON
        export_data = []
        for article in all_articles:
            article_data = {
                "title": article.title,
                "description": article.description,
                "url": article.url,
                "source": article.source,
                "published_at": article.published_at,
                "country": article.country,
                "language": article.language,
                "category": article.category,
                "scraped_text": article.scraped_text,
                "analysis": article.analysis
            }
            export_data.append(article_data)
        
        with open("analysis_results_monthly.json", 'w', encoding='utf-8') as f:
            json.dump(export_data, f, indent=2, ensure_ascii=False)
        
        print("✅ Results exported to analysis_results_monthly.json")
        
        # Show date distribution
        dates = [article.published_at for article in all_articles if article.published_at]
        if dates:
            dates.sort()
            print(f"📅 Date range: {dates[0]} to {dates[-1]}")
            
            # Count by month
            month_counts = {}
            for article in all_articles:
                if article.published_at:
                    month = article.published_at[:7]  # YYYY-MM
                    month_counts[month] = month_counts.get(month, 0) + 1
            
            print("📈 Articles by month:")
            for month in sorted(month_counts.keys()):
                print(f"   {month}: {month_counts[month]} articles")
    else:
        print("❌ No articles found across all months")

if __name__ == "__main__":
    asyncio.run(collect_news_articles_monthly())

