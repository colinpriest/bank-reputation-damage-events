import asyncio
from mediastack_search import MediaStackSearch
from datetime import date, timedelta

async def collect_news_articles():
    """Collect and analyze news articles for Bank of America"""
    
    # Initialize and search
    searcher = MediaStackSearch()
    success = await searcher.search(
        search_phrase="Bank of America",
        start_date=date(2023, 7, 1),
        end_date=date(2023, 7, 31),
        exclude_keywords=["earnings", "quarterly", "profit"]
    )

    # Process results
    if success:
        print(f"Found {searcher.article_count()} articles")
        await searcher.scrape_all_articles()
        await searcher.analyse_all_articles("Bank of America", "junk fees and fake accounts")
        # After running analysis
        if searcher.entity_match():
            print("Found articles about the banking entity!")
        if searcher.event_match():
            print("Found articles about the specific event!")
        searcher.export_results("analysis_results.json")
    else:
        print("Search failed")

if __name__ == "__main__":
    asyncio.run(collect_news_articles())