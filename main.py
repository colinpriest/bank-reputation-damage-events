"""
Main application for bank reputation damage event collection.
Demonstrates usage of the comprehensive data source integration system.

This script now includes OCC Enforcement data source extraction alongside FDIC ED&O.
Available functions:
- main(): Run comprehensive demonstration with both FDIC and OCC connectors
- run_single_connector_example(): Run both FDIC and OCC connectors for last week
- run_occ_connector_only(): Run only OCC connector for quick testing
- run_query_example(): Query events from database
- run_statistics_example(): Get database statistics
"""

import asyncio
import json
import os
from datetime import date, datetime, timedelta
from typing import Dict, Any
from dotenv import load_dotenv

from storage.repository import EventRepository
from ingestion.orchestrators.scheduler import EventScheduler, run_daily_collection, run_monthly_backfill
import structlog

# Load environment variables from .env file
load_dotenv()

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()


async def main():
    """Main application entry point."""
    logger.info("Starting bank reputation damage event collection system")
    
    # Initialize repository
    repository = EventRepository("bank_events.db")
    
    # Initialize scheduler
    scheduler = EventScheduler(repository)
    
    try:
        # Example 1: Run daily collection for yesterday
        logger.info("Running daily collection for yesterday")
        yesterday = date.today() - timedelta(days=1)
        daily_results = await scheduler.run_daily_collection(yesterday)
        
        print("\n=== DAILY COLLECTION RESULTS ===")
        print(f"Date: {daily_results['date']}")
        print(f"Total Events: {daily_results['total_events']}")
        print(f"Errors: {len(daily_results['errors'])}")
        
        for connector, result in daily_results['connectors'].items():
            print(f"\n{connector}:")
            print(f"  Status: {result['status']}")
            print(f"  Events Fetched: {result['events_fetched']}")
            print(f"  Events Stored: {result['events_stored']}")
            if 'error' in result:
                print(f"  Error: {result['error']}")
        
        # Example 2: Run specific connectors
        logger.info("Running FDIC ED&O connector for last 30 days")
        thirty_days_ago = date.today() - timedelta(days=30)
        fdic_results = await scheduler.run_connector('fdic_edo', thirty_days_ago)
        
        print(f"\n=== FDIC ED&O RESULTS ===")
        print(f"Connector: {fdic_results['connector']}")
        print(f"Since: {fdic_results['since']}")
        print(f"Events Fetched: {fdic_results['events_fetched']}")
        print(f"Events Stored: {fdic_results['events_stored']}")
        print(f"Status: {fdic_results['status']}")
        
        # Example 2b: Run OCC Enforcement connector
        logger.info("Running OCC Enforcement connector for last 30 days")
        occ_results = await scheduler.run_connector('occ_enforcement', thirty_days_ago)
        
        print(f"\n=== OCC ENFORCEMENT RESULTS ===")
        print(f"Connector: {occ_results['connector']}")
        print(f"Since: {occ_results['since']}")
        print(f"Events Fetched: {occ_results['events_fetched']}")
        print(f"Events Stored: {occ_results['events_stored']}")
        print(f"Status: {occ_results['status']}")
        
        # Example 3: Get system statistics
        logger.info("Getting system statistics")
        stats = scheduler.get_statistics()
        
        print(f"\n=== SYSTEM STATISTICS ===")
        if 'database' in stats:
            db_stats = stats['database']
            print(f"Total Events in Database: {db_stats.get('total_events', 0)}")
            if 'date_range' in db_stats:
                print(f"Date Range: {db_stats['date_range']['start']} to {db_stats['date_range']['end']}")
            print(f"Total Penalties: ${db_stats.get('total_penalties_usd', 0):,}")
            
            if 'category_distribution' in db_stats:
                print("\nCategory Distribution:")
                for category, count in db_stats['category_distribution'].items():
                    print(f"  {category}: {count}")
            
            if 'regulator_distribution' in db_stats:
                print("\nRegulator Distribution:")
                for regulator, count in db_stats['regulator_distribution'].items():
                    print(f"  {regulator}: {count}")
        
        # Example 4: Health check
        logger.info("Running health check")
        health = await scheduler.health_check()
        
        print(f"\n=== HEALTH CHECK ===")
        print(f"Overall Status: {health['overall_status']}")
        print(f"Timestamp: {health['timestamp']}")
        
        for connector, status in health['connectors'].items():
            print(f"\n{connector}:")
            print(f"  Status: {status['status']}")
            print(f"  Message: {status['message']}")
        
        # Example 5: Query events from database
        logger.info("Querying recent events from database")
        recent_events = repository.get_events(
            start_date=date.today() - timedelta(days=7),
            limit=10
        )
        
        print(f"\n=== RECENT EVENTS (Last 7 days) ===")
        print(f"Found {len(recent_events)} events")
        
        for event in recent_events[:5]:  # Show first 5
            print(f"\nEvent ID: {event.event_id}")
            print(f"Title: {event.title}")
            print(f"Institutions: {', '.join(event.institutions)}")
            print(f"Date: {event.event_date}")
            print(f"Categories: {', '.join(event.categories)}")
            print(f"Materiality Score: {event.reputational_damage.materiality_score}")
            if event.amounts.get('penalties_usd', 0) > 0:
                print(f"Penalty: ${event.amounts['penalties_usd']:,}")
        
        # Example 6: Monthly backfill (commented out to avoid long execution)
        logger.info("Running monthly backfill for current month")
        current_month = datetime.now()
        while current_month.year >= 2020:
            monthly_results = await scheduler.run_monthly_backfill(
                current_month.year, current_month.month
            )
            print(f"\n=== MONTHLY BACKFILL RESULTS ===")
            print(f"Period: {monthly_results['period']['start']} to {monthly_results['period']['end']}")
            print(f"Total Events: {monthly_results['total_events']}")
            
            # Move to previous month
            if current_month.month == 1:
                current_month = current_month.replace(year=current_month.year - 1, month=12)
            else:
                current_month = current_month.replace(month=current_month.month - 1)
        
        logger.info("Application completed successfully")
        
    except Exception as e:
        logger.error("Application failed", error=str(e))
        raise


def run_single_connector_example():
    """Example of running a single connector synchronously."""
    import asyncio
    
    async def run_connector_examples():
        repository = EventRepository("bank_events.db")
        scheduler = EventScheduler(repository)
        
        # Run FDIC connector for last week
        last_week = date.today() - timedelta(days=7)
        fdic_results = await scheduler.run_connector('fdic_edo', last_week)
        
        print("FDIC ED&O Connector Results:")
        print(json.dumps(fdic_results, indent=2, default=str))
        
        # Run OCC connector for last week
        occ_results = await scheduler.run_connector('occ_enforcement', last_week)
        
        print("\nOCC Enforcement Connector Results:")
        print(json.dumps(occ_results, indent=2, default=str))
    
    asyncio.run(run_connector_examples())


def run_statistics_example():
    """Example of getting database statistics."""
    repository = EventRepository("bank_events.db")
    stats = repository.get_statistics()
    
    print("Database Statistics:")
    print(json.dumps(stats, indent=2, default=str))


def run_query_example():
    """Example of querying events from the database."""
    repository = EventRepository("bank_events.db")
    
    # Get events from last month
    last_month = date.today() - timedelta(days=30)
    
    events = repository.get_events(
        start_date=last_month,
        categories=['regulatory_action', 'fine'],
        limit=20
    )
    
    print(f"Found {len(events)} regulatory events in the last 30 days:")
    for event in events:
        print(f"- {event.title} ({event.event_date})")


def run_occ_connector_only():
    """Run only the OCC Enforcement connector for quick testing."""
    import asyncio
    
    async def run_occ():
        repository = EventRepository("bank_events.db")
        scheduler = EventScheduler(repository)
        
        # Run OCC connector for last 7 days
        last_week = date.today() - timedelta(days=7)
        results = await scheduler.run_connector('occ_enforcement', last_week)
        
        print("OCC Enforcement Connector Results:")
        print(json.dumps(results, indent=2, default=str))
        
        if results['events_stored'] > 0:
            print(f"\nSuccessfully stored {results['events_stored']} events from OCC")
        else:
            print("\nNo new events found from OCC")
    
    asyncio.run(run_occ())


if __name__ == "__main__":
    # Check if required environment variables are set
    if not os.getenv("FDIC_API_KEY"):
        print("Warning: FDIC_API_KEY not set. BankFind enrichment will be limited.")
        print("Please create a .env file in the project root with:")
        print("FDIC_API_KEY=your-fdic-api-key-here")
        print("You can get an API key from: https://banks.data.fdic.gov/developers/")
    
    if not os.getenv("NEWS_API_KEY"):
        print("Info: NEWS_API_KEY not set. Financial news monitoring will be disabled.")
        print("To enable news monitoring, add NEWS_API_KEY to your .env file")
        print("Get a free API key from: https://newsapi.org/register")
    
    if not os.getenv("MEDIASTACK_API_KEY"):
        print("Info: MEDIASTACK_API_KEY not set. Additional news monitoring will be disabled.")
        print("To enable MediaStack news monitoring, add MEDIASTACK_API_KEY to your .env file")
        print("Get a free API key from: https://mediastack.com/")
    
    # Run the main application
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nApplication interrupted by user")
    except Exception as e:
        print(f"Application failed: {e}")
        import traceback
        traceback.print_exc()
