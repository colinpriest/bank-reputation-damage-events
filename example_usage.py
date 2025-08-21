"""
Example usage of the Bank Reputation Damage Events Collection System.

This script demonstrates how to use the system for various common tasks.
"""

import asyncio
import json
from datetime import date, timedelta
from pathlib import Path

from storage.repository import EventRepository
from ingestion.orchestrators.scheduler import EventScheduler


async def example_daily_collection():
    """Example: Run daily collection for recent events."""
    print("=== Daily Collection Example ===")
    
    # Initialize repository and scheduler
    repository = EventRepository("example_events.db")
    scheduler = EventScheduler(repository)
    
    # Run daily collection for yesterday
    yesterday = date.today() - timedelta(days=1)
    results = await scheduler.run_daily_collection(yesterday)
    
    print(f"Collection Date: {results['date']}")
    print(f"Total Events Collected: {results['total_events']}")
    print(f"Errors: {len(results['errors'])}")
    
    for connector, result in results['connectors'].items():
        print(f"\n{connector.upper()}:")
        print(f"  Status: {result['status']}")
        print(f"  Events Fetched: {result['events_fetched']}")
        print(f"  Events Stored: {result['events_stored']}")
    
    return results


async def example_specific_connector():
    """Example: Run specific connectors for a date range."""
    print("\n=== Specific Connector Examples ===")
    
    repository = EventRepository("example_events.db")
    scheduler = EventScheduler(repository)
    
    # Run FDIC ED&O connector for last 30 days
    thirty_days_ago = date.today() - timedelta(days=30)
    fdic_results = await scheduler.run_connector('fdic_edo', thirty_days_ago)
    
    print(f"FDIC ED&O Connector: {fdic_results['connector']}")
    print(f"Since: {fdic_results['since']}")
    print(f"Events Fetched: {fdic_results['events_fetched']}")
    print(f"Events Stored: {fdic_results['events_stored']}")
    print(f"Status: {fdic_results['status']}")
    
    if fdic_results['errors']:
        print(f"Errors: {len(fdic_results['errors'])}")
        for error in fdic_results['errors'][:3]:  # Show first 3 errors
            print(f"  - {error['error']}")
    
    # Run OCC Enforcement connector for last 30 days
    occ_results = await scheduler.run_connector('occ_enforcement', thirty_days_ago)
    
    print(f"\nOCC Enforcement Connector: {occ_results['connector']}")
    print(f"Since: {occ_results['since']}")
    print(f"Events Fetched: {occ_results['events_fetched']}")
    print(f"Events Stored: {occ_results['events_stored']}")
    print(f"Status: {occ_results['status']}")
    
    if occ_results['errors']:
        print(f"Errors: {len(occ_results['errors'])}")
        for error in occ_results['errors'][:3]:  # Show first 3 errors
            print(f"  - {error['error']}")
    
    return {'fdic': fdic_results, 'occ': occ_results}


async def example_occ_connector():
    """Example: Run OCC Enforcement connector specifically."""
    print("\n=== OCC Enforcement Connector Example ===")
    
    repository = EventRepository("example_events.db")
    scheduler = EventScheduler(repository)
    
    # Run OCC connector for last 7 days
    last_week = date.today() - timedelta(days=7)
    results = await scheduler.run_connector('occ_enforcement', last_week)
    
    print(f"OCC Enforcement Connector Results:")
    print(f"  Connector: {results['connector']}")
    print(f"  Since: {results['since']}")
    print(f"  Events Fetched: {results['events_fetched']}")
    print(f"  Events Stored: {results['events_stored']}")
    print(f"  Status: {results['status']}")
    
    if results['errors']:
        print(f"  Errors: {len(results['errors'])}")
        for error in results['errors'][:3]:
            print(f"    - {error['error']}")
    
    # Show some details about collected events
    if results['events_stored'] > 0:
        print(f"\n  Sample Events Collected:")
        # Get the most recent events from this connector
        recent_events = repository.get_events(
            start_date=last_week,
            limit=3
        )
        
        for event in recent_events:
            print(f"    - {event.title}")
            print(f"      Date: {event.event_date}")
            print(f"      Institutions: {', '.join(event.institutions)}")
            print(f"      Categories: {', '.join(event.categories)}")
            if event.amounts.get('penalties_usd', 0) > 0:
                print(f"      Penalty: ${event.amounts['penalties_usd']:,}")
    
    return results


def example_query_events():
    """Example: Query events from the database."""
    print("\n=== Query Events Example ===")
    
    repository = EventRepository("example_events.db")
    
    # Get recent events
    recent_events = repository.get_events(
        start_date=date.today() - timedelta(days=7),
        limit=5
    )
    
    print(f"Recent Events (Last 7 days): {len(recent_events)}")
    
    for event in recent_events:
        print(f"\nEvent: {event.title}")
        print(f"  ID: {event.event_id}")
        print(f"  Date: {event.event_date}")
        print(f"  Institutions: {', '.join(event.institutions)}")
        print(f"  Categories: {', '.join(event.categories)}")
        print(f"  Materiality Score: {event.reputational_damage.materiality_score}")
        
        if event.amounts.get('penalties_usd', 0) > 0:
            print(f"  Penalty: ${event.amounts['penalties_usd']:,}")
    
    # Get events by category
    regulatory_events = repository.get_events(
        categories=['regulatory_action'],
        limit=3
    )
    
    print(f"\nRegulatory Events: {len(regulatory_events)}")
    for event in regulatory_events:
        print(f"  - {event.title} ({event.event_date})")
    
    return recent_events


def example_statistics():
    """Example: Get database statistics."""
    print("\n=== Statistics Example ===")
    
    repository = EventRepository("example_events.db")
    stats = repository.get_statistics()
    
    print(f"Database Statistics:")
    print(f"  Total Events: {stats['total_events']}")
    
    if 'date_range' in stats:
        print(f"  Date Range: {stats['date_range']['start']} to {stats['date_range']['end']}")
    
    print(f"  Total Penalties: ${stats['total_penalties_usd']:,}")
    
    if 'category_distribution' in stats:
        print(f"\nCategory Distribution:")
        for category, count in sorted(stats['category_distribution'].items(), 
                                    key=lambda x: x[1], reverse=True)[:5]:
            print(f"  {category}: {count}")
    
    if 'regulator_distribution' in stats:
        print(f"\nRegulator Distribution:")
        for regulator, count in sorted(stats['regulator_distribution'].items(), 
                                     key=lambda x: x[1], reverse=True)[:5]:
            print(f"  {regulator}: {count}")
    
    return stats


async def example_health_check():
    """Example: Run health check on connectors."""
    print("\n=== Health Check Example ===")
    
    repository = EventRepository("example_events.db")
    scheduler = EventScheduler(repository)
    
    health = await scheduler.health_check()
    
    print(f"Overall Status: {health['overall_status']}")
    print(f"Timestamp: {health['timestamp']}")
    
    for connector, status in health['connectors'].items():
        print(f"\n{connector.upper()}:")
        print(f"  Status: {status['status']}")
        print(f"  Message: {status['message']}")
        print(f"  Test Date: {status['test_date']}")
    
    return health


def example_institution_enrichment():
    """Example: Institution enrichment with BankFind."""
    print("\n=== Institution Enrichment Example ===")
    
    repository = EventRepository("example_events.db")
    
    # Example: Store institution information
    success = repository.upsert_institution(
        cert="12345",
        name="Example Bank, N.A.",
        rssd="1234567890",
        state="NY",
        primary_reg="OCC",
        aliases=["Example Bank", "ExampleBank"]
    )
    
    print(f"Institution stored: {success}")
    
    # Retrieve institution information
    institution = repository.get_institution("12345")
    if institution:
        print(f"Institution found:")
        print(f"  Name: {institution['name']}")
        print(f"  RSSD: {institution['rssd']}")
        print(f"  State: {institution['state']}")
        print(f"  Primary Regulator: {institution['primary_reg']}")
        print(f"  Aliases: {institution['aliases']}")
    
    return institution


async def example_monthly_backfill():
    """Example: Monthly backfill (commented out to avoid long execution)."""
    print("\n=== Monthly Backfill Example (Commented Out) ===")
    
    # This example is commented out to avoid long execution times
    # Uncomment to run monthly backfill
    
    """
    repository = EventRepository("example_events.db")
    scheduler = EventScheduler(repository)
    
    # Run backfill for current month
    current_month = date.today()
    results = await scheduler.run_monthly_backfill(
        current_month.year, 
        current_month.month
    )
    
    print(f"Backfill Period: {results['period']['start']} to {results['period']['end']}")
    print(f"Total Events: {results['total_events']}")
    
    for connector, result in results['connectors'].items():
        print(f"\n{connector}:")
        print(f"  Events Fetched: {result['events_fetched']}")
        print(f"  Events in Period: {result['events_in_period']}")
        print(f"  Events Stored: {result['events_stored']}")
    
    return results
    """
    
    print("Monthly backfill example is commented out to avoid long execution.")
    print("Uncomment the code in example_monthly_backfill() to run it.")
    return None


async def main():
    """Run all examples."""
    print("Bank Reputation Damage Events Collection System - Examples")
    print("=" * 60)
    
    try:
        # Run examples
        await example_daily_collection()
        await example_specific_connector()
        await example_occ_connector()
        example_query_events()
        example_statistics()
        await example_health_check()
        example_institution_enrichment()
        await example_monthly_backfill()
        
        print("\n" + "=" * 60)
        print("All examples completed successfully!")
        
    except Exception as e:
        print(f"\nError running examples: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    # Check if required environment variables are set
    import os
    if not os.getenv("FDIC_API_KEY"):
        print("Warning: FDIC_API_KEY not set. BankFind enrichment will be limited.")
        print("Set it with: export FDIC_API_KEY='your-api-key'")
    
    # Run examples
    asyncio.run(main())
