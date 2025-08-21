"""
Scheduler and orchestrator for bank reputation damage event collection.
Manages execution of all connectors according to specified schedules and SLOs.
"""

import asyncio
import os
from datetime import date, datetime, timedelta
from typing import List, Dict, Any, Optional
import structlog
from pathlib import Path
from dotenv import load_dotenv

from ingestion.connectors.base import Connector
from ingestion.connectors.fdic_edo import FdicEdoConnector
from ingestion.connectors.occ_enforcement import OccEnforcementConnector
from ingestion.connectors.ffiec_bankfind import BankFindConnector
from ingestion.connectors.newsapi import NewsApiConnector
from ingestion.connectors.mediastack import MediaStackConnector
from storage.repository import EventRepository

# Load environment variables from .env file
load_dotenv()


class EventScheduler:
    """Scheduler for bank reputation damage event collection."""
    
    def __init__(self, repository: EventRepository):
        self.repository = repository
        self.logger = structlog.get_logger("scheduler")
        
        # Initialize connectors
        self.connectors: Dict[str, Connector] = {
            'fdic_edo': FdicEdoConnector(),
            'occ_enforcement': OccEnforcementConnector(),
            'ffiec_bankfind': BankFindConnector(),
            'newsapi': NewsApiConnector(),
            'mediastack': MediaStackConnector(),
        }
        
        # Schedule configuration
        self.schedules = {
            # Regulators: daily at 06:00 ET
            'fdic_edo': {'frequency': 'daily', 'time': '06:00', 'timezone': 'America/New_York'},
            'occ_enforcement': {'frequency': 'daily', 'time': '06:00', 'timezone': 'America/New_York'},
            'frb_enforcement': {'frequency': 'daily', 'time': '06:00', 'timezone': 'America/New_York'},
            'nydfs_enforcement': {'frequency': 'daily', 'time': '06:00', 'timezone': 'America/New_York'},
            'cfpb_enforcement': {'frequency': 'daily', 'time': '06:00', 'timezone': 'America/New_York'},
            
            # State breaches: daily at 07:00 PT/ET
            'state_breaches_ca': {'frequency': 'daily', 'time': '07:00', 'timezone': 'America/Los_Angeles'},
            'state_breaches_me': {'frequency': 'daily', 'time': '07:00', 'timezone': 'America/New_York'},
            
            # CourtListener: daily search
            'courtlistener': {'frequency': 'daily', 'time': '08:00', 'timezone': 'America/New_York'},
            
            # Bank failures: hourly check on Fridays, otherwise daily
            'fdic_failed_banks': {'frequency': 'friday_hourly', 'time': '09:00', 'timezone': 'America/New_York'},
        }
    
    async def run_daily_collection(self, target_date: Optional[date] = None) -> Dict[str, Any]:
        """
        Run daily collection for all active connectors.
        Returns summary of collection results.
        """
        if target_date is None:
            target_date = date.today() - timedelta(days=1)  # Yesterday by default
        
        self.logger.info("Starting daily collection", target_date=target_date.isoformat())
        
        results = {
            'date': target_date.isoformat(),
            'connectors': {},
            'total_events': 0,
            'errors': []
        }
        
        # Run each connector
        for connector_name, connector in self.connectors.items():
            if connector_name in ['ffiec_bankfind']:
                continue  # Skip BankFind as it's used for enrichment
            
            try:
                self.logger.info(f"Running connector: {connector_name}")
                
                # Fetch updates
                events = await connector.fetch_updates(target_date)
                
                # Store events
                stored_count = 0
                for event in events:
                    try:
                        # Enrich with BankFind data if available
                        if 'ffiec_bankfind' in self.connectors:
                            bankfind = self.connectors['ffiec_bankfind']
                            event = await bankfind._enrich_event_institutions_async(event)
                        
                        # Store in repository
                        if self.repository.upsert_event(event):
                            stored_count += 1
                    
                    except Exception as e:
                        self.logger.error(f"Failed to store event from {connector_name}", 
                                        event_id=event.event_id, error=str(e))
                        results['errors'].append({
                            'connector': connector_name,
                            'event_id': event.event_id,
                            'error': str(e)
                        })
                
                results['connectors'][connector_name] = {
                    'events_fetched': len(events),
                    'events_stored': stored_count,
                    'status': 'success'
                }
                results['total_events'] += stored_count
                
                self.logger.info(f"Completed {connector_name}", 
                               fetched=len(events), stored=stored_count)
                
            except Exception as e:
                self.logger.error(f"Failed to run connector: {connector_name}", error=str(e))
                results['connectors'][connector_name] = {
                    'events_fetched': 0,
                    'events_stored': 0,
                    'status': 'error',
                    'error': str(e)
                }
                results['errors'].append({
                    'connector': connector_name,
                    'error': str(e)
                })
        
        self.logger.info("Daily collection completed", 
                        total_events=results['total_events'],
                        errors=len(results['errors']))
        
        return results
    
    async def run_monthly_backfill(self, year: int, month: int) -> Dict[str, Any]:
        """
        Run monthly backfill for all connectors.
        Used for historical data collection.
        """
        self.logger.info("Starting monthly backfill", year=year, month=month)
        
        start_date = date(year, month, 1)
        if month == 12:
            end_date = date(year + 1, 1, 1) - timedelta(days=1)
        else:
            end_date = date(year, month + 1, 1) - timedelta(days=1)
        
        results = {
            'period': {'start': start_date.isoformat(), 'end': end_date.isoformat()},
            'connectors': {},
            'total_events': 0,
            'errors': []
        }
        
        # Run each connector for the month
        for connector_name, connector in self.connectors.items():
            if connector_name == 'ffiec_bankfind':
                continue  # Skip BankFind as it's used for enrichment
            
            try:
                self.logger.info(f"Running monthly backfill for: {connector_name}")
                
                # Fetch updates for the entire month
                events = await connector.fetch_updates(start_date)
                
                # Filter events to the specific month
                month_events = [
                    event for event in events 
                    if start_date <= event.event_date <= end_date
                ]
                
                # Store events
                stored_count = 0
                for event in month_events:
                    try:
                        # Enrich with BankFind data if available
                        if 'ffiec_bankfind' in self.connectors:
                            bankfind = self.connectors['ffiec_bankfind']
                            event = await bankfind._enrich_event_institutions_async(event)
                        
                        # Store in repository
                        if self.repository.upsert_event(event):
                            stored_count += 1
                    
                    except Exception as e:
                        self.logger.error(f"Failed to store event from {connector_name}", 
                                        event_id=event.event_id, error=str(e))
                        results['errors'].append({
                            'connector': connector_name,
                            'event_id': event.event_id,
                            'error': str(e)
                        })
                
                results['connectors'][connector_name] = {
                    'events_fetched': len(events),
                    'events_in_period': len(month_events),
                    'events_stored': stored_count,
                    'status': 'success'
                }
                results['total_events'] += stored_count
                
                self.logger.info(f"Completed monthly backfill for {connector_name}", 
                               fetched=len(events), in_period=len(month_events), stored=stored_count)
                
            except Exception as e:
                self.logger.error(f"Failed to run monthly backfill for: {connector_name}", error=str(e))
                results['connectors'][connector_name] = {
                    'events_fetched': 0,
                    'events_in_period': 0,
                    'events_stored': 0,
                    'status': 'error',
                    'error': str(e)
                }
                results['errors'].append({
                    'connector': connector_name,
                    'error': str(e)
                })
        
        self.logger.info("Monthly backfill completed", 
                        total_events=results['total_events'],
                        errors=len(results['errors']))
        
        return results
    
    async def run_connector(self, connector_name: str, since: date) -> Dict[str, Any]:
        """
        Run a specific connector.
        """
        if connector_name not in self.connectors:
            raise ValueError(f"Unknown connector: {connector_name}")
        
        connector = self.connectors[connector_name]
        
        try:
            self.logger.info(f"Running connector: {connector_name}", since=since.isoformat())
            
            # Fetch updates
            events = await connector.fetch_updates(since)
            
            # Store events
            stored_count = 0
            errors = []
            
            for event in events:
                try:
                    # Enrich with BankFind data if available
                    if 'ffiec_bankfind' in self.connectors:
                        bankfind = self.connectors['ffiec_bankfind']
                        event = await bankfind._enrich_event_institutions_async(event)
                    
                    # Store in repository
                    if self.repository.upsert_event(event):
                        stored_count += 1
                
                except Exception as e:
                    self.logger.error(f"Failed to store event from {connector_name}", 
                                    event_id=event.event_id, error=str(e))
                    errors.append({
                        'event_id': event.event_id,
                        'error': str(e)
                    })
            
            result = {
                'connector': connector_name,
                'since': since.isoformat(),
                'events_fetched': len(events),
                'events_stored': stored_count,
                'status': 'success',
                'errors': errors
            }
            
            self.logger.info(f"Completed {connector_name}", 
                           fetched=len(events), stored=stored_count)
            
            return result
            
        except Exception as e:
            self.logger.error(f"Failed to run connector: {connector_name}", error=str(e))
            return {
                'connector': connector_name,
                'since': since.isoformat(),
                'events_fetched': 0,
                'events_stored': 0,
                'status': 'error',
                'error': str(e),
                'errors': []
            }
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get collection statistics."""
        try:
            db_stats = self.repository.get_statistics()
            
            stats = {
                'database': db_stats,
                'connectors': {
                    name: {
                        'status': 'active',
                        'schedule': self.schedules.get(name, {})
                    }
                    for name in self.connectors.keys()
                },
                'last_updated': datetime.now().isoformat()
            }
            
            return stats
            
        except Exception as e:
            self.logger.error("Failed to get statistics", error=str(e))
            return {'error': str(e)}
    
    async def health_check(self) -> Dict[str, Any]:
        """Run health check on all connectors."""
        health_results = {
            'timestamp': datetime.now().isoformat(),
            'connectors': {},
            'overall_status': 'healthy'
        }
        
        for connector_name, connector in self.connectors.items():
            try:
                # Test connector with a recent date
                test_date = date.today() - timedelta(days=7)
                
                # Try to discover items (this tests the connector without making too many requests)
                if hasattr(connector, 'discover_items'):
                    items = await connector.discover_items(test_date)
                    status = 'healthy'
                    message = f"Discovered {len(items)} items"
                else:
                    status = 'unknown'
                    message = "No discover_items method"
                
                health_results['connectors'][connector_name] = {
                    'status': status,
                    'message': message,
                    'test_date': test_date.isoformat()
                }
                
                if status != 'healthy':
                    health_results['overall_status'] = 'degraded'
                
            except Exception as e:
                health_results['connectors'][connector_name] = {
                    'status': 'unhealthy',
                    'message': str(e),
                    'test_date': test_date.isoformat()
                }
                health_results['overall_status'] = 'unhealthy'
        
        return health_results


# Convenience functions for common operations
async def run_daily_collection(repository: EventRepository, target_date: Optional[date] = None) -> Dict[str, Any]:
    """Run daily collection with default repository."""
    scheduler = EventScheduler(repository)
    return await scheduler.run_daily_collection(target_date)


async def run_monthly_backfill(repository: EventRepository, year: int, month: int) -> Dict[str, Any]:
    """Run monthly backfill with default repository."""
    scheduler = EventScheduler(repository)
    return await scheduler.run_monthly_backfill(year, month)


async def run_connector(repository: EventRepository, connector_name: str, since: date) -> Dict[str, Any]:
    """Run specific connector with default repository."""
    scheduler = EventScheduler(repository)
    return await scheduler.run_connector(connector_name, since)
