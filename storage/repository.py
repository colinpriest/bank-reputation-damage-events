"""
Storage repository for bank reputation damage events.
Handles upsert logic and idempotency keys.
"""

import json
import sqlite3
from datetime import datetime, date
from typing import List, Dict, Any, Optional
import structlog
from pathlib import Path

from ingestion.normalizers.events_model import Event, SourceRef


class EventRepository:
    """Repository for storing and retrieving bank reputation damage events."""
    
    def __init__(self, db_path: str = "bank_events.db"):
        self.db_path = db_path
        self.logger = structlog.get_logger("repository")
        self._init_database()
    
    def _init_database(self):
        """Initialize the database with required tables."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS institutions (
                    cert TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    rssd TEXT,
                    lei TEXT,
                    state TEXT,
                    primary_reg TEXT,
                    aliases TEXT,  -- JSONB equivalent
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS events (
                    event_id TEXT PRIMARY KEY,
                    payload TEXT NOT NULL,  -- JSONB equivalent
                    event_date DATE NOT NULL,
                    categories TEXT,  -- JSON array
                    regulators TEXT,  -- JSON array
                    penalties_usd BIGINT DEFAULT 0,
                    customers_affected BIGINT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS sources (
                    event_id TEXT,
                    url TEXT NOT NULL,
                    title TEXT,
                    publisher TEXT,
                    date_published DATE,
                    type TEXT,
                    FOREIGN KEY (event_id) REFERENCES events(event_id)
                )
            """)
            
            # Create indexes
            conn.execute("CREATE INDEX IF NOT EXISTS idx_events_date ON events(event_date)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_events_penalties ON events(penalties_usd)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_sources_event_id ON sources(event_id)")
            
            conn.commit()
    
    def upsert_event(self, event: Event) -> bool:
        """
        Upsert an event with idempotency.
        Returns True if event was inserted/updated, False if already exists with same data.
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Check if event exists
                cursor = conn.execute(
                    "SELECT payload FROM events WHERE event_id = ?",
                    (event.event_id,)
                )
                existing = cursor.fetchone()
                
                # Serialize event to JSON
                event_json = event.model_dump_json()
                
                if existing:
                    # Check if data has changed
                    if existing[0] == event_json:
                        self.logger.debug("Event unchanged", event_id=event.event_id)
                        return False
                    
                    # Update existing event
                    conn.execute("""
                        UPDATE events 
                        SET payload = ?, updated_at = CURRENT_TIMESTAMP
                        WHERE event_id = ?
                    """, (event_json, event.event_id))
                    
                    self.logger.info("Event updated", event_id=event.event_id)
                else:
                    # Insert new event
                    conn.execute("""
                        INSERT INTO events (
                            event_id, payload, event_date, categories, regulators,
                            penalties_usd, customers_affected
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (
                        event.event_id,
                        event_json,
                        event.event_date.isoformat(),
                        json.dumps(event.categories),
                        json.dumps(event.reputational_damage.drivers.regulator_involved),
                        event.amounts.get('penalties_usd', 0),
                        event.reputational_damage.drivers.customers_affected
                    ))
                    
                    self.logger.info("Event inserted", event_id=event.event_id)
                
                # Upsert sources
                self._upsert_sources(conn, event.event_id, event.sources)
                
                conn.commit()
                return True
                
        except Exception as e:
            self.logger.error("Failed to upsert event", event_id=event.event_id, error=str(e))
            raise
    
    def _upsert_sources(self, conn: sqlite3.Connection, event_id: str, sources: List[SourceRef]):
        """Upsert sources for an event."""
        # Delete existing sources
        conn.execute("DELETE FROM sources WHERE event_id = ?", (event_id,))
        
        # Insert new sources
        for source in sources:
            conn.execute("""
                INSERT INTO sources (
                    event_id, url, title, publisher, date_published, type
                ) VALUES (?, ?, ?, ?, ?, ?)
            """, (
                event_id,
                str(source.url),
                source.title,
                source.publisher,
                source.date_published.isoformat(),
                source.source_type
            ))
    
    def get_events(self, 
                   start_date: Optional[date] = None,
                   end_date: Optional[date] = None,
                   categories: Optional[List[str]] = None,
                   regulators: Optional[List[str]] = None,
                   limit: Optional[int] = None) -> List[Event]:
        """Retrieve events with optional filtering."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                query = "SELECT payload FROM events WHERE 1=1"
                params = []
                
                if start_date:
                    query += " AND event_date >= ?"
                    params.append(start_date.isoformat())
                
                if end_date:
                    query += " AND event_date <= ?"
                    params.append(end_date.isoformat())
                
                if categories:
                    # Simple JSON array contains check
                    category_conditions = []
                    for category in categories:
                        category_conditions.append("categories LIKE ?")
                        params.append(f'%"{category}"%')
                    query += f" AND ({' OR '.join(category_conditions)})"
                
                if regulators:
                    # Simple JSON array contains check
                    regulator_conditions = []
                    for regulator in regulators:
                        regulator_conditions.append("regulators LIKE ?")
                        params.append(f'%"{regulator}"%')
                    query += f" AND ({' OR '.join(regulator_conditions)})"
                
                query += " ORDER BY event_date DESC"
                
                if limit:
                    query += " LIMIT ?"
                    params.append(limit)
                
                cursor = conn.execute(query, params)
                rows = cursor.fetchall()
                
                events = []
                for row in rows:
                    try:
                        event_data = json.loads(row[0])
                        event = Event(**event_data)
                        events.append(event)
                    except Exception as e:
                        self.logger.error("Failed to deserialize event", error=str(e))
                        continue
                
                return events
                
        except Exception as e:
            self.logger.error("Failed to retrieve events", error=str(e))
            raise
    
    def get_event_by_id(self, event_id: str) -> Optional[Event]:
        """Retrieve a specific event by ID."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute(
                    "SELECT payload FROM events WHERE event_id = ?",
                    (event_id,)
                )
                row = cursor.fetchone()
                
                if row:
                    event_data = json.loads(row[0])
                    return Event(**event_data)
                return None
                
        except Exception as e:
            self.logger.error("Failed to retrieve event", event_id=event_id, error=str(e))
            raise
    
    def get_events_by_institution(self, institution_name: str) -> List[Event]:
        """Retrieve events for a specific institution."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute(
                    "SELECT payload FROM events WHERE payload LIKE ?",
                    (f'%"institutions":%"{institution_name}"%',)
                )
                rows = cursor.fetchall()
                
                events = []
                for row in rows:
                    try:
                        event_data = json.loads(row[0])
                        event = Event(**event_data)
                        events.append(event)
                    except Exception as e:
                        self.logger.error("Failed to deserialize event", error=str(e))
                        continue
                
                return events
                
        except Exception as e:
            self.logger.error("Failed to retrieve events by institution", 
                            institution=institution_name, error=str(e))
            raise
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get database statistics."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                stats = {}
                
                # Total events
                cursor = conn.execute("SELECT COUNT(*) FROM events")
                stats['total_events'] = cursor.fetchone()[0]
                
                # Date range
                cursor = conn.execute("""
                    SELECT MIN(event_date), MAX(event_date) FROM events
                """)
                date_range = cursor.fetchone()
                if date_range[0]:
                    stats['date_range'] = {
                        'start': date_range[0],
                        'end': date_range[1]
                    }
                
                # Total penalties
                cursor = conn.execute("SELECT SUM(penalties_usd) FROM events")
                stats['total_penalties_usd'] = cursor.fetchone()[0] or 0
                
                # Categories distribution
                cursor = conn.execute("""
                    SELECT categories, COUNT(*) FROM events 
                    GROUP BY categories
                """)
                category_counts = {}
                for row in cursor.fetchall():
                    try:
                        categories = json.loads(row[0])
                        for category in categories:
                            category_counts[category] = category_counts.get(category, 0) + row[1]
                    except:
                        continue
                stats['category_distribution'] = category_counts
                
                # Regulators distribution
                cursor = conn.execute("""
                    SELECT regulators, COUNT(*) FROM events 
                    GROUP BY regulators
                """)
                regulator_counts = {}
                for row in cursor.fetchall():
                    try:
                        regulators = json.loads(row[0])
                        for regulator in regulators:
                            regulator_counts[regulator] = regulator_counts.get(regulator, 0) + row[1]
                    except:
                        continue
                stats['regulator_distribution'] = regulator_counts
                
                return stats
                
        except Exception as e:
            self.logger.error("Failed to get statistics", error=str(e))
            raise
    
    def upsert_institution(self, cert: str, name: str, **kwargs) -> bool:
        """Upsert institution information."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Check if institution exists
                cursor = conn.execute(
                    "SELECT name FROM institutions WHERE cert = ?",
                    (cert,)
                )
                existing = cursor.fetchone()
                
                if existing:
                    # Update existing
                    conn.execute("""
                        UPDATE institutions 
                        SET name = ?, rssd = ?, lei = ?, state = ?, primary_reg = ?, 
                            aliases = ?, updated_at = CURRENT_TIMESTAMP
                        WHERE cert = ?
                    """, (
                        name,
                        kwargs.get('rssd'),
                        kwargs.get('lei'),
                        kwargs.get('state'),
                        kwargs.get('primary_reg'),
                        json.dumps(kwargs.get('aliases', [])),
                        cert
                    ))
                else:
                    # Insert new
                    conn.execute("""
                        INSERT INTO institutions (
                            cert, name, rssd, lei, state, primary_reg, aliases
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (
                        cert,
                        name,
                        kwargs.get('rssd'),
                        kwargs.get('lei'),
                        kwargs.get('state'),
                        kwargs.get('primary_reg'),
                        json.dumps(kwargs.get('aliases', []))
                    ))
                
                conn.commit()
                return True
                
        except Exception as e:
            self.logger.error("Failed to upsert institution", cert=cert, error=str(e))
            raise
    
    def get_institution(self, cert: str) -> Optional[Dict[str, Any]]:
        """Get institution information by CERT."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute(
                    "SELECT * FROM institutions WHERE cert = ?",
                    (cert,)
                )
                row = cursor.fetchone()
                
                if row:
                    return {
                        'cert': row[0],
                        'name': row[1],
                        'rssd': row[2],
                        'lei': row[3],
                        'state': row[4],
                        'primary_reg': row[5],
                        'aliases': json.loads(row[6]) if row[6] else [],
                        'updated_at': row[7]
                    }
                return None
                
        except Exception as e:
            self.logger.error("Failed to get institution", cert=cert, error=str(e))
            raise
