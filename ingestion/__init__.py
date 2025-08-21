"""
Bank Reputation Damage Events - Ingestion Package

This package contains the data ingestion components for collecting
bank reputation damage events from multiple authoritative sources.
"""

__version__ = "1.0.0"
__author__ = "Bank Reputation Monitor"
__description__ = "Comprehensive data ingestion system for bank reputation damage events"

from .normalizers.events_model import Event, SourceRef, ReputationalDrivers, ReputationalDamage

__all__ = [
    "Event",
    "SourceRef", 
    "ReputationalDrivers",
    "ReputationalDamage"
]

