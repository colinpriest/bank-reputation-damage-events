"""
Data normalization components for bank reputation damage events.
"""

from .events_model import Event, SourceRef, ReputationalDrivers, ReputationalDamage
from .mappings import (
    map_category, 
    map_nature, 
    map_regulator, 
    calculate_materiality_score,
    extract_money_amounts,
    normalize_money_to_usd
)

__all__ = [
    "Event",
    "SourceRef",
    "ReputationalDrivers", 
    "ReputationalDamage",
    "map_category",
    "map_nature",
    "map_regulator",
    "calculate_materiality_score",
    "extract_money_amounts",
    "normalize_money_to_usd"
]



