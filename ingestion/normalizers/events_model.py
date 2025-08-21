"""
Pydantic models for bank reputation damage events.
Maps to the strict JSON schema specified in the requirements.
"""

from datetime import date
from typing import List, Literal, Optional, Dict, Any
from pydantic import BaseModel, HttpUrl, conint, constr


class SourceRef(BaseModel):
    """Reference to a source document or article."""
    title: str
    publisher: str
    url: HttpUrl
    date_published: date
    source_type: Literal["regulator", "media", "court"]


class ReputationalDrivers(BaseModel):
    """Drivers of reputational damage."""
    fine_usd: Optional[int] = 0
    customers_affected: Optional[int] = None
    service_disruption_hours: Optional[int] = None
    executive_changes: bool = False
    litigation_status: Literal["none", "filed", "settled", "ongoing"] = "none"
    regulator_involved: List[str] = []


class ReputationalDamage(BaseModel):
    """Reputational damage assessment."""
    nature: List[str]
    materiality_score: conint(ge=1, le=5)
    drivers: ReputationalDrivers


class Event(BaseModel):
    """Complete bank reputation damage event."""
    event_id: str  # kebab-case
    title: str
    institutions: List[str]
    parent_company: Optional[str] = None
    us_operations: bool = True
    jurisdictions: List[str]
    categories: List[str]
    event_date: date
    reported_dates: List[date]
    summary: constr(max_length=480)
    reputational_damage: ReputationalDamage
    amounts: Dict[str, Any]  # penalties_usd, settlements_usd, other_amounts_usd, original_text
    sources: List[SourceRef]
    source_count: int
    confidence: Literal["high", "medium", "low"]


class QueryInfo(BaseModel):
    """Query metadata."""
    timeframe: Dict[str, str]  # start, end, timezone
    scope_note: str


class EventCollection(BaseModel):
    """Complete collection of events with metadata."""
    query: QueryInfo
    events: List[Event]
    dedupe_note: str
    coverage_notes: str
    last_updated: str


# Category and nature constants for validation
VALID_CATEGORIES = [
    "regulatory_action", "lawsuit", "fine", "data_breach", "fraud", 
    "operational_outage", "discrimination", "sanctions_aml", "executive_misconduct",
    "esg_controversy", "labor_dispute", "customer_service_crisis", "technology_failure",
    "partnership_failure", "governance_issue", "market_manipulation", "predatory_practices",
    "investigation", "financial_performance", "brand_marketing", "other"
]

VALID_NATURE_TYPES = [
    "compliance_failure", "customer_trust", "governance", "operational_resilience",
    "data_security", "fairness_discrimination", "market_integrity", "executive_conduct",
    "environmental_social", "labor_relations", "product_controversy", "partner_reputation", "other"
]

VALID_REGULATORS = [
    "OCC", "FDIC", "FRB", "SEC", "CFPB", "DOJ", "State AG", "NYDFS", "NCUA", "Other"
]
