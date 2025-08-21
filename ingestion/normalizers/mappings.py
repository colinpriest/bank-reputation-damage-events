"""
Category and nature mappings for bank reputation damage events.
Provides controlled vocabulary for normalization across different data sources.
"""

from typing import Dict, List, Tuple
import re


# Category mappings from source-specific terms to standardized categories
CATEGORY_MAPPINGS = {
    # Regulatory actions
    "consent order": "regulatory_action",
    "cease and desist": "regulatory_action", 
    "cease-and-desist": "regulatory_action",
    "order to cease and desist": "regulatory_action",
    "formal agreement": "regulatory_action",
    "written agreement": "regulatory_action",
    "memorandum of understanding": "regulatory_action",
    "enforcement action": "regulatory_action",
    "regulatory order": "regulatory_action",
    
    # Fines and penalties
    "civil money penalty": "fine",
    "civil monetary penalty": "fine",
    "monetary penalty": "fine",
    "financial penalty": "fine",
    "administrative penalty": "fine",
    "regulatory fine": "fine",
    
    # Data breaches
    "data breach": "data_breach",
    "data breach notice": "data_breach",
    "security breach": "data_breach",
    "cybersecurity incident": "data_breach",
    "privacy breach": "data_breach",
    "information security incident": "data_breach",
    
    # Bank failures
    "bank failure": "financial_performance",
    "bank closure": "financial_performance",
    "receivership": "financial_performance",
    "liquidation": "financial_performance",
    "bankruptcy": "financial_performance",
    
    # Lawsuits
    "lawsuit": "lawsuit",
    "complaint": "lawsuit",
    "litigation": "lawsuit",
    "class action": "lawsuit",
    "legal action": "lawsuit",
    "court filing": "lawsuit",
    
    # Fraud
    "fraud": "fraud",
    "fraudulent activity": "fraud",
    "financial fraud": "fraud",
    "banking fraud": "fraud",
    
    # Operational issues
    "outage": "operational_outage",
    "service disruption": "operational_outage",
    "system failure": "operational_outage",
    "technology failure": "technology_failure",
    "cyber attack": "technology_failure",
    
    # Compliance failures
    "bsa violation": "sanctions_aml",
    "aml violation": "sanctions_aml",
    "anti-money laundering": "sanctions_aml",
    "sanctions violation": "sanctions_aml",
    "compliance failure": "compliance_failure",
    
    # Discrimination
    "discrimination": "discrimination",
    "fair lending violation": "discrimination",
    "redlining": "discrimination",
    "predatory lending": "predatory_practices",
    
    # Executive misconduct
    "executive misconduct": "executive_misconduct",
    "executive resignation": "executive_misconduct",
    "ceo scandal": "executive_misconduct",
    "management misconduct": "executive_misconduct",
    
    # ESG and social issues
    "environmental controversy": "esg_controversy",
    "greenwashing": "esg_controversy",
    "labor dispute": "labor_dispute",
    "strike": "labor_dispute",
    "mass layoff": "labor_dispute",
    
    # Customer service
    "customer service failure": "customer_service_crisis",
    "service crisis": "customer_service_crisis",
    "customer complaint": "customer_service_crisis",
    
    # Governance
    "governance issue": "governance_issue",
    "board controversy": "governance_issue",
    "shareholder activism": "governance_issue",
    "proxy battle": "governance_issue",
    
    # Market integrity
    "market manipulation": "market_manipulation",
    "insider trading": "market_manipulation",
    "securities fraud": "market_manipulation",
    
    # Investigations
    "investigation": "investigation",
    "probe": "investigation",
    "inquiry": "investigation",
    "examination": "investigation",
    
    # Brand and marketing
    "marketing controversy": "brand_marketing",
    "brand crisis": "brand_marketing",
    "pr crisis": "brand_marketing",
    "public relations crisis": "brand_marketing",
    
    # Partnership failures
    "partnership failure": "partnership_failure",
    "vendor scandal": "partnership_failure",
    "fintech partnership failure": "partnership_failure",
}


# Nature mappings for reputational damage
NATURE_MAPPINGS = {
    # Compliance failures
    "compliance failure": "compliance_failure",
    "regulatory violation": "compliance_failure",
    "bsa/aml failure": "compliance_failure",
    "sanctions violation": "compliance_failure",
    
    # Customer trust
    "customer trust": "customer_trust",
    "customer confidence": "customer_trust",
    "trust breach": "customer_trust",
    "customer harm": "customer_trust",
    
    # Governance
    "governance": "governance",
    "board oversight": "governance",
    "management oversight": "governance",
    "corporate governance": "governance",
    
    # Operational resilience
    "operational resilience": "operational_resilience",
    "system reliability": "operational_resilience",
    "service availability": "operational_resilience",
    "business continuity": "operational_resilience",
    
    # Data security
    "data security": "data_security",
    "information security": "data_security",
    "cybersecurity": "data_security",
    "privacy protection": "data_security",
    
    # Fairness and discrimination
    "fairness discrimination": "fairness_discrimination",
    "discriminatory practices": "fairness_discrimination",
    "fair lending": "fairness_discrimination",
    "equal access": "fairness_discrimination",
    
    # Market integrity
    "market integrity": "market_integrity",
    "financial market integrity": "market_integrity",
    "trading integrity": "market_integrity",
    "market manipulation": "market_integrity",
    
    # Executive conduct
    "executive conduct": "executive_conduct",
    "leadership conduct": "executive_conduct",
    "management behavior": "executive_conduct",
    "executive ethics": "executive_conduct",
    
    # Environmental and social
    "environmental social": "environmental_social",
    "esg impact": "environmental_social",
    "social responsibility": "environmental_social",
    "environmental impact": "environmental_social",
    
    # Labor relations
    "labor relations": "labor_relations",
    "employee relations": "labor_relations",
    "workforce management": "labor_relations",
    "employment practices": "labor_relations",
    
    # Product controversy
    "product controversy": "product_controversy",
    "product practices": "product_controversy",
    "lending practices": "product_controversy",
    "fee practices": "product_controversy",
    
    # Partner reputation
    "partner reputation": "partner_reputation",
    "vendor reputation": "partner_reputation",
    "third-party risk": "partner_reputation",
    "business partner": "partner_reputation",
}


# Regulator mappings
REGULATOR_MAPPINGS = {
    "occ": "OCC",
    "office of the comptroller of the currency": "OCC",
    "fdic": "FDIC", 
    "federal deposit insurance corporation": "FDIC",
    "frb": "FRB",
    "federal reserve board": "FRB",
    "federal reserve": "FRB",
    "sec": "SEC",
    "securities and exchange commission": "SEC",
    "cfpb": "CFPB",
    "consumer financial protection bureau": "CFPB",
    "doj": "DOJ",
    "department of justice": "DOJ",
    "nydfs": "NYDFS",
    "new york department of financial services": "NYDFS",
    "ncua": "NCUA",
    "national credit union administration": "NCUA",
    "state ag": "State AG",
    "state attorney general": "State AG",
    "attorney general": "State AG",
}


# Materiality scoring rules
MATERIALITY_RULES = [
    # Score 5: Most severe
    {
        "score": 5,
        "conditions": [
            {"field": "amounts.penalties_usd", "operator": ">=", "value": 1000000000},  # ≥ $1B
            {"field": "reputational_damage.drivers.customers_affected", "operator": ">=", "value": 5000000},  # ≥ 5M
            {"field": "categories", "operator": "contains", "value": "financial_performance"},  # Bank failure
            {"field": "title", "operator": "contains_any", "value": ["congressional hearing", "ceo resignation scandal"]},
        ]
    },
    # Score 4: High severity
    {
        "score": 4,
        "conditions": [
            {"field": "amounts.penalties_usd", "operator": ">=", "value": 100000000},  # ≥ $100M
            {"field": "reputational_damage.drivers.customers_affected", "operator": ">=", "value": 1000000},  # ≥ 1M
            {"field": "title", "operator": "contains_any", "value": ["mass layoff", "esg controversy", "sustained negative press"]},
        ]
    },
    # Score 3: Medium-high severity
    {
        "score": 3,
        "conditions": [
            {"field": "amounts.penalties_usd", "operator": ">=", "value": 10000000},  # ≥ $10M
            {"field": "reputational_damage.drivers.customers_affected", "operator": ">=", "value": 100000},  # ≥ 100k
            {"field": "title", "operator": "contains_any", "value": ["significant outage", "workforce reduction", "multiple state ag"]},
        ]
    },
    # Score 2: Medium severity
    {
        "score": 2,
        "conditions": [
            {"field": "amounts.penalties_usd", "operator": ">=", "value": 1000000},  # ≥ $1M
            {"field": "reputational_damage.drivers.customers_affected", "operator": ">=", "value": 10000},  # ≥ 10k
            {"field": "title", "operator": "contains_any", "value": ["executive misconduct", "branch closure", "fintech partnership failure"]},
        ]
    },
    # Score 1: Low severity (default)
    {
        "score": 1,
        "conditions": [
            {"field": "title", "operator": "contains_any", "value": ["investigation", "discrimination allegation", "customer service failure"]},
        ]
    },
]


def map_category(source_text: str) -> str:
    """Map source text to standardized category."""
    source_lower = source_text.lower()
    
    for pattern, category in CATEGORY_MAPPINGS.items():
        if pattern in source_lower:
            return category
    
    return "other"


def map_nature(source_text: str) -> List[str]:
    """Map source text to standardized nature types."""
    source_lower = source_text.lower()
    natures = []
    
    for pattern, nature in NATURE_MAPPINGS.items():
        if pattern in source_lower:
            natures.append(nature)
    
    if not natures:
        natures.append("other")
    
    return natures


def map_regulator(source_text: str) -> str:
    """Map source text to standardized regulator name."""
    source_lower = source_text.lower()
    
    for pattern, regulator in REGULATOR_MAPPINGS.items():
        if pattern in source_lower:
            return regulator
    
    return "Other"


def calculate_materiality_score(event_data: Dict) -> int:
    """Calculate materiality score based on event data."""
    for rule in MATERIALITY_RULES:
        score = rule["score"]
        conditions = rule["conditions"]
        
        for condition in conditions:
            field = condition["field"]
            operator = condition["operator"]
            value = condition["value"]
            
            # Navigate nested fields
            field_value = event_data
            for key in field.split("."):
                if isinstance(field_value, dict):
                    field_value = field_value.get(key, "")
                else:
                    field_value = ""
                    break
            
            # Apply operator
            if operator == ">=":
                if isinstance(field_value, (int, float)) and field_value >= value:
                    return score
            elif operator == "contains":
                if isinstance(field_value, str) and value in field_value.lower():
                    return score
                elif isinstance(field_value, list) and value in field_value:
                    return score
            elif operator == "contains_any":
                if isinstance(value, list):
                    for v in value:
                        if isinstance(field_value, str) and v in field_value.lower():
                            return score
    
    return 1  # Default to lowest score


# Money amount extraction patterns
MONEY_PATTERNS = [
    r'\$[\d,]+(?:\.\d{2})?(?:\s*(?:million|billion|thousand|k|m|b))?',
    r'[\d,]+(?:\.\d{2})?\s*(?:million|billion|thousand|k|m|b)\s*(?:dollars?|usd)?',
    r'(?:penalty|fine|settlement|payment)\s*(?:of\s*)?\$?[\d,]+(?:\.\d{2})?',
]


def extract_money_amounts(text: str) -> Tuple[int, str]:
    """Extract money amounts from text and return normalized USD amount and original text."""
    original_text = ""
    total_amount = 0
    
    for pattern in MONEY_PATTERNS:
        matches = re.findall(pattern, text, re.IGNORECASE)
        for match in matches:
            original_text += f"{match}; "
            
            # Normalize to USD
            amount = normalize_money_to_usd(match)
            total_amount += amount
    
    return total_amount, original_text.strip("; ")


def normalize_money_to_usd(amount_text: str) -> int:
    """Convert money text to USD amount in cents."""
    # Remove currency symbols and commas
    clean_text = re.sub(r'[$,]', '', amount_text.lower())
    
    # Extract number
    number_match = re.search(r'(\d+(?:\.\d{2})?)', clean_text)
    if not number_match:
        return 0
    
    number = float(number_match.group(1))
    
    # Apply multipliers
    if any(word in clean_text for word in ['billion', 'b']):
        number *= 1000000000
    elif any(word in clean_text for word in ['million', 'm']):
        number *= 1000000
    elif any(word in clean_text for word in ['thousand', 'k']):
        number *= 1000
    
    return int(number)
