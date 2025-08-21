"""
FFIEC/FDIC BankFind Suite connector for institution identity and crosswalk.
Provides institution enrichment with CERT, primary regulator, website, historical names, and failures data.
"""

import os
import json
from datetime import date
from typing import List, Dict, Any, Optional
import httpx
from selectolax.parser import HTMLParser

from .base import BaseConnector
from ingestion.normalizers.events_model import Event, SourceRef, ReputationalDrivers, ReputationalDamage


class BankFindConnector(BaseConnector):
    """Connector for FDIC BankFind Suite API."""
    
    def __init__(self):
        super().__init__("ffiec_bankfind")
        self.api_key = os.getenv("FDIC_API_KEY")
        if not self.api_key:
            self.logger.warning("FDIC_API_KEY not found in environment")
        
        # Updated to use the correct FDIC BankFind Suite API endpoints
        self.base_url = "https://banks.data.fdic.gov"
    
    async def discover_items(self, since: date) -> List[Dict[str, Any]]:
        """BankFind doesn't have a discovery mechanism - it's used for enrichment."""
        return []
    
    async def fetch_item_detail(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Not applicable for BankFind."""
        return {}
    
    def parse_item(self, item_data: Dict[str, Any]) -> Dict[str, Any]:
        """Not applicable for BankFind."""
        return {}
    
    def normalize_item(self, parsed_data: Dict[str, Any]) -> Event:
        """Not applicable for BankFind."""
        raise NotImplementedError("BankFind is used for enrichment, not event creation")
    
    async def search_institution(self, name: str, state: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Search for institution by name and optionally state.
        Returns institution data with CERT, regulator, etc.
        """
        if not self.api_key:
            self.logger.warning("Cannot search without FDIC_API_KEY")
            return None
        
        try:
            # Build search filters
            filters = f'NAME:"{name}"'
            if state:
                filters += f' AND STALP:"{state}"'
            
            params = {
                'format': 'json',
                'fields': 'CERT,NAME,STALP,ACTIVE,PRIMARY_REG,ID_RSSD,LEI,WEBSITE,OFFICES',
                'filters': filters,
                'limit': 10
            }
            
            headers = {
                'Accept': 'application/json'
            }
            
            response = await self._make_request(
                f"{self.base_url}/api/institutions",
                params=params,
                headers=headers
            )
            
            data = response.json()
            institutions = data.get('data', [])
            
            if institutions:
                # Return the first (most relevant) match
                return institutions[0]
            
            return None
            
        except Exception as e:
            self.logger.error("Failed to search institution", name=name, state=state, error=str(e))
            return None
    
    async def get_institution_detail(self, cert: str) -> Optional[Dict[str, Any]]:
        """
        Get detailed institution information by CERT number.
        """
        if not self.api_key:
            self.logger.warning("Cannot fetch details without FDIC_API_KEY")
            return None
        
        try:
            params = {
                'format': 'json',
                'fields': 'CERT,NAME,STALP,ACTIVE,PRIMARY_REG,ID_RSSD,LEI,WEBSITE,OFFICES,HISTORY'
            }
            
            headers = {
                'Accept': 'application/json'
            }
            
            response = await self._make_request(
                f"{self.base_url}/api/institutions/{cert}",
                params=params,
                headers=headers
            )
            
            data = response.json()
            return data.get('data', [{}])[0] if data.get('data') else None
            
        except Exception as e:
            self.logger.error("Failed to get institution detail", cert=cert, error=str(e))
            return None
    
    async def get_institution_history(self, cert: str) -> List[Dict[str, Any]]:
        """
        Get institution history including mergers, acquisitions, name changes.
        """
        if not self.api_key:
            self.logger.warning("Cannot fetch history without FDIC_API_KEY")
            return []
        
        try:
            params = {
                'format': 'json',
                'fields': 'CERT,NAME,STALP,ACTIVE,PRIMARY_REG,ID_RSSD,LEI,WEBSITE,OFFICES,HISTORY'
            }
            
            headers = {
                'Accept': 'application/json'
            }
            
            response = await self._make_request(
                f"{self.base_url}/api/institutions/{cert}/history",
                params=params,
                headers=headers
            )
            
            data = response.json()
            return data.get('data', [])
            
        except Exception as e:
            self.logger.error("Failed to get institution history", cert=cert, error=str(e))
            return []
    
    async def get_failed_institutions(self, since: date) -> List[Dict[str, Any]]:
        """
        Get list of failed institutions since a given date.
        """
        if not self.api_key:
            self.logger.warning("Cannot fetch failures without FDIC_API_KEY")
            return []
        
        try:
            params = {
                'format': 'json',
                'fields': 'CERT,NAME,STALP,FAIL_DATE,ACQUIRER,TRANSACTION_TYPE',
                'filters': f'FAIL_DATE>="{since.isoformat()}"',
                'limit': 100
            }
            
            headers = {
                'Accept': 'application/json'
            }
            
            response = await self._make_request(
                f"{self.base_url}/api/failures",
                params=params,
                headers=headers
            )
            
            data = response.json()
            return data.get('data', [])
            
        except Exception as e:
            self.logger.error("Failed to get failed institutions", since=since.isoformat(), error=str(e))
            return []
    
    def enrich_event_institutions(self, event: Event) -> Event:
        """
        Enrich event with institution information from BankFind.
        This is a synchronous wrapper for async operations.
        """
        import asyncio
        
        try:
            # Run async enrichment
            loop = asyncio.get_event_loop()
            return loop.run_until_complete(self._enrich_event_institutions_async(event))
        except RuntimeError:
            # No event loop, create new one
            return asyncio.run(self._enrich_event_institutions_async(event))
    
    async def _enrich_event_institutions_async(self, event: Event) -> Event:
        """
        Asynchronously enrich event with institution information.
        """
        enriched_institutions = []
        
        for institution_name in event.institutions:
            # Try to find institution in BankFind
            bank_data = await self.search_institution(institution_name)
            
            if bank_data:
                # Add BankFind identifiers to the event
                enriched_institutions.append({
                    'name': institution_name,
                    'cert': bank_data.get('CERT'),
                    'rssd': bank_data.get('ID_RSSD'),
                    'lei': bank_data.get('LEI'),
                    'primary_regulator': bank_data.get('PRIMARY_REG'),
                    'state': bank_data.get('STALP'),
                    'active': bank_data.get('ACTIVE'),
                    'website': bank_data.get('WEBSITE')
                })
                
                self.logger.info(
                    "Enriched institution",
                    name=institution_name,
                    cert=bank_data.get('CERT'),
                    regulator=bank_data.get('PRIMARY_REG')
                )
            else:
                # Keep original institution name if not found
                enriched_institutions.append({
                    'name': institution_name,
                    'cert': None,
                    'rssd': None,
                    'lei': None,
                    'primary_regulator': None,
                    'state': None,
                    'active': None,
                    'website': None
                })
        
        # Update event with enriched data
        # Note: This would require extending the Event model to include enriched institution data
        # For now, we'll just log the enrichment results
        
        return event
    
    async def resolve_institution_identity(self, name: str, state: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Resolve institution identity using fuzzy matching and BankFind data.
        Returns standardized institution information.
        """
        from rapidfuzz import fuzz
        
        # First try exact search
        result = await self.search_institution(name, state)
        if result:
            return result
        
        # If no exact match, try fuzzy search
        # Get a broader set of institutions for fuzzy matching
        try:
            params = {
                'format': 'json',
                'fields': 'CERT,NAME,STALP,ACTIVE,PRIMARY_REG,ID_RSSD',
                'limit': 100
            }
            
            if state:
                params['filters'] = f'STALP:"{state}"'
            
            headers = {
                'Accept': 'application/json'
            }
            
            response = await self._make_request(
                f"{self.base_url}/api/institutions",
                params=params,
                headers=headers
            )
            
            data = response.json()
            institutions = data.get('data', [])
            
            # Find best match using fuzzy string matching
            best_match = None
            best_score = 0
            
            for inst in institutions:
                inst_name = inst.get('NAME', '')
                score = fuzz.ratio(name.lower(), inst_name.lower())
                
                if score > best_score and score > 80:  # 80% similarity threshold
                    best_score = score
                    best_match = inst
            
            if best_match:
                self.logger.info(
                    "Fuzzy matched institution",
                    original=name,
                    matched=best_match.get('NAME'),
                    score=best_score
                )
                return best_match
            
            return None
            
        except Exception as e:
            self.logger.error("Failed to resolve institution identity", name=name, error=str(e))
            return None
