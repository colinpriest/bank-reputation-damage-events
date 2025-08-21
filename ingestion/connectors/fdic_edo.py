"""
FDIC Enforcement Decisions & Orders (ED&O) connector.
Scrapes monthly updated database of FDIC enforcement decisions, orders, and notices.
"""

import re
import json
from datetime import date, datetime
from typing import List, Dict, Any, Optional
from urllib.parse import urljoin, urlparse
import dateparser
from selectolax.parser import HTMLParser
from pdfminer.high_level import extract_text
import io

from .base import BaseConnector
from ingestion.normalizers.events_model import Event, SourceRef, ReputationalDrivers, ReputationalDamage


class FdicEdoConnector(BaseConnector):
    """Connector for FDIC Enforcement Decisions & Orders."""
    
    def __init__(self):
        super().__init__("fdic_edo")
        self.base_url = "https://orders.fdic.gov"
        self.search_url = f"{self.base_url}/s/"
    
    async def discover_items(self, since: date) -> List[Dict[str, Any]]:
        """Discover enforcement orders from the FDIC ED&O search page."""
        try:
            # Get the main search page
            response = await self._make_request(self.search_url)
            html = HTMLParser(response.text)
            
            items = []
            
            # Look for order listings in various sections
            # Press Release Orders section
            press_release_section = html.css_first('section:contains("Press Release Orders")')
            if press_release_section:
                items.extend(self._extract_orders_from_section(press_release_section, since))
            
            # Recent Orders section
            recent_section = html.css_first('section:contains("Recent Orders")')
            if recent_section:
                items.extend(self._extract_orders_from_section(recent_section, since))
            
            # Search results if any
            search_results = html.css('.slds-card')
            for card in search_results:
                item = self._extract_order_from_card(card)
                if item and self._is_recent_order(item, since):
                    items.append(item)
            
            self.logger.info("Discovered orders", count=len(items))
            return items
            
        except Exception as e:
            self.logger.error("Failed to discover items", error=str(e))
            return []
    
    def _extract_orders_from_section(self, section, since: date) -> List[Dict[str, Any]]:
        """Extract orders from a section of the page."""
        items = []
        
        # Look for order links
        links = section.css('a[href*="/orders/"]')
        for link in links:
            item = self._extract_order_from_link(link)
            if item and self._is_recent_order(item, since):
                items.append(item)
        
        return items
    
    def _extract_order_from_link(self, link) -> Optional[Dict[str, Any]]:
        """Extract order information from a link element."""
        try:
            href = link.attributes.get('href')
            if not href:
                return None
            
            title = link.text().strip()
            if not title:
                return None
            
            # Extract order number from title or URL
            order_number = self._extract_order_number(title, href)
            
            return {
                'id': order_number,
                'title': title,
                'url': urljoin(self.base_url, href),
                'order_number': order_number,
                'discovered_date': datetime.now().date()
            }
        except Exception as e:
            self.logger.error("Failed to extract order from link", error=str(e))
            return None
    
    def _extract_order_from_card(self, card) -> Optional[Dict[str, Any]]:
        """Extract order information from a card element."""
        try:
            # Look for title and link
            title_elem = card.css_first('h3, h4, .title')
            if not title_elem:
                return None
            
            title = title_elem.text().strip()
            
            # Look for link
            link = card.css_first('a[href*="/orders/"]')
            if not link:
                return None
            
            href = link.attributes.get('href')
            if not href:
                return None
            
            order_number = self._extract_order_number(title, href)
            
            return {
                'id': order_number,
                'title': title,
                'url': urljoin(self.base_url, href),
                'order_number': order_number,
                'discovered_date': datetime.now().date()
            }
        except Exception as e:
            self.logger.error("Failed to extract order from card", error=str(e))
            return None
    
    def _extract_order_number(self, title: str, url: str) -> str:
        """Extract order number from title or URL."""
        # Try to extract from URL first
        url_match = re.search(r'/orders/([^/]+)', url)
        if url_match:
            return url_match.group(1)
        
        # Try to extract from title
        title_match = re.search(r'(?:Order|Notice)\s+(?:No\.?\s*)?([A-Z0-9\-]+)', title, re.IGNORECASE)
        if title_match:
            return title_match.group(1)
        
        # Fallback to URL path
        path = urlparse(url).path
        return path.split('/')[-1] or 'unknown'
    
    def _is_recent_order(self, item: Dict[str, Any], since: date) -> bool:
        """Check if order is recent enough to process."""
        # For now, include all discovered orders
        # In a real implementation, you'd check the order date
        return True
    
    async def fetch_item_detail(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Fetch detailed information for a specific order."""
        try:
            url = item['url']
            response = await self._make_request(url)
            
            # Parse the order detail page
            html = HTMLParser(response.text)
            
            # Extract PDF link if available
            pdf_link = html.css_first('a[href*=".pdf"]')
            pdf_url = None
            if pdf_link:
                pdf_url = urljoin(url, pdf_link.attributes.get('href', ''))
            
            # Extract metadata from the page
            metadata = self._extract_metadata_from_page(html)
            
            # Download and parse PDF if available
            pdf_text = ""
            if pdf_url:
                try:
                    pdf_response = await self._make_request(pdf_url)
                    pdf_text = extract_text(io.BytesIO(pdf_response.content))
                except Exception as e:
                    self.logger.warning("Failed to download PDF", url=pdf_url, error=str(e))
            
            return {
                **item,
                'html_content': response.text,
                'pdf_url': pdf_url,
                'pdf_text': pdf_text,
                'metadata': metadata
            }
            
        except Exception as e:
            self.logger.error("Failed to fetch item detail", item_id=item.get('id'), error=str(e))
            return item
    
    def _extract_metadata_from_page(self, html: HTMLParser) -> Dict[str, Any]:
        """Extract metadata from the order detail page."""
        metadata = {}
        
        # Look for common metadata patterns
        # Institution name
        institution_elem = html.css_first('.institution-name, .bank-name, h1, h2')
        if institution_elem:
            metadata['institution'] = institution_elem.text().strip()
        
        # Date
        date_elem = html.css_first('.date, .order-date, time')
        if date_elem:
            date_text = date_elem.text().strip()
            try:
                metadata['date'] = dateparser.parse(date_text).date()
            except:
                pass
        
        # Order type
        type_elem = html.css_first('.order-type, .type')
        if type_elem:
            metadata['order_type'] = type_elem.text().strip()
        
        return metadata
    
    def parse_item(self, item_data: Dict[str, Any]) -> Dict[str, Any]:
        """Parse raw order data into structured format."""
        try:
            parsed = {
                'external_id': item_data.get('order_number', ''),
                'title': item_data.get('title', ''),
                'url': item_data.get('url', ''),
                'pdf_url': item_data.get('pdf_url'),
                'institution': '',
                'order_type': '',
                'event_date': None,
                'penalty_amount': 0,
                'penalty_text': '',
                'docket_number': '',
                'state': '',
                'summary': ''
            }
            
            # Extract from metadata
            metadata = item_data.get('metadata', {})
            parsed['institution'] = metadata.get('institution', '')
            parsed['order_type'] = metadata.get('order_type', '')
            parsed['event_date'] = metadata.get('date')
            
            # Parse PDF text if available
            pdf_text = item_data.get('pdf_text', '')
            if pdf_text:
                pdf_parsed = self._parse_pdf_text(pdf_text)
                parsed.update(pdf_parsed)
            
            # Parse HTML content as fallback
            html_content = item_data.get('html_content', '')
            if html_content and not parsed['institution']:
                html_parsed = self._parse_html_content(html_content)
                parsed.update(html_parsed)
            
            # Generate summary
            parsed['summary'] = self._generate_summary(parsed)
            
            return parsed
            
        except Exception as e:
            self.logger.error("Failed to parse item", item_id=item_data.get('id'), error=str(e))
            return {}
    
    def _parse_pdf_text(self, pdf_text: str) -> Dict[str, Any]:
        """Parse PDF text to extract key information."""
        parsed = {}
        
        # Extract institution name
        institution_match = re.search(r'(?:against|involving|regarding)\s+([A-Z][A-Za-z\s&.,]+?)(?:\s+Bank|\s+National|\s+Federal|\s+State|\.|$)', pdf_text, re.IGNORECASE)
        if institution_match:
            parsed['institution'] = institution_match.group(1).strip()
        
        # Extract penalty amount
        penalty_match = re.search(r'(?:civil\s+)?(?:money|monetary)\s+penalty\s+(?:of\s*)?\$?([\d,]+(?:\.\d{2})?)', pdf_text, re.IGNORECASE)
        if penalty_match:
            penalty_text = penalty_match.group(0)
            penalty_amount = self.extract_money_amount(penalty_text)[0]
            parsed['penalty_amount'] = penalty_amount
            parsed['penalty_text'] = penalty_text
        
        # Extract docket number
        docket_match = re.search(r'(?:docket|case)\s+(?:no\.?\s*)?([A-Z0-9\-]+)', pdf_text, re.IGNORECASE)
        if docket_match:
            parsed['docket_number'] = docket_match.group(1)
        
        # Extract state
        state_match = re.search(r'(?:of|in)\s+([A-Z]{2})\s+(?:Bank|National|Federal)', pdf_text)
        if state_match:
            parsed['state'] = state_match.group(1)
        
        # Extract date
        date_match = re.search(r'(?:effective|issued|dated)\s+(?:date\s+)?([A-Za-z]+\s+\d{1,2},?\s+\d{4})', pdf_text, re.IGNORECASE)
        if date_match:
            try:
                parsed['event_date'] = dateparser.parse(date_match.group(1)).date()
            except:
                pass
        
        return parsed
    
    def _parse_html_content(self, html_content: str) -> Dict[str, Any]:
        """Parse HTML content to extract key information."""
        parsed = {}
        html = HTMLParser(html_content)
        
        # Extract institution name from various selectors
        for selector in ['.institution', '.bank-name', 'h1', 'h2', '.title']:
            elem = html.css_first(selector)
            if elem:
                text = elem.text().strip()
                if text and len(text) > 3:
                    parsed['institution'] = text
                    break
        
        # Extract date
        for selector in ['.date', '.order-date', 'time', '.effective-date']:
            elem = html.css_first(selector)
            if elem:
                try:
                    parsed['event_date'] = dateparser.parse(elem.text().strip()).date()
                    break
                except:
                    continue
        
        return parsed
    
    def _generate_summary(self, parsed: Dict[str, Any]) -> str:
        """Generate a summary of the order."""
        parts = []
        
        if parsed['institution']:
            parts.append(f"FDIC enforcement action against {parsed['institution']}")
        
        if parsed['order_type']:
            parts.append(f"({parsed['order_type']})")
        
        if parsed['penalty_amount'] > 0:
            parts.append(f"with penalty of ${parsed['penalty_amount']:,}")
        
        if parsed['state']:
            parts.append(f"in {parsed['state']}")
        
        summary = " ".join(parts)
        return summary[:480] if summary else "FDIC enforcement action"
    
    def normalize_item(self, parsed_data: Dict[str, Any]) -> Event:
        """Normalize parsed data into Event model."""
        try:
            # Generate event ID
            event_date = parsed_data.get('event_date') or date.today()
            external_id = parsed_data.get('external_id', 'unknown')
            event_id = self.generate_event_id(external_id, event_date)
            
            # Map categories
            order_type = parsed_data.get('order_type', '').lower()
            categories = []
            if any(term in order_type for term in ['consent', 'cease', 'desist']):
                categories.append('regulatory_action')
            if parsed_data.get('penalty_amount', 0) > 0:
                categories.append('fine')
            if not categories:
                categories.append('regulatory_action')
            
            # Map nature
            nature = self.map_nature(order_type)
            
            # Calculate materiality score
            event_data = {
                'amounts': {'penalties_usd': parsed_data.get('penalty_amount', 0)},
                'title': parsed_data.get('title', ''),
                'categories': categories
            }
            materiality_score = self.calculate_materiality_score(event_data)
            
            # Create reputational drivers
            drivers = ReputationalDrivers(
                fine_usd=parsed_data.get('penalty_amount', 0),
                regulator_involved=['FDIC']
            )
            
            # Create reputational damage
            reputational_damage = ReputationalDamage(
                nature=nature,
                materiality_score=materiality_score,
                drivers=drivers
            )
            
            # Create source reference
            source = SourceRef(
                title=parsed_data.get('title', 'FDIC Enforcement Order'),
                publisher='FDIC',
                url=parsed_data.get('url', ''),
                date_published=event_date,
                source_type='regulator'
            )
            
            # Create amounts
            amounts = {
                'penalties_usd': parsed_data.get('penalty_amount', 0),
                'settlements_usd': 0,
                'other_amounts_usd': 0,
                'original_text': parsed_data.get('penalty_text', '')
            }
            
            # Create event
            event = Event(
                event_id=event_id,
                title=parsed_data.get('title', 'FDIC Enforcement Action'),
                institutions=[parsed_data.get('institution', 'Unknown Institution')],
                us_operations=True,
                jurisdictions=['USA', parsed_data.get('state', '')] if parsed_data.get('state') else ['USA'],
                categories=categories,
                event_date=event_date,
                reported_dates=[event_date],
                summary=parsed_data.get('summary', ''),
                reputational_damage=reputational_damage,
                amounts=amounts,
                sources=[source],
                source_count=1,
                confidence='high' if parsed_data.get('pdf_text') else 'medium'
            )
            
            return event
            
        except Exception as e:
            self.logger.error("Failed to normalize item", error=str(e))
            raise
