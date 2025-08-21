"""
OCC Enforcement Actions & EASearch connector.
Scrapes monthly releases and searchable enforcement database for actions since 2012.
"""

import re
import json
from datetime import date, datetime, timedelta
from typing import List, Dict, Any, Optional
from urllib.parse import urljoin, urlparse
import dateparser
import httpx
from selectolax.parser import HTMLParser

from .base import BaseConnector
from ingestion.normalizers.events_model import Event, SourceRef, ReputationalDrivers, ReputationalDamage


class OccEnforcementConnector(BaseConnector):
    """Connector for OCC Enforcement Actions & EASearch."""
    
    def __init__(self):
        super().__init__("occ_enforcement")
        self.base_url = "https://www.occ.gov"
        self.news_base = f"{self.base_url}/news-issuances/news-releases"
        # Updated EASearch URL to current structure
        self.easearch_url = "https://www.occ.gov/topics/laws-regulations/enforcement-actions/index-enforcement-actions.html"
    
    async def discover_items(self, since: date) -> List[Dict[str, Any]]:
        """Discover enforcement actions from OCC monthly news releases."""
        try:
            items = []
            
            # Get current year and month
            current_year = datetime.now().year
            current_month = datetime.now().month
            
            # Check last 6 months for news releases, but avoid current month
            # since news releases for current month may not be published yet
            for i in range(1, 7):  # Start from 1 to skip current month
                check_date = date(current_year, current_month, 1) - timedelta(days=30*i)
                year = check_date.year
                month = check_date.month
                
                # Try to get monthly news release
                news_url = f"{self.news_base}/{year}/nr-occ-{year}-{month:02d}.html"
                
                try:
                    # Use a custom request method that doesn't retry on 404s
                    response = await self._make_request_no_retry_404(news_url)
                    html = HTMLParser(response.text)
                    
                    # Look for enforcement action links
                    enforcement_links = html.css('a[href*="EASearch"]')
                    for link in enforcement_links:
                        item = self._extract_enforcement_from_link(link, year, month)
                        if item and self._is_recent_action(item, since):
                            items.append(item)
                    
                    self.logger.info(f"Found {len(enforcement_links)} enforcement links in {year}-{month:02d}")
                    
                except Exception as e:
                    self.logger.debug(f"No news release found for {year}-{month:02d}: {e}")
                    continue
            
            # Also check EASearch for recent actions
            easearch_items = await self._discover_from_easearch(since)
            items.extend(easearch_items)
            
            self.logger.info("Discovered enforcement actions", count=len(items))
            return items
            
        except Exception as e:
            self.logger.error("Failed to discover items", error=str(e))
            return []
    
    def _extract_enforcement_from_link(self, link, year: int, month: int) -> Optional[Dict[str, Any]]:
        """Extract enforcement action information from a news release link."""
        try:
            href = link.attributes.get('href')
            if not href:
                return None
            
            title = link.text().strip()
            if not title:
                return None
            
            # Extract EA number from title or URL
            ea_number = self._extract_ea_number(title, href)
            
            return {
                'id': ea_number,
                'title': title,
                'url': urljoin(self.base_url, href),
                'ea_number': ea_number,
                'year': year,
                'month': month,
                'discovered_date': datetime.now().date()
            }
        except Exception as e:
            self.logger.error("Failed to extract enforcement from link", error=str(e))
            return None
    
    def _extract_ea_number(self, title: str, url: str) -> str:
        """Extract EA number from title or URL."""
        # Try to extract from URL first
        url_match = re.search(r'EA-ENF-(\d{4}-\d+)', url)
        if url_match:
            return f"EA-ENF-{url_match.group(1)}"
        
        # Try to extract from title
        title_match = re.search(r'(?:EA|Enforcement Action)\s*(?:No\.?\s*)?([A-Z0-9\-]+)', title, re.IGNORECASE)
        if title_match:
            return title_match.group(1)
        
        # Fallback to URL path
        path = urlparse(url).path
        return path.split('/')[-1] or 'unknown'
    
    def _is_recent_action(self, item: Dict[str, Any], since: date) -> bool:
        """Check if enforcement action is recent enough to process."""
        # For now, include all discovered actions
        # In a real implementation, you'd check the action date
        return True
    
    async def _make_request_no_retry_404(self, url: str, **kwargs) -> httpx.Response:
        """Make HTTP request without retrying on 404 errors."""
        try:
            response = await self.client.get(url, **kwargs)
            response.raise_for_status()
            return response
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                # Don't retry 404s, just raise the exception
                raise
            elif e.response.status_code in [429, 500, 502, 503, 504]:
                self.logger.warning(
                    "HTTP error, retrying",
                    status_code=e.response.status_code,
                    url=url
                )
                raise  # Retry
            else:
                self.logger.error(
                    "HTTP error",
                    status_code=e.response.status_code,
                    url=url
                )
                raise
        except Exception as e:
            self.logger.error("Request failed", error=str(e), url=url)
            raise
    
    async def _discover_from_easearch(self, since: date) -> List[Dict[str, Any]]:
        """Discover enforcement actions from EASearch interface."""
        try:
            # Get the main enforcement actions page
            response = await self._make_request_no_retry_404(self.easearch_url)
            html = HTMLParser(response.text)
            
            items = []
            
            # Look for enforcement action links, but be more selective
            action_links = html.css('a[href*="enforcement"]')
            
            for link in action_links:
                href = link.attributes.get('href', '')
                
                # Skip problematic URLs and external sites
                if any(skip_url in href for skip_url in [
                    'enforcement-actions-types.html',
                    'index-enforcement-actions.html',
                    'sec.gov',  # Skip external SEC links
                    'federalreserve.gov',  # Skip external FRB links
                    'fdic.gov'  # Skip external FDIC links
                ]):
                    continue
                
                # Only process OCC-specific enforcement actions
                if not href.startswith('http') or 'occ.gov' in href:
                    item = self._extract_action_from_easearch_link(link)
                    if item and self._is_recent_action(item, since):
                        items.append(item)
            
            self.logger.info(f"Found {len(items)} enforcement actions from EASearch")
            return items
            
        except Exception as e:
            self.logger.error("Failed to discover from EASearch", error=str(e))
            return []
    
    def _extract_action_from_easearch_link(self, link) -> Optional[Dict[str, Any]]:
        """Extract enforcement action from EASearch link."""
        try:
            href = link.attributes.get('href')
            if not href:
                return None
            
            title = link.text().strip()
            if not title:
                return None
            
            # Extract EA number
            ea_number = self._extract_ea_number(title, href)
            
            return {
                'id': ea_number,
                'title': title,
                'url': urljoin(self.easearch_url, href),
                'ea_number': ea_number,
                'discovered_date': datetime.now().date()
            }
        except Exception as e:
            self.logger.error("Failed to extract action from EASearch link", error=str(e))
            return None
    
    async def fetch_item_detail(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Fetch detailed information for a specific enforcement action."""
        try:
            url = item['url']
            response = await self._make_request(url)
            
            # Parse the enforcement action detail page
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
                    from pdfminer.high_level import extract_text
                    import io
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
        """Extract metadata from the enforcement action detail page."""
        metadata = {}
        
        # Look for common metadata patterns
        # Institution name
        institution_elem = html.css_first('.institution-name, .bank-name, h1, h2, .title')
        if institution_elem:
            metadata['institution'] = institution_elem.text().strip()
        
        # Date
        date_elem = html.css_first('.date, .action-date, time, .effective-date')
        if date_elem:
            date_text = date_elem.text().strip()
            try:
                metadata['date'] = dateparser.parse(date_text).date()
            except:
                pass
        
        # Action type
        type_elem = html.css_first('.action-type, .type, .enforcement-type')
        if type_elem:
            metadata['action_type'] = type_elem.text().strip()
        
        # Subject matter
        subject_elem = html.css_first('.subject-matter, .subject, .matter')
        if subject_elem:
            metadata['subject_matter'] = subject_elem.text().strip()
        
        return metadata
    
    def parse_item(self, item_data: Dict[str, Any]) -> Dict[str, Any]:
        """Parse raw enforcement action data into structured format."""
        try:
            parsed = {
                'external_id': item_data.get('ea_number', ''),
                'title': item_data.get('title', ''),
                'url': item_data.get('url', ''),
                'pdf_url': item_data.get('pdf_url'),
                'institution': '',
                'action_type': '',
                'subject_matter': '',
                'event_date': None,
                'penalty_amount': 0,
                'penalty_text': '',
                'summary': ''
            }
            
            # Extract from metadata
            metadata = item_data.get('metadata', {})
            parsed['institution'] = metadata.get('institution', '')
            parsed['action_type'] = metadata.get('action_type', '')
            parsed['subject_matter'] = metadata.get('subject_matter', '')
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
        
        # Extract action type
        action_type_match = re.search(r'(?:consent\s+order|cease\s+and\s+desist|formal\s+agreement|written\s+agreement)', pdf_text, re.IGNORECASE)
        if action_type_match:
            parsed['action_type'] = action_type_match.group(0)
        
        # Extract subject matter
        subject_matter_patterns = [
            r'(?:BSA|Bank Secrecy Act|AML|Anti-Money Laundering)',
            r'(?:Fair Lending|Equal Credit Opportunity)',
            r'(?:Community Reinvestment Act|CRA)',
            r'(?:Truth in Lending|TILA)',
            r'(?:Real Estate Settlement Procedures|RESPA)'
        ]
        
        for pattern in subject_matter_patterns:
            match = re.search(pattern, pdf_text, re.IGNORECASE)
            if match:
                parsed['subject_matter'] = match.group(0)
                break
        
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
        for selector in ['.date', '.action-date', 'time', '.effective-date']:
            elem = html.css_first(selector)
            if elem:
                try:
                    parsed['event_date'] = dateparser.parse(elem.text().strip()).date()
                    break
                except:
                    continue
        
        return parsed
    
    def _generate_summary(self, parsed: Dict[str, Any]) -> str:
        """Generate a summary of the enforcement action."""
        parts = []
        
        if parsed['institution']:
            parts.append(f"OCC enforcement action against {parsed['institution']}")
        
        if parsed['action_type']:
            parts.append(f"({parsed['action_type']})")
        
        if parsed['subject_matter']:
            parts.append(f"regarding {parsed['subject_matter']}")
        
        if parsed['penalty_amount'] > 0:
            parts.append(f"with penalty of ${parsed['penalty_amount']:,}")
        
        summary = " ".join(parts)
        return summary[:480] if summary else "OCC enforcement action"
    
    def normalize_item(self, parsed_data: Dict[str, Any]) -> Event:
        """Normalize parsed data into Event model."""
        try:
            # Generate event ID
            event_date = parsed_data.get('event_date') or date.today()
            external_id = parsed_data.get('external_id', 'unknown')
            event_id = self.generate_event_id(external_id, event_date)
            
            # Map categories
            action_type = parsed_data.get('action_type', '').lower()
            categories = []
            if any(term in action_type for term in ['consent', 'cease', 'desist']):
                categories.append('regulatory_action')
            if parsed_data.get('penalty_amount', 0) > 0:
                categories.append('fine')
            if not categories:
                categories.append('regulatory_action')
            
            # Map nature based on subject matter
            subject_matter = parsed_data.get('subject_matter', '').lower()
            nature = []
            if any(term in subject_matter for term in ['bsa', 'aml', 'anti-money laundering']):
                nature.append('sanctions_aml')
            if any(term in subject_matter for term in ['fair lending', 'equal credit']):
                nature.append('fairness_discrimination')
            if not nature:
                nature = self.map_nature(action_type)
            
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
                regulator_involved=['OCC']
            )
            
            # Create reputational damage
            reputational_damage = ReputationalDamage(
                nature=nature,
                materiality_score=materiality_score,
                drivers=drivers
            )
            
            # Create source reference
            source = SourceRef(
                title=parsed_data.get('title', 'OCC Enforcement Action'),
                publisher='OCC',
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
                title=parsed_data.get('title', 'OCC Enforcement Action'),
                institutions=[parsed_data.get('institution', 'Unknown Institution')],
                us_operations=True,
                jurisdictions=['USA'],
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
