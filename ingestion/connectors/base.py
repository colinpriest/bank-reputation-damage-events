"""
Base connector interface and abstract base class for data source connectors.
All connectors must implement the Connector protocol.
"""

from abc import ABC, abstractmethod
from datetime import date
from typing import List, Protocol, Optional, Dict, Any
import httpx
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential

from ingestion.normalizers.events_model import Event


class Connector(Protocol):
    """Protocol for data source connectors."""
    source_name: str
    
    async def fetch_updates(self, since: date) -> List[Event]:
        """Fetch updates from the data source since the given date."""
        ...


class BaseConnector(ABC):
    """Abstract base class for data source connectors."""
    
    def __init__(self, source_name: str, timeout: int = 30):
        self.source_name = source_name
        self.timeout = timeout
        self.logger = structlog.get_logger(source_name)
        self._client: Optional[httpx.AsyncClient] = None
    
    @property
    def client(self) -> httpx.AsyncClient:
        """Get or create HTTP client."""
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=self.timeout,
                http2=True,
                verify=False,  # Disable SSL verification for government sites
                follow_redirects=True,  # Follow redirects automatically
                headers={
                    "User-Agent": "bank-reputation-monitor/1.0"
                }
            )
        return self._client
    
    async def close(self):
        """Close the HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    async def _make_request(self, url: str, **kwargs) -> httpx.Response:
        """Make HTTP request with retry logic."""
        try:
            response = await self.client.get(url, **kwargs)
            response.raise_for_status()
            return response
        except httpx.HTTPStatusError as e:
            if e.response.status_code in [429, 500, 502, 503, 504]:
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
        except httpx.SSLError as e:
            self.logger.error("SSL certificate error", error=str(e), url=url)
            raise
        except httpx.ConnectError as e:
            self.logger.error("Connection error", error=str(e), url=url)
            raise
        except Exception as e:
            self.logger.error("Request failed", error=str(e), url=url)
            raise
    
    @abstractmethod
    async def discover_items(self, since: date) -> List[Dict[str, Any]]:
        """Discover items to process from the data source."""
        pass
    
    @abstractmethod
    async def fetch_item_detail(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Fetch detailed information for a specific item."""
        pass
    
    @abstractmethod
    def parse_item(self, item_data: Dict[str, Any]) -> Dict[str, Any]:
        """Parse raw item data into structured format."""
        pass
    
    @abstractmethod
    def normalize_item(self, parsed_data: Dict[str, Any]) -> Event:
        """Normalize parsed data into Event model."""
        pass
    
    async def fetch_updates(self, since: date) -> List[Event]:
        """Main method to fetch updates from the data source."""
        self.logger.info("Starting fetch", since=since.isoformat())
        
        try:
            # Discover items
            items = await self.discover_items(since)
            self.logger.info("Discovered items", count=len(items))
            
            events = []
            for item in items:
                try:
                    # Fetch detail
                    item_detail = await self.fetch_item_detail(item)
                    
                    # Parse
                    parsed_data = self.parse_item(item_detail)
                    
                    # Normalize
                    event = self.normalize_item(parsed_data)
                    events.append(event)
                    
                except Exception as e:
                    self.logger.error(
                        "Failed to process item",
                        item=item.get('id', 'unknown'),
                        error=str(e)
                    )
                    continue
            
            self.logger.info("Fetch completed", events_count=len(events))
            return events
            
        except Exception as e:
            self.logger.error("Fetch failed", error=str(e))
            raise
        finally:
            await self.close()
    
    def generate_event_id(self, external_id: str, event_date: date) -> str:
        """Generate standardized event ID."""
        from urllib.parse import quote
        import re
        
        # Create kebab-case ID
        base = f"{self.source_name}:{external_id}:{event_date.isoformat()}"
        # Replace non-alphanumeric chars with hyphens
        kebab = re.sub(r'[^a-zA-Z0-9]', '-', base.lower())
        # Remove multiple consecutive hyphens
        kebab = re.sub(r'-+', '-', kebab)
        # Remove leading/trailing hyphens
        kebab = kebab.strip('-')
        
        return kebab
    
    def extract_money_amount(self, text: str) -> tuple[int, str]:
        """Extract money amount from text."""
        from ingestion.normalizers.mappings import extract_money_amounts
        return extract_money_amounts(text)
    
    def map_category(self, text: str) -> str:
        """Map text to standardized category."""
        from ingestion.normalizers.mappings import map_category
        return map_category(text)
    
    def map_nature(self, text: str) -> List[str]:
        """Map text to standardized nature types."""
        from ingestion.normalizers.mappings import map_nature
        return map_nature(text)
    
    def map_regulator(self, text: str) -> str:
        """Map text to standardized regulator name."""
        from ingestion.normalizers.mappings import map_regulator
        return map_regulator(text)
    
    def calculate_materiality_score(self, event_data: Dict[str, Any]) -> int:
        """Calculate materiality score for event."""
        from ingestion.normalizers.mappings import calculate_materiality_score
        return calculate_materiality_score(event_data)
