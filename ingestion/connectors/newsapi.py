"""
NewsAPI.org connector for financial news and bank reputation monitoring.
Provides news articles about banks, financial institutions, and regulatory actions.
"""

import os
import json
from datetime import date, datetime, timedelta
from typing import List, Dict, Any, Optional
import httpx
from urllib.parse import quote_plus

from .base import BaseConnector
from ingestion.normalizers.events_model import Event, SourceRef, ReputationalDrivers, ReputationalDamage


class NewsApiConnector(BaseConnector):
    """Connector for NewsAPI.org financial news."""
    
    def __init__(self):
        super().__init__("newsapi")
        self.api_key = os.getenv("NEWS_API_KEY")
        if not self.api_key:
            self.logger.warning("NEWS_API_KEY not found in environment")
        
        self.base_url = "https://newsapi.org/v2"
        
        # Keywords for bank reputation damage events
        self.reputation_keywords = [
            "bank fine", "bank penalty", "regulatory action", "enforcement action",
            "bank scandal", "bank fraud", "money laundering", "compliance failure",
            "bank settlement", "bank lawsuit", "bank investigation", "bank misconduct",
            "FDIC", "OCC", "Federal Reserve", "CFPB", "SEC enforcement",
            "bankruptcy", "bank failure", "financial crisis", "bankruptcy filing"
        ]
        
        # Major US banks to monitor
        self.major_banks = [
            "JPMorgan Chase", "Bank of America", "Wells Fargo", "Citigroup",
            "Goldman Sachs", "Morgan Stanley", "U.S. Bancorp", "PNC Financial",
            "Capital One", "American Express", "Bank of New York Mellon",
            "State Street", "Charles Schwab", "Citizens Financial", "Fifth Third"
        ]
    
    async def discover_items(self, since: date) -> List[Dict[str, Any]]:
        """Discover news articles related to bank reputation events."""
        if not self.api_key:
            self.logger.warning("Cannot search without NEWS_API_KEY")
            return []
        
        try:
            articles = []
            
            # Search for each major bank with reputation keywords
            for bank in self.major_banks:
                bank_articles = await self._search_bank_news(bank, since)
                articles.extend(bank_articles)
            
            # Search for general financial regulatory news
            regulatory_articles = await self._search_regulatory_news(since)
            articles.extend(regulatory_articles)
            
            # Remove duplicates based on URL
            seen_urls = set()
            unique_articles = []
            for article in articles:
                if article.get('url') not in seen_urls:
                    seen_urls.add(article.get('url'))
                    unique_articles.append(article)
            
            self.logger.info(f"Discovered {len(unique_articles)} unique news articles")
            return unique_articles
            
        except Exception as e:
            self.logger.error("Failed to discover news articles", error=str(e))
            return []
    
    async def _search_bank_news(self, bank_name: str, since: date) -> List[Dict[str, Any]]:
        """Search for news about a specific bank."""
        try:
            # Create search query combining bank name with reputation keywords
            query_parts = [bank_name]
            query_parts.extend(self.reputation_keywords[:5])  # Use first 5 keywords
            
            query = " OR ".join([f'"{part}"' for part in query_parts])
            
            params = {
                'q': query,
                'from': since.isoformat(),
                'to': date.today().isoformat(),
                'language': 'en',
                'sortBy': 'relevancy',
                'pageSize': 20,
                'apiKey': self.api_key
            }
            
            response = await self._make_request(
                f"{self.base_url}/everything",
                params=params
            )
            
            data = response.json()
            articles = data.get('articles', [])
            
            # Add bank name to each article for tracking
            for article in articles:
                article['bank_name'] = bank_name
                article['search_query'] = query
            
            self.logger.info(f"Found {len(articles)} articles for {bank_name}")
            return articles
            
        except Exception as e:
            self.logger.error(f"Failed to search news for {bank_name}", error=str(e))
            return []
    
    async def _search_regulatory_news(self, since: date) -> List[Dict[str, Any]]:
        """Search for general financial regulatory news."""
        try:
            regulatory_keywords = [
                "banking regulation", "financial regulation", "bank enforcement",
                "FDIC enforcement", "OCC enforcement", "Federal Reserve enforcement",
                "banking compliance", "financial penalties", "regulatory fines"
            ]
            
            query = " OR ".join([f'"{keyword}"' for keyword in regulatory_keywords])
            
            params = {
                'q': query,
                'from': since.isoformat(),
                'to': date.today().isoformat(),
                'language': 'en',
                'sortBy': 'relevancy',
                'pageSize': 30,
                'apiKey': self.api_key
            }
            
            response = await self._make_request(
                f"{self.base_url}/everything",
                params=params
            )
            
            data = response.json()
            articles = data.get('articles', [])
            
            # Mark as regulatory news
            for article in articles:
                article['bank_name'] = 'regulatory'
                article['search_query'] = query
            
            self.logger.info(f"Found {len(articles)} regulatory news articles")
            return articles
            
        except Exception as e:
            self.logger.error("Failed to search regulatory news", error=str(e))
            return []
    
    async def fetch_item_detail(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """NewsAPI articles come with full content, so just return the item."""
        return item
    
    def parse_item(self, item_data: Dict[str, Any]) -> Dict[str, Any]:
        """Parse news article data into structured format."""
        try:
            # Extract publication date
            published_at = item_data.get('publishedAt')
            if published_at:
                try:
                    # Parse ISO format date
                    pub_date = datetime.fromisoformat(published_at.replace('Z', '+00:00'))
                    event_date = pub_date.date()
                except:
                    event_date = date.today()
            else:
                event_date = date.today()
            
            # Extract institutions mentioned
            institutions = []
            bank_name = item_data.get('bank_name')
            if bank_name and bank_name != 'regulatory':
                institutions.append(bank_name)
            
            # Try to extract other bank names from title and description
            title = item_data.get('title', '')
            description = item_data.get('description', '')
            content = f"{title} {description}"
            
            for bank in self.major_banks:
                if bank.lower() in content.lower():
                    if bank not in institutions:
                        institutions.append(bank)
            
            # Determine categories based on content
            categories = []
            content_lower = content.lower()
            
            if any(word in content_lower for word in ['fine', 'penalty', 'settlement']):
                categories.append('fine')
            if any(word in content_lower for word in ['enforcement', 'regulatory', 'compliance']):
                categories.append('regulatory_action')
            if any(word in content_lower for word in ['fraud', 'scandal', 'misconduct']):
                categories.append('misconduct')
            if any(word in content_lower for word in ['lawsuit', 'litigation', 'legal']):
                categories.append('litigation')
            if any(word in content_lower for word in ['bankruptcy', 'failure']):
                categories.append('financial_distress')
            
            # Default category if none found
            if not categories:
                categories.append('news')
            
            # Determine regulators mentioned
            regulators = []
            if any(reg in content_lower for reg in ['fdic', 'federal deposit insurance']):
                regulators.append('FDIC')
            if any(reg in content_lower for reg in ['occ', 'office of the comptroller']):
                regulators.append('OCC')
            if any(reg in content_lower for reg in ['federal reserve', 'fed']):
                regulators.append('Federal Reserve')
            if any(reg in content_lower for reg in ['cfpb', 'consumer financial protection']):
                regulators.append('CFPB')
            if any(reg in content_lower for reg in ['sec', 'securities and exchange']):
                regulators.append('SEC')
            
            # Calculate materiality score based on source and content
            materiality_score = self._calculate_materiality_score(item_data)
            
            return {
                'title': item_data.get('title', 'Unknown Title'),
                'description': item_data.get('description', ''),
                'content': item_data.get('content', ''),
                'url': item_data.get('url', ''),
                'source': item_data.get('source', {}).get('name', 'Unknown'),
                'published_at': published_at,
                'event_date': event_date,
                'institutions': institutions,
                'categories': categories,
                'regulators': regulators,
                'materiality_score': materiality_score,
                'search_query': item_data.get('search_query', ''),
                'bank_name': item_data.get('bank_name', '')
            }
            
        except Exception as e:
            self.logger.error("Failed to parse news article", error=str(e), article=item_data.get('title'))
            return {}
    
    def _calculate_materiality_score(self, article: Dict[str, Any]) -> int:
        """Calculate materiality score for news article (1-5 scale)."""
        score = 1
        content = f"{article.get('title', '')} {article.get('description', '')}".lower()
        
        # High-impact keywords
        high_impact = ['fine', 'penalty', 'settlement', 'enforcement', 'fraud', 'scandal']
        if any(word in content for word in high_impact):
            score += 2
        
        # Major banks mentioned
        major_banks = ['jpmorgan', 'bank of america', 'wells fargo', 'citigroup', 'goldman sachs']
        if any(bank in content for bank in major_banks):
            score += 1
        
        # Regulatory agencies mentioned
        regulators = ['fdic', 'occ', 'federal reserve', 'cfpb', 'sec']
        if any(reg in content for reg in regulators):
            score += 1
        
        # Reputable news sources
        reputable_sources = ['reuters', 'bloomberg', 'wall street journal', 'financial times', 'cnbc']
        source_name = article.get('source', {}).get('name', '').lower()
        if any(source in source_name for source in reputable_sources):
            score += 1
        
        return min(score, 5)  # Cap at 5
    
    def normalize_item(self, parsed_data: Dict[str, Any]) -> Event:
        """Normalize parsed news data into Event model."""
        try:
            # Create source reference
            source_ref = SourceRef(
                source_id=parsed_data.get('url', ''),
                source_type='news_article',
                source_name=parsed_data.get('source', 'Unknown'),
                url=parsed_data.get('url', ''),
                accessed_at=datetime.now()
            )
            
            # Create reputational drivers
            drivers = ReputationalDrivers(
                regulatory_action='regulatory_action' in parsed_data.get('categories', []),
                financial_penalty='fine' in parsed_data.get('categories', []),
                legal_action='litigation' in parsed_data.get('categories', []),
                operational_risk='misconduct' in parsed_data.get('categories', []),
                market_impact=False,  # Would need market data to determine
                customer_impact=False  # Would need customer data to determine
            )
            
            # Create reputational damage assessment
            damage = ReputationalDamage(
                materiality_score=parsed_data.get('materiality_score', 1),
                severity_level='medium' if parsed_data.get('materiality_score', 1) >= 3 else 'low',
                impact_scope='institution' if parsed_data.get('institutions') else 'industry',
                recovery_timeline='unknown'
            )
            
            # Create event ID
            event_id = f"newsapi-{parsed_data.get('source', 'unknown')}-{parsed_data.get('event_date')}-{hash(parsed_data.get('url', '')) % 10000}"
            
            return Event(
                event_id=event_id,
                title=parsed_data.get('title', 'Unknown Title'),
                description=parsed_data.get('description', ''),
                event_date=parsed_data.get('event_date', date.today()),
                institutions=parsed_data.get('institutions', []),
                categories=parsed_data.get('categories', ['news']),
                regulators=parsed_data.get('regulators', []),
                amounts={},  # News articles don't typically have monetary amounts
                source_refs=[source_ref],
                reputational_drivers=drivers,
                reputational_damage=damage,
                metadata={
                    'news_source': parsed_data.get('source', 'Unknown'),
                    'search_query': parsed_data.get('search_query', ''),
                    'bank_name': parsed_data.get('bank_name', ''),
                    'published_at': parsed_data.get('published_at', '')
                }
            )
            
        except Exception as e:
            self.logger.error("Failed to normalize news article", error=str(e))
            raise


