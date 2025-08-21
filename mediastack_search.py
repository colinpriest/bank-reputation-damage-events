#!/usr/bin/env python3
"""
MediaStack Search and News Article Analysis Classes
"""

from __future__ import annotations

import asyncio
import os
import json
import httpx
import requests
from datetime import date, datetime
from typing import List, Optional, Dict, Any, Literal, Annotated
from dataclasses import dataclass
from dotenv import load_dotenv
from playwright.async_api import async_playwright
from openai import OpenAI
import tempfile
import time
from pydantic import BaseModel, Field, ValidationInfo, field_validator, ConfigDict, AwareDatetime
from enum import Enum

# Load environment variables
load_dotenv()

@dataclass
class NewsArticle:
    """Represents a news article from MediaStack API"""
    title: str
    description: str
    content: str
    url: str
    source: str
    published_at: str
    country: str
    language: str
    category: str
    scraped_text: Optional[str] = None
    pdf_path: Optional[str] = None
    analysis: Optional[Dict[str, Any]] = None
    
    @classmethod
    def from_mediastack_json(cls, article_data: Dict[str, Any]) -> 'NewsArticle':
        """Create NewsArticle from MediaStack API JSON response"""
        return cls(
            title=article_data.get('title', ''),
            description=article_data.get('description', ''),
            content=article_data.get('content', ''),
            url=article_data.get('url', ''),
            source=article_data.get('source', ''),
            published_at=article_data.get('published_at', ''),
            country=article_data.get('country', ''),
            language=article_data.get('language', ''),
            category=article_data.get('category', '')
        )
    
    async def scrape(self) -> bool:
        """Scrape the article URL to get text content and create PDF using Playwright"""
        try:
            print(f"🌐 Scraping with Playwright: {self.url}")
            
            async with async_playwright() as p:
                # Launch browser with realistic settings
                browser = await p.chromium.launch(
                    headless=True,
                    args=[
                        '--no-sandbox',
                        '--disable-blink-features=AutomationControlled',
                        '--disable-dev-shm-usage',
                        '--disable-extensions',
                        '--disable-web-security',
                        '--disable-features=VizDisplayCompositor'
                    ]
                )
                
                # Create a new page with realistic user agent and viewport
                page = await browser.new_page(
                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
                )
                
                # Set viewport for realistic browsing
                await page.set_viewport_size({"width": 1920, "height": 1080})
                
                # Set extra HTTP headers to appear more like a real browser
                await page.set_extra_http_headers({
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                    'Sec-Fetch-Site': 'none',
                    'Sec-Fetch-Mode': 'navigate',
                    'Sec-Fetch-User': '?1',
                    'Sec-Fetch-Dest': 'document',
                    'Cache-Control': 'max-age=0'
                })
                
                try:
                    # Navigate to the URL with multiple fallback strategies
                    print(f"📄 Loading page: {self.url}")
                    await page.goto(
                        self.url, 
                        wait_until="domcontentloaded", 
                        timeout=45000
                    )
                    
                    # Wait for potential dynamic content
                    await page.wait_for_timeout(3000)
                    
                    # Try to wait for network to be idle (best case)
                    try:
                        await page.wait_for_load_state("networkidle", timeout=10000)
                    except:
                        print("⚠️ Network didn't become idle, but continuing...")
                    
                    # Extract text content using multiple strategies
                    text_content = ""
                    
                    # Strategy 1: Try to find main content areas
                    content_selectors = [
                        'article',
                        'main', 
                        '.article-content',
                        '.post-content',
                        '.entry-content',
                        '.story-content',
                        '.article-body',
                        '.post-body',
                        '.content',
                        '[class*="content"]',
                        '[class*="article"]',
                        '[class*="story"]'
                    ]
                    
                    for selector in content_selectors:
                        try:
                            element = await page.query_selector(selector)
                            if element:
                                text_content = await element.inner_text()
                                if len(text_content.strip()) > 200:  # Good content found
                                    print(f"✅ Found content using selector: {selector}")
                                    break
                        except:
                            continue
                    
                    # Strategy 2: If no main content found, get all text from body
                    if len(text_content.strip()) < 200:
                        try:
                            body = await page.query_selector('body')
                            if body:
                                text_content = await body.inner_text()
                                print("✅ Extracted content from body")
                        except:
                            pass
                    
                    # Strategy 3: Final fallback - get all visible text
                    if len(text_content.strip()) < 200:
                        try:
                            text_content = await page.evaluate('''
                                () => {
                                    // Remove script and style elements
                                    const scripts = document.querySelectorAll('script, style, nav, header, footer, aside, .nav, .header, .footer, .sidebar');
                                    scripts.forEach(el => el.remove());
                                    
                                    // Get all text content
                                    return document.body.innerText || document.body.textContent || '';
                                }
                            ''')
                            print("✅ Extracted content using JavaScript")
                        except:
                            pass
                    
                    # Clean up the extracted text
                    if text_content:
                        # Remove excessive whitespace and clean up
                        lines = [line.strip() for line in text_content.split('\n') if line.strip()]
                        text_content = '\n'.join(lines)
                        
                        # Store the scraped text
                        self.scraped_text = text_content[:10000]  # Limit to 10k chars
                        print(f"✅ Successfully scraped {len(text_content)} characters of content")
                        
                        # Generate PDF for record keeping
                        try:
                            with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp_file:
                                self.pdf_path = tmp_file.name
                            
                            await page.pdf(
                                path=self.pdf_path,
                                format="A4",
                                print_background=True,
                                margin={"top": "1cm", "right": "1cm", "bottom": "1cm", "left": "1cm"}
                            )
                            print(f"✅ PDF created: {self.pdf_path}")
                        except Exception as e:
                            print(f"⚠️ PDF creation failed: {e}")
                            self.pdf_path = None
                        
                        await browser.close()
                        return True
                    else:
                        print("❌ No content could be extracted from the page")
                        await browser.close()
                        return False
                        
                except Exception as nav_error:
                    print(f"❌ Navigation failed: {nav_error}")
                    await browser.close()
                    return False
                    
        except Exception as e:
            print(f"❌ Playwright scraping failed: {e}")
            return False
        
        return False
    
    async def analyse(self, banking_entity: str, specific_event: Optional[str] = None) -> Dict[str, Any]:
        """Analyze the article using ChatGPT-4o-mini"""
        try:
            # Check if we have the required data
            if not self.scraped_text and not self.content:
                return {"error": "No content available for analysis"}
            
            # Prepare content for analysis - use scraped text if available, otherwise use content
            content_to_analyze = self.scraped_text if self.scraped_text else self.content
            
            # Ensure we have meaningful content
            if not content_to_analyze or len(content_to_analyze.strip()) < 50:
                return {"error": "Content too short for meaningful analysis"}
            
            # Limit content length for API token limits
            if len(content_to_analyze) > 8000:  # Increased limit for better analysis
                content_to_analyze = content_to_analyze[:8000]
            
            # Prepare the analysis prompt
            event_check = ""
            if specific_event:
                event_check = f'Also check if this article is specifically about the event: "{specific_event}"'
            
            # Build the JSON schema dynamically based on whether specific_event is provided
            json_schema_lines = [
                '    "about_banking_entity": "Yes/No",'
            ]
            
            if specific_event:
                json_schema_lines.append('    "about_specific_event": "Yes/No",')
            
            json_schema_lines.extend([
                '    "sentiment_wording": -1 to 1,',
                '    "sentiment_facts": -1 to 1,',
                '    "article_summary": "One paragraph summary of the events described in the article",',
                '    "category": "regulatory/legal|operational|conduct/ethics|esg/social|customer/market|financial_performance|brand_marketing|third_party|governance",',
                '    "affected_stakeholders": ["..."],',
                '    "reputation_management_steps": {',
                '        "public_relations_communication": [],',
                '        "customer_remediation": [],',
                '        "regulatory_legal_engagement": [],',
                '        "governance_accountability": [],',
                '        "operational_control_enhancements": [],',
                '        "stakeholder_engagement": [],',
                '        "monitoring_feedback_loops": [],',
                '        "good_deeds": []',
                '    }',
                '}'
            ])
            
            json_schema = '\n'.join(json_schema_lines)
            
            prompt = f"""
             Analyze this news article:
 
             Title: {self.title}
             Description: {self.description}
             Source: {self.source}
             Published: {self.published_at}
 
             Article Content:
             {content_to_analyze}
 
             ROLE & GOAL
             You are a US banking industry expert analyzing news articles about US banking entities.
 
             TASKS
             1) Identify whether the article is about the US banking entity named {banking_entity}.
             2) {event_check if specific_event else "Skip this step"}
             3) Assess sentiment:
                a) sentiment_wording: Tone of writing (negative to positive rhetoric).
                b) sentiment_facts: Nature of the facts reported (negative to positive event).
             4) Write a one paragraph summary of the events described in the article, including the date of the event, the location of the event, and the nature of the event, and the impact of the event on the banking entity and its stakeholders.
             5) Categorize the event (choose one: regulatory/legal, operational, conduct/ethics, esg/social, customer/market, financial_performance, brand_marketing, third_party, governance).
             6) List the affected stakeholders (from: shareholders, management, employees, regulators, customers, business_partners, financial_system, communities, economy, natural_environment).
             7) Extract explicitly mentioned reputation management steps, grouped into:
                * public_relations_communication
                * customer_remediation
                * regulatory_legal_engagement
                * governance_accountability
                * operational_control_enhancements
                * stakeholder_engagement
                * monitoring_feedback_loops
                * good_deeds
 
             OUTPUT RULES
             - Only include information explicitly mentioned in the article.
             - If no data for a field, output "" or [].
             - Use numeric values between -1 and 1 for sentiment (decimals allowed).
             - Include "about_specific_event" only if a specific event was provided.
             - Output valid JSON only, no extra text.
 
             JSON schema:
             {{
             {json_schema}
             }}
             """
            
            # Get OpenAI API key
            openai_api_key = os.getenv("OPENAI_API_KEY")
            if not openai_api_key:
                return {"error": "OpenAI API key not found"}
            
            # Initialize OpenAI client
            from openai import OpenAI
            client = OpenAI(api_key=openai_api_key)
            
            # Send to ChatGPT-4o-mini with text-only messages
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {
                        "role": "system", 
                        "content": "You are a financial news analyst specializing in bank reputation risk assessment. Always respond with valid JSON only, no additional text."
                    },
                    {
                        "role": "user", 
                        "content": prompt
                    }
                ],
                temperature=0.1,
                max_tokens=2000,
                response_format={"type": "json_object"}  # Force JSON response
            )
            
            # Parse the response
            analysis_text = response.choices[0].message.content
            
            # Try to parse JSON from the response
            try:
                self.analysis = json.loads(analysis_text)
                
                # Clean up the analysis if specific_event wasn't provided
                if not specific_event and "about_specific_event" in self.analysis:
                    del self.analysis["about_specific_event"]
                
                print(f"✅ Analysis completed for: {self.title[:50]}...")
                return self.analysis
                
            except json.JSONDecodeError as e:
                print(f"❌ JSON parsing failed: {e}")
                print(f"Raw response: {analysis_text[:500]}...")
                return {"error": f"JSON parsing failed: {e}", "raw_response": analysis_text}
                
        except Exception as e:
            print(f"❌ Analysis failed: {e}")
            return {"error": f"Analysis failed: {str(e)}"}

class MediaStackSearch:
    """Class for searching MediaStack API and managing results"""
    
    def __init__(self):
        self.api_key = os.getenv("MEDIASTACK_API_KEY")
        if not self.api_key:
            raise ValueError("MEDIASTACK_API_KEY not found in environment")
        
        self.base_url = "https://api.mediastack.com/v1/news"
        self.articles: List[NewsArticle] = []
    
    async def search(self, 
                    search_phrase: str, 
                    start_date: date, 
                    end_date: date, 
                    exclude_keywords: Optional[List[str]] = None) -> bool:
        """
        Search MediaStack API for articles matching criteria
        
        Args:
            search_phrase: The banking entity to search for
            start_date: Start date for search
            end_date: End date for search
            exclude_keywords: Optional list of keywords to filter out
            
        Returns:
            bool: True if search completed successfully
        """
        try:
            # Clear previous results
            self.articles = []
            
            # Set up search parameters
            params = {
                'access_key': self.api_key,
                'keywords': search_phrase,
                'date': f"{start_date.strftime('%Y-%m-%d')},{end_date.strftime('%Y-%m-%d')}",
                'languages': 'en',
                'countries': 'us',
                'categories': 'business',
                'limit': 100,
                'offset': 0
            }
            
            print(f"🔍 Searching for '{search_phrase}' from {start_date} to {end_date}")
            
            # Collect all results with pagination
            offset = 0
            page = 1
            
            async with httpx.AsyncClient() as client:
                while True:
                    params['offset'] = offset
                    
                    print(f"📄 Fetching page {page} (offset: {offset})...")
                    
                    response = await client.get(
                        self.base_url,
                        params=params,
                        timeout=30.0
                    )
                    
                    if response.status_code == 200:
                        data = response.json()
                        articles_data = data.get('data', [])
                        
                        if not articles_data:
                            print(f"✅ No more articles found on page {page}")
                            break
                        
                        # Filter out articles with exclude keywords
                        if exclude_keywords:
                            filtered_articles = []
                            for article in articles_data:
                                title_lower = article.get('title', '').lower()
                                desc_lower = article.get('description', '').lower()
                                content_lower = article.get('content', '').lower()
                                
                                # Check if any exclude keyword is present
                                should_exclude = any(
                                    keyword.lower() in title_lower or 
                                    keyword.lower() in desc_lower or 
                                    keyword.lower() in content_lower
                                    for keyword in exclude_keywords
                                )
                                
                                if not should_exclude:
                                    filtered_articles.append(article)
                            
                            articles_data = filtered_articles
                        
                        # Convert to NewsArticle objects
                        for article_data in articles_data:
                            news_article = NewsArticle.from_mediastack_json(article_data)
                            self.articles.append(news_article)
                        
                        print(f"✅ Found {len(articles_data)} articles on page {page}")
                        
                        # Check if we got fewer articles than the limit (indicating last page)
                        if len(articles_data) < params['limit']:
                            print(f"✅ Reached last page (got {len(articles_data)} articles, limit was {params['limit']})")
                            break
                        
                        # Move to next page
                        offset += params['limit']
                        page += 1
                        
                        # Add a small delay to be respectful to the API
                        await asyncio.sleep(1)
                    else:
                        print(f"❌ Error response: {response.status_code}")
                        return False
            
            print(f"📊 Total articles found: {len(self.articles)}")
            return True
            
        except Exception as e:
            print(f"❌ Search failed: {e}")
            return False
    
    def article_count(self) -> int:
        """Return the number of articles found and stored in memory"""
        return len(self.articles)
    
    def get_article(self, index: int) -> Optional[NewsArticle]:
        """Return the NewsArticle object at the specified index"""
        if 0 <= index < len(self.articles):
            return self.articles[index]
        return None
    
    async def scrape_all_articles(self) -> Dict[str, int]:
        """Scrape all articles in the collection"""
        results = {"success": 0, "failed": 0}
        
        print(f"🔄 Scraping {len(self.articles)} articles...")
        
        for i, article in enumerate(self.articles):
            print(f"   Scraping article {i+1}/{len(self.articles)}: {article.title[:50]}...")
            
            success = await article.scrape()
            if success:
                results["success"] += 1
            else:
                results["failed"] += 1
            
            # Small delay between requests
            await asyncio.sleep(0.5)
        
        print(f"✅ Scraping complete: {results['success']} successful, {results['failed']} failed")
        return results
    
    async def analyse_all_articles(self, banking_entity: str, specific_event: Optional[str] = None) -> Dict[str, int]:
        """Analyze all articles in the collection"""
        results = {"success": 0, "failed": 0}
        
        print(f"🧠 Analyzing {len(self.articles)} articles...")
        if specific_event:
            print(f"🎯 Looking for articles about specific event: '{specific_event}'")
        
        for i, article in enumerate(self.articles):
            print(f"   Analyzing article {i+1}/{len(self.articles)}: {article.title[:50]}...")
            
            analysis = await article.analyse(banking_entity, specific_event)
            if "error" not in analysis:
                results["success"] += 1
            else:
                results["failed"] += 1
                print(f"      Analysis failed: {analysis.get('error', 'Unknown error')}")
            
            # Small delay between requests to avoid rate limiting
            await asyncio.sleep(1)
        
        print(f"✅ Analysis complete: {results['success']} successful, {results['failed']} failed")
        return results
    
    def export_results(self, filename: str) -> bool:
        """Export all articles and their analysis to a JSON file"""
        try:
            export_data = []
            
            for article in self.articles:
                article_data = {
                    "title": article.title,
                    "description": article.description,
                    "url": article.url,
                    "source": article.source,
                    "published_at": article.published_at,
                    "country": article.country,
                    "language": article.language,
                    "category": article.category,
                    "scraped_text": article.scraped_text,
                    "analysis": article.analysis
                }
                export_data.append(article_data)
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(export_data, f, indent=2, ensure_ascii=False)
            
            print(f"✅ Results exported to {filename}")
            return True
            
        except Exception as e:
            print(f"❌ Export failed: {e}")
            return False
    
    def entity_match(self) -> bool:
        """Return True if any article has about_banking_entity set to 'Yes'"""
        for article in self.articles:
            if (article.analysis and 
                isinstance(article.analysis, dict) and 
                article.analysis.get('about_banking_entity') == 'Yes'):
                return True
        return False
    
    def event_match(self) -> bool:
        """Return True if any article has about_specific_event set to 'Yes'"""
        for article in self.articles:
            if (article.analysis and 
                isinstance(article.analysis, dict) and 
                article.analysis.get('about_specific_event') == 'Yes'):
                return True
        return False

# ---- Enumerations ------------------------------------------------------------

class Category(str, Enum):
    regulatory_legal = "regulatory/legal"
    operational = "operational"
    conduct_ethics = "conduct/ethics"
    esg_social = "esg/social"
    customer_market = "customer/market"
    financial_performance = "financial_performance"
    brand_marketing = "brand_marketing"
    third_party = "third_party"
    governance = "governance"


class Stakeholder(str, Enum):
    shareholders = "shareholders"
    management = "management"
    employees = "employees"
    regulators = "regulators"
    customers = "customers"
    business_partners = "business_partners"
    financial_system = "financial_system"
    communities = "communities"
    economy = "economy"
    natural_environment = "natural_environment"


YesNo = Literal["Yes", "No"]
Sentiment = Annotated[float, Field(ge=-1.0, le=1.0, description="-1 (extremely negative) to 1 (extremely positive)")]

# Optional: constrain free-text action items a bit (length + trim)
ActionText = Annotated[str, Field(min_length=3, max_length=300, strip_whitespace=True)]


# ---- Nested models -----------------------------------------------------------

class ReputationManagementSteps(BaseModel):
    """
    Only include items explicitly mentioned in the article.
    Leave arrays empty if not mentioned. Avoid inference.
    """
    model_config = ConfigDict(extra="forbid")

    public_relations_communication: List[ActionText] = Field(default_factory=list)
    customer_remediation: List[ActionText] = Field(default_factory=list)
    regulatory_legal_engagement: List[ActionText] = Field(default_factory=list)
    governance_accountability: List[ActionText] = Field(default_factory=list)
    operational_control_enhancements: List[ActionText] = Field(default_factory=list)
    stakeholder_engagement: List[ActionText] = Field(default_factory=list)
    monitoring_feedback_loops: List[ActionText] = Field(default_factory=list)
    good_deeds: List[ActionText] = Field(default_factory=list)

    @field_validator(
        "public_relations_communication",
        "customer_remediation",
        "regulatory_legal_engagement",
        "governance_accountability",
        "operational_control_enhancements",
        "stakeholder_engagement",
        "monitoring_feedback_loops",
        "good_deeds",
        mode="after",
    )
    def dedupe_and_sort(cls, v: List[str]) -> List[str]:
        """Normalize: de-duplicate while preserving stable order."""
        seen = set()
        out = []
        for x in v:
            xx = x.strip()
            if xx and xx.lower() not in seen:
                seen.add(xx.lower())
                out.append(xx)
        return out


# ---- Top-level model ---------------------------------------------------------

class ArticleAnalysis(BaseModel):
    """
    Strict schema for article analysis results. Use this to validate LLM outputs.
    """
    model_config = ConfigDict(extra="forbid")

    about_banking_entity: YesNo = Field(description='Whether the article is about the specified US banking entity ("Yes" or "No").')
    # Include this field only when a specific event was part of the prompt; keep Optional for validation.
    about_specific_event: Optional[YesNo] = Field(
        default=None,
        description='Whether the article is about the specified event ("Yes" or "No"). Omit if no event was provided.'
    )

    sentiment_wording: Sentiment = Field(description="Tone of the writing (rhetoric).")
    sentiment_facts: Sentiment = Field(description="Valence of the underlying facts reported.")

    category: Category

    affected_stakeholders: List[Stakeholder] = Field(
        default_factory=list,
        description="Stakeholder groups explicitly affected, from the closed list."
    )

    reputation_management_steps: ReputationManagementSteps

    # --- Validators -----------------------------------------------------------

    @field_validator("affected_stakeholders", mode="after")
    def dedupe_stakeholders(cls, v: List[Stakeholder]) -> List[Stakeholder]:
        """Remove duplicates while keeping original order."""
        seen = set()
        out: List[Stakeholder] = []
        for s in v:
            if s not in seen:
                seen.add(s)
                out.append(s)
        return out

    @field_validator("sentiment_wording", "sentiment_facts", mode="after")
    def round_sentiment(cls, v: float) -> float:
        """Optionally normalize to one decimal place for consistency."""
        return round(float(v), 2)  # adjust to 1 or 2 decimals to suit your pipeline

# Example usage
#async def main():
#    """Example usage of the MediaStackSearch class"""
#    
#    # Initialize search
#    searcher = MediaStackSearch()
#    
#    # Search for articles about a specific bank
#    success = await searcher.search(
#        search_phrase="JPMorgan Chase,
#        start_date=date(2024, 1, 1),
#        end_date=date(2024, 1, 31),
#        exclude_keywords=["earnings", "quarterly", "profit"]  # Exclude routine earnings news
#    )
    
#    if success:
#        print(f"Found {searcher.article_count()} articles")
        
        # Scrape all articles
#        await searcher.scrape_all_articles()
        
        # Analyze all articles
#        await searcher.analyse_all_articles("JPMorgan Chase", "data breach incident")
        
        # Export results
#        searcher.export_results("jpmorgan_analysis.json")
        
        # Example of accessing individual articles
#        if searcher.article_count() > 0:
#            first_article = searcher.get_article(0)
#            print(f"First article: {first_article.title}")
#            if first_article.analysis:
#                print(f"Analysis: {first_article.analysis}")


#if __name__ == "__main__":
#    asyncio.run(main())
