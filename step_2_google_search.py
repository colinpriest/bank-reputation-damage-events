import os
import json
import requests
from typing import List, Dict, Any, Optional
from enum import Enum
import math
import time
from urllib.parse import urlparse

import pandas as pd
from openai import OpenAI, APIError
# Instructor will handle the Pydantic integration
import instructor
from pydantic import BaseModel, HttpUrl, field_validator, ValidationError, Field

# Use python-dotenv to find and read the .env file directly
try:
    from dotenv import load_dotenv, find_dotenv, get_key
    load_dotenv(find_dotenv())
except ImportError:
    print("dotenv not installed. Run 'pip install python-dotenv'.")
    find_dotenv = lambda: None
    get_key = lambda path, key: None


class MediaDataError(RuntimeError):
    """Custom exception for errors during media data fetching or validation."""

class Sentiment(str, Enum):
    positive = "positive"
    neutral = "neutral"
    negative = "negative"

class MediaArticle(BaseModel):
    title: str
    url: HttpUrl = Field(..., alias="link") 
    source: str
    date: str
    summary: str
    sentiment: Sentiment
    sentiment_score: float
    is_relevant: bool = Field(description="True if the article is directly about the specified event and entity.")
    relevance_reason: str = Field(description="A brief reason for the relevance decision.")

    @field_validator("sentiment_score")
    @classmethod
    def score_in_range(cls, v: float) -> float:
        if not -1.0 <= v <= 1.0:
            raise ValueError("sentiment_score must be between -1.0 and 1.0")
        return v

class MediaArticleResponse(BaseModel):
    articles: List[MediaArticle]

def google_custom_search(
    query: str, 
    api_key: str, 
    cse_id: str, 
    total_results_to_fetch: int = 10,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    ### MODIFICATION: Added parameter to handle site restrictions
    site_restriction_query: str = ""
) -> List[Dict[str, Any]]:
    """
    Performs a paginated search using the Google Custom Search API, with an optional date range and site restrictions.
    """
    if total_results_to_fetch <= 0:
        return []
        
    url = "https://www.googleapis.com/customsearch/v1"
    all_results = []
    num_pages = math.ceil(total_results_to_fetch / 10)

    ### MODIFICATION: Append the site restriction/exclusion string to the main query
    final_query = f"{query} {site_restriction_query}".strip()
    
    print(f"🔍 Searching Google for '{final_query}' (aiming for {total_results_to_fetch} results across {num_pages} pages)...")
    if start_date and end_date:
        print(f"   - Restricting search to date range: {start_date} to {end_date}")

    for page in range(num_pages):
        start_index = 1 + (page * 10)
        params = { "key": api_key, "cx": cse_id, "q": final_query, "num": 10, "start": start_index }
        
        if start_date and end_date:
            params["sort"] = f"date:r:{start_date.replace('-', '')}:{end_date.replace('-', '')}"
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            results = response.json().get("items", [])
            if not results:
                print(f"  - No more results found on page {page + 1}. Ending search.")
                break
            
            all_results.extend(results)
            print(f"  - Fetched {len(results)} results from page {page + 1}. Total so far: {len(all_results)}.")
            if len(all_results) >= total_results_to_fetch:
                break
        except requests.exceptions.RequestException as e:
            raise MediaDataError(f"Google Search API error on page {page + 1}: {e}") from e

    final_results = all_results[:total_results_to_fetch]
    print(f"✅ Found a total of {len(final_results)} results from Google for this pass.")
    return final_results

def fetch_and_process_media_coverage(
    bank_name: str,
    year: str,
    case_study_summary: str,
    *,
    num_articles_to_find: int = 50,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    model: str = "gpt-4o",
    timeout_seconds: int = 120,
) -> List[MediaArticle]:
    """
    Finds articles with a two-pass Google Search (prioritizing Tier 1 sources) and processes them with OpenAI.
    """
    dotenv_path = find_dotenv()
    if not dotenv_path:
        raise MediaDataError("Could not find the .env file.")

    google_api_key = get_key(dotenv_path, "GOOGLE_API_KEY")
    google_cse_id = get_key(dotenv_path, "GOOGLE_CSE_ID")
    openai_api_key = get_key(dotenv_path, "OPENAI_API_KEY")

    if not all([google_api_key, google_cse_id, openai_api_key]):
        raise MediaDataError("Missing required API keys in your .env file.")

    TIER_1_SOURCES = [
        "nytimes.com", "wsj.com", "forbes.com", "reuters.com", "apnews.com",
        "bloomberg.com", "theguardian.com", "ft.com", "economist.com", "bbc.com/news"
    ]
    tier_1_target = math.ceil(num_articles_to_find * 0.7)
    
    print("\n--- Starting Pass 1: Searching Tier 1 Sources ---")
    tier_1_site_query = " OR ".join([f"site:{site}" for site in TIER_1_SOURCES])
    # MODIFICATION: Add the Wikipedia exclusion directly to the base query
    base_search_query = f"{bank_name} {case_study_summary.split('.')[0]} {year} -site:wikipedia.org"
    
    tier_1_results = google_custom_search(
        base_search_query, 
        google_api_key, 
        google_cse_id, 
        total_results_to_fetch=tier_1_target,
        start_date=start_date,
        end_date=end_date,
        site_restriction_query=tier_1_site_query
    )

    print("\n--- Starting Pass 2: Searching Other Sources ---")
    remaining_articles_needed = num_articles_to_find - len(tier_1_results)
    
    other_results = []
    if remaining_articles_needed > 0:
        ### MODIFICATION: Add a buffer to the second search to account for potential duplicates.
        # We'll fetch 20% more than needed, plus a small constant (2) to be safe.
        other_results_target = math.ceil(remaining_articles_needed * 1.2) + 2
        print(f"   - Aiming to fetch ~{other_results_target} articles in Pass 2 to create a buffer.")

        exclusion_site_query = " ".join([f"-site:{site}" for site in TIER_1_SOURCES])
        other_results = google_custom_search(
            base_search_query, 
            google_api_key, 
            google_cse_id, 
            total_results_to_fetch=other_results_target,
            start_date=start_date,
            end_date=end_date,
            site_restriction_query=exclusion_site_query
        )
    else:
        print("Tier 1 search fulfilled the request. Skipping Pass 2.")

    combined_results = tier_1_results + other_results
    
    seen_links = set()
    search_results = []
    for item in combined_results:
        link = item.get("link")
        if link not in seen_links:
            seen_links.add(link)
            search_results.append(item)

    ### MODIFICATION: After deduplication, trim the list to the exact target size.
    final_unique_results = search_results[:num_articles_to_find]
            
    print(f"\nCombined and deduplicated results. Total unique articles to process: {len(final_unique_results)}")
    
    if not final_unique_results:
        return []

    # Process results in batches
    client = instructor.patch(OpenAI(api_key=openai_api_key, timeout=timeout_seconds))
    all_processed_articles = []
    batch_size = 10
    
    result_batches = [final_unique_results[i:i + batch_size] for i in range(0, len(final_unique_results), batch_size)]

    print(f"🤖 Processing {len(final_unique_results)} results in {len(result_batches)} batches of {batch_size}...")

    for i, batch in enumerate(result_batches):
        print(f"  - Processing batch {i + 1} of {len(result_batches)}...")
        
        formatted_results = json.dumps([{"title": item.get("title"), "link": item.get("link"), "snippet": item.get("snippet")} for item in batch], indent=2)

        prompt = f"""
        Based ONLY on the provided JSON data from a Google search, process every single article in the list. Do not filter any out.
        Your task is to:
        1. Extract the title, link (as 'url'), and create a summary from the snippet.
        2. Infer the source, date, sentiment, and sentiment_score.
        3. **Determine Relevance**: For each article, set 'is_relevant' to true if the title and snippet are directly about the '{bank_name}' event in '{year}'. Otherwise, set it to false.
        4. **Provide a Reason**: If 'is_relevant' is false, briefly explain why in the 'relevance_reason' field (e.g., "Irrelevant topic," "Wrong year," "Corporate homepage"). If it is relevant, set the reason to "Relevant".
        
        SEARCH RESULTS:
        {formatted_results}
        """

        try:
            response = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": "You are an expert data processing assistant. Your output must strictly adhere to the requested Pydantic schema for every item provided."},
                    {"role": "user", "content": prompt},
                ],
                response_model=MediaArticleResponse,
            )
            all_processed_articles.extend(response.articles)
            print(f"    - Successfully processed {len(response.articles)} articles in this batch.")
        except (APIError, ValidationError) as e:
            print(f"    - ⚠️ Error processing batch {i + 1}: {e}. Skipping this batch.")
            continue
        
        time.sleep(1) 

    print("✅ OpenAI successfully processed all batches.")
    return all_processed_articles


if __name__ == "__main__":
    try:
        events_df = pd.read_excel("./event_spreadsheets/US_bank_Reputational_Damage_2015_2025 - Claude.xlsx")
        iRow = 0
        bank_name = str(events_df.iloc[iRow]["Bank"])
        year = str(events_df.iloc[iRow]["Case Name + Year"])[-4:]
        case_study_summary = str(events_df.iloc[iRow]["Event Summary"])

        TOTAL_ARTICLES_TO_FIND = 50
        SEARCH_START_DATE = None 
        SEARCH_END_DATE = None

        print("\n" + "=" * 80)
        print(f"Starting Hybrid Search and Process Workflow with Instructor (Target: {TOTAL_ARTICLES_TO_FIND} Articles):")
        print(f"  Bank:    {bank_name}")
        print(f"  Year:    {year}")
        print("=" * 80)

        articles = fetch_and_process_media_coverage(
            bank_name,
            year,
            case_study_summary,
            num_articles_to_find=TOTAL_ARTICLES_TO_FIND,
            start_date=SEARCH_START_DATE,
            end_date=SEARCH_END_DATE,
        )
        
        print(f"\n✅ Success! Found and processed {len(articles)} articles.\n")
        pretty_json = json.dumps([a.model_dump(mode='json') for a in articles], indent=2)
        final_json = {
            "bank_name": bank_name,
            "year": year,
            "case_study_summary": case_study_summary,
            "article_count": len(articles),
            "relevant_article_count": sum(1 for a in articles if a.is_relevant),
            "articles": [a.model_dump(mode='json') for a in articles]
        }
        print(json.dumps(final_json, indent=2))

    except (MediaDataError, ValidationError, ValueError, FileNotFoundError) as e:
        print(f"❌ Error: {e}")
    except Exception as e:
        print(f"An unexpected application error occurred: {e}")

    print("\n" + "=" * 80)
    print("Completed")