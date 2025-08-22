import ast
from datetime import timedelta
import os
import json
import requests
from typing import List, Dict, Any, Optional
from enum import Enum
import math
import time
from urllib.parse import urlparse
import io
import unicodedata 
import tiktoken 

import pandas as pd
from openai import OpenAI, APIError
import instructor
from pydantic import BaseModel, HttpUrl, field_validator, ValidationError, Field

# --- Dependency Setup ---
try:
    from dotenv import load_dotenv, find_dotenv, get_key
    load_dotenv(find_dotenv())
except ImportError:
    print("dotenv not installed. Run 'pip install python-dotenv'.")
    find_dotenv = lambda: None; get_key = lambda path, key: None

try:
    from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
    from readability import Document
    from bs4 import BeautifulSoup
except ImportError:
    print("Required scraping libraries not installed. Run 'pip install playwright readability-lxml beautifulsoup4'.")
    print("Then, install browser binaries with 'playwright install'.")
    sync_playwright, PlaywrightTimeoutError, Document, BeautifulSoup = None, None, None, None

try:
    import pdfplumber
except ImportError:
    print("pdfplumber is not installed. Run 'pip install pdfplumber' to handle PDF files.")
    pdfplumber = None

# --- Helper Functions ---

def postprocess_to_ascii(text: str) -> str:
    """
    Converts a string to ASCII by replacing known control characters and
    normalizing Unicode to its closest ASCII representation.
    """
    if not isinstance(text, str):
        return ""
    text = text.replace('\u0006', ' ')
    normalized_text = unicodedata.normalize('NFKD', text)
    return normalized_text.encode('ascii', 'ignore').decode('utf-8')

def get_contextual_summary(
    text: str, 
    query_context: str, 
    client: OpenAI, 
    model: str = "gpt-4o-mini",
    max_tokens: int = 8000
) -> str:
    """
    Splits long text into chunks and generates a focused summary of each, then combines them.
    """
    try:
        encoding = tiktoken.encoding_for_model(model)
    except KeyError:
        encoding = tiktoken.get_encoding("cl100k_base")

    tokens = encoding.encode(text)
    chunks = [tokens[i:i + max_tokens] for i in range(0, len(tokens), max_tokens)]
    
    summaries = []
    print(f"     - Summarizing {len(chunks)} chunks...")
    for i, chunk in enumerate(chunks):
        chunk_text = encoding.decode(chunk)
        prompt = f"""
        Read the following text chunk and summarize any and all information related to the query: "{query_context}".
        Focus exclusively on details matching the query. If no relevant information is found in this chunk, respond with "No relevant information."
        
        TEXT CHUNK:
        ---
        {chunk_text}
        ---
        """
        try:
            response = client.chat.completions.create(
                 model=model,
                 messages=[{"role": "user", "content": prompt}],
                 max_tokens=4000
             )
            summary = response.choices[0].message.content
            if "no relevant information" not in summary.lower():
                summaries.append(summary)
            print(f"       - Chunk {i+1}/{len(chunks)} summarized.")
        except Exception as e:
            print(f"       - Error summarizing chunk {i+1}: {e}")
        time.sleep(1)

    if not summaries:
        return "The document was too long and a contextual summary could not be created."

    combined_summary_text = "\n\n---\n\n".join(summaries)
    if len(summaries) > 1:
        final_prompt = f"""
        The following are focused summaries from different parts of a long document.
        Combine them into a single, clean, and coherent summary that covers all key points related to the query: "{query_context}".
        
        SUMMARIES:
        ---
        {combined_summary_text}
        ---
        """
        final_response = client.chat.completions.create(
             model=model,
             messages=[{"role": "user", "content": final_prompt}],
             max_tokens=4000
         )
        return final_response.choices[0].message.content
    else:
        return combined_summary_text

# --- Pydantic Models & Data Structures ---

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
    main_entity: str = Field(description="The main company, organization, or person the article is about.")
    full_text: str = Field(description="The full scraped text of the article, or a contextual summary if the original text was too long.")
    customer_impact: bool = Field(description="True if the event described will directly impact the bank's customers.")

    @field_validator("sentiment_score")
    @classmethod
    def score_in_range(cls, v: float) -> float:
        if not -1.0 <= v <= 1.0:
            raise ValueError("sentiment_score must be between -1.0 and 1.0")
        return v

# --- Core Functions ---

def google_custom_search(query: str, api_key: str, cse_id: str, total_results_to_fetch: int = 10, start_date: Optional[str] = None, end_date: Optional[str] = None, site_restriction_query: str = "") -> List[Dict[str, Any]]:
    if total_results_to_fetch <= 0: return []
    url = "https://www.googleapis.com/customsearch/v1"
    all_results, num_pages = [], math.ceil(total_results_to_fetch / 10)
    final_query = f"{query} {site_restriction_query}".strip()
    print(f"🔍 Searching Google for '{final_query}' (aiming for {total_results_to_fetch} results)...")
    for page in range(num_pages):
        start_index = 1 + (page * 10)
        params = {"key": api_key, "cx": cse_id, "q": final_query, "num": 10, "start": start_index}
        if start_date and end_date: params["sort"] = f"date:r:{start_date.replace('-', '')}:{end_date.replace('-', '')}"
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            results = response.json().get("items", [])
            if not results: break
            all_results.extend(results)
            if len(all_results) >= total_results_to_fetch: break
        except requests.exceptions.RequestException as e:
            raise MediaDataError(f"Google Search API error on page {page + 1}: {e}") from e
    return all_results[:total_results_to_fetch]

def fetch_and_process_media_coverage(
    bank_name: str,
    year: str,
    case_study_summary: str,
    *,
    num_articles_to_find: int = 50,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    model: str = "gpt-4o",
    timeout_seconds: int = 300,
) -> List[MediaArticle]:
    dotenv_path = find_dotenv(); google_api_key = get_key(dotenv_path, "GOOGLE_API_KEY"); google_cse_id = get_key(dotenv_path, "GOOGLE_CSE_ID"); openai_api_key = get_key(dotenv_path, "OPENAI_API_KEY")
    if not all([google_api_key, google_cse_id, openai_api_key]): raise MediaDataError("Missing required API keys.")
    if not all([sync_playwright, Document, BeautifulSoup]): raise MediaDataError("Scraping libraries not installed.")
    
    is_unknown_bank = bank_name.strip().lower() == "unknown bank"
    base_search_query = f"bank {case_study_summary.split('.')[0]} {year} -site:wikipedia.org" if is_unknown_bank else f"{bank_name} {case_study_summary.split('.')[0]} {year} -site:wikipedia.org"
    
    TIER_1_SOURCES = ["nytimes.com", "wsj.com", "forbes.com", "reuters.com", "apnews.com", "bloomberg.com", "theguardian.com", "ft.com", "economist.com", "bbc.com/news"]
    tier_1_target = math.ceil(num_articles_to_find * 0.7)
    tier_1_site_query = " OR ".join([f"site:{site}" for site in TIER_1_SOURCES])
    tier_1_results = google_custom_search(base_search_query, google_api_key, google_cse_id, tier_1_target, start_date, end_date, tier_1_site_query)
    
    other_results = []; remaining_articles_needed = num_articles_to_find - len(tier_1_results)
    if remaining_articles_needed > 0:
        other_results_target = math.ceil(remaining_articles_needed * 1.2) + 2
        exclusion_site_query = " ".join([f"-site:{site}" for site in TIER_1_SOURCES])
        other_results = google_custom_search(base_search_query, google_api_key, google_cse_id, other_results_target, start_date, end_date, exclusion_site_query)
    
    combined_results = tier_1_results + other_results
    seen_links = set(); search_results = [item for item in combined_results if (link := item.get("link")) and not (link in seen_links or seen_links.add(link))]
    final_unique_results = search_results[:num_articles_to_find]
    
    if not final_unique_results: return []

    client = instructor.patch(OpenAI(api_key=openai_api_key, timeout=timeout_seconds))
    all_processed_articles = []
    
    relevance_instruction = f"Determine Relevance: Set 'is_relevant' to true ONLY IF the article is directly about a banking-related event in '{year}' as described..." if is_unknown_bank else f"Determine Relevance: Set 'is_relevant' to true ONLY IF the article is directly about the '{bank_name}' event in '{year}'."

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        for i, item in enumerate(final_unique_results):
            print(f"   - Processing article {i + 1} of {len(final_unique_results)}: {item.get('link')}")
            
            url = item.get("link")
            text = "Scraping failed or was not possible."
            page = None
            max_retries = 2
            
            for attempt in range(max_retries + 1):
                try:
                    if url and url.lower().endswith('.pdf'):
                        if not pdfplumber: raise MediaDataError("pdfplumber required for PDFs.")
                        headers = {'User-Agent': 'Mozilla/5.0'}
                        response = requests.get(url, timeout=60, headers=headers)
                        response.raise_for_status()
                        pdf_file = io.BytesIO(response.content)
                        extracted_text = []
                        with pdfplumber.open(pdf_file) as pdf:
                            for pdf_page in pdf.pages:
                                page_text = pdf_page.extract_text()
                                if page_text: extracted_text.append(page_text)
                        text = "\n".join(extracted_text)
                        break  # Success, exit retry loop
                    elif url:
                        page = browser.new_page()
                        page.goto(url, wait_until='domcontentloaded', timeout=45000)
                        html_content = page.content()
                        doc = Document(html_content)
                        soup = BeautifulSoup(doc.summary(), 'html.parser')
                        text = soup.get_text(separator='\n', strip=True)
                        break  # Success, exit retry loop
                except Exception as e:
                    if attempt < max_retries:
                        print(f"     - ⚠️ Attempt {attempt + 1} failed: {e}. Retrying...")
                        time.sleep(2)  # Wait before retry
                        if page: 
                            page.close()
                            page = None
                    else:
                        text = f"Scraping failed after {max_retries + 1} attempts: {e}"
                finally:
                    if page: page.close()
            
            try:
                encoding = tiktoken.encoding_for_model(model)
            except KeyError:
                encoding = tiktoken.get_encoding("cl100k_base")

            token_count = len(encoding.encode(text))
            TOKEN_THRESHOLD = 90000 

            if token_count > TOKEN_THRESHOLD:
                print(f"     - ⚠️ Text is too long ({token_count} tokens). Generating contextual summary...")
                query_context = f"{bank_name} {case_study_summary} {year}"
                processed_text = get_contextual_summary(text, query_context, client)
            else:
                processed_text = text

            scraped_article_data = {
                "title": postprocess_to_ascii(item.get("title")),
                "link": url,
                "snippet": postprocess_to_ascii(item.get("snippet")),
                "full_text": postprocess_to_ascii(processed_text)
            }
            
            formatted_results = json.dumps(scraped_article_data, indent=2)
            prompt = f"""Based ONLY on the provided JSON data for a SINGLE article, your task is to analyze it and extract the following:
1. Core Info: title, link (as 'url'), and a concise 'summary'.
2. Metadata: source, date, sentiment, and sentiment_score.
3. Main Entity: The primary company/bank in 'main_entity'.
4. Customer Impact: A 'customer_impact' boolean.
5. {relevance_instruction}
6. Relevance Reason: A 'relevance_reason'.
7. Full Text: Copy the provided 'full_text' into the output.
ARTICLE DATA: {formatted_results}"""

            try:
                article = client.chat.completions.create(
                    model=model, 
                    messages=[{"role": "system", "content": "You are an expert data processing assistant. Your output must strictly adhere to the requested Pydantic schema."}, {"role": "user", "content": prompt}], 
                    response_model=MediaArticle,
                    max_tokens=8000
                )
                
                article.title = postprocess_to_ascii(article.title)
                article.summary = postprocess_to_ascii(article.summary)
                article.relevance_reason = postprocess_to_ascii(article.relevance_reason)
                article.main_entity = postprocess_to_ascii(article.main_entity)
                article.full_text = postprocess_to_ascii(article.full_text)
                
                all_processed_articles.append(article)
                print(f"     - Successfully processed and cleaned.")
            except (APIError, ValidationError) as e:
                print(f"     - ⚠️ Error processing article: {e}. Skipping.")
                continue
            time.sleep(1) 
        browser.close()
    return all_processed_articles

# --- Main Execution Block ---
if __name__ == "__main__":
    try:
        # NOTE: You will need to have a file named "us_bank_reputation_events_final.xlsx"
        # with columns 'institutions', 'date_first_public', and 'title'.
        events_df = pd.read_excel("us_bank_reputation_events_final.xlsx")
        iRow = 0
        event_id = str(events_df.iloc[iRow]["event_id"])
        bank_name = ast.literal_eval(str(events_df.iloc[iRow]["institutions"]))[0]
        date_first_public = events_df.iloc[iRow]["date_first_public"]
        year = date_first_public.year
        case_study_summary = str(events_df.iloc[iRow]["title"])
        
        TOTAL_ARTICLES_TO_FIND = 50
        SEARCH_START_DATE = date_first_public.strftime('%Y-%m-%d')
        SEARCH_END_DATE = (date_first_public + timedelta(days=90)).strftime('%Y-%m-%d')

        print("\n" + "=" * 80 + "\nStarting Workflow...")
        articles = fetch_and_process_media_coverage(
            bank_name, 
            str(year), 
            case_study_summary, 
            num_articles_to_find=TOTAL_ARTICLES_TO_FIND, 
            start_date=SEARCH_START_DATE, 
            end_date=SEARCH_END_DATE
        )
        print(f"\n✅ Success! Processed {len(articles)} articles.\n")
        
        final_json = {
            "event_id": event_id,
            "bank_name": bank_name, "year": year, "case_study_summary": case_study_summary,
            "article_count": len(articles),
            "articles": [a.model_dump(mode='json') for a in articles if a.is_relevant]
        }
        
        output_dir = "./news_articles"
        os.makedirs(output_dir, exist_ok=True)
        output_filename = os.path.join(output_dir, f"{bank_name.replace(' ', '_')}_{year}_media_analysis.json")

        with open(output_filename, 'w', encoding='utf-8') as f:
            json.dump(final_json, f, indent=4, ensure_ascii=False)
        print(f"📄 Results saved to {output_filename}")

    except FileNotFoundError:
        print("❌ Error: 'us_bank_reputation_events_final.xlsx' not found. Please ensure the file is in the correct directory.")
    except (MediaDataError, ValidationError, ValueError) as e:
        print(f"❌ Error: {e}")
    except Exception as e:
        print(f"An unexpected application error occurred: {e}")
    print("\n" + "=" * 80 + "\nCompleted")