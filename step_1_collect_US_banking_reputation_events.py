import os
import calendar
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Literal

import pandas as pd
import requests
from openpyxl.utils import get_column_letter

# For API calls and Pydantic validation
import openai
import instructor
from pydantic import BaseModel, Field, HttpUrl

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    def _load_env_file(path: str = ".env") -> None:
        try:
            with open(path, "r", encoding="utf-8") as fp:
                for line in fp:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        key, value = line.split("=", 1)
                        os.environ.setdefault(key.strip(), value.strip())
        except FileNotFoundError:
            pass
    _load_env_file()

# Use instructor to patch the OpenAI client
client = openai.OpenAI()
instructor.patch(client=client)

# --- Pydantic Models for Schema Validation ---

class Source(BaseModel):
    title: str = Field(..., description="The headline of the source article or document.")
    publisher: str = Field(..., description="The publisher or organization (e.g., 'The New York Times', 'FDIC').")
    url: HttpUrl
    date_published: str = Field(..., description="Publication date in 'YYYY-MM-DD' format.")
    source_type: Literal["regulator", "media", "court"]

class ReputationalDamage(BaseModel):
    nature: List[str]
    materiality_score: int = Field(..., ge=1, le=5)
    drivers: Dict[str, Any]

class BankEvent(BaseModel):
    # FIX: Updated description to enforce a clear and unique ID format.
    event_id: str = Field(..., description="A unique kebab-case ID formatted as 'YYYY-MM-DD-primary-institution-event-keyword' (e.g., '2023-03-10-silicon-valley-bank-failure').")
    title: str
    institutions: List[str]
    parent_company: Optional[str] = None
    jurisdictions: List[str]
    categories: List[str]
    summary: str
    reputational_damage: ReputationalDamage
    amounts: Dict[str, Any] = Field(default_factory=dict)
    sources: List[Source]
    date_first_public: str

class ProcessedEvents(BaseModel):
    events: List[BankEvent]
    notes: str

# --- Data Fetching Functions ---

def run_google_search(query: str, sort_by_date_range: str, pages: int = 3) -> List[Dict[str, Any]]:
    """Generic function to run a paginated Google Custom Search."""
    api_key = os.getenv("GOOGLE_API_KEY")
    cse_id = os.getenv("GOOGLE_CSE_ID")
    if not api_key or not cse_id:
        raise ValueError("Missing GOOGLE_API_KEY or GOOGLE_CSE_ID in environment variables.")

    url = "https://www.googleapis.com/customsearch/v1"
    all_results = []
    
    for i in range(pages):
        start_index = 1 + (i * 10)
        params = {
            "key": api_key, "cx": cse_id, "q": query,
            "num": 10, "sort": sort_by_date_range, "start": start_index
        }
        try:
            response = requests.get(url, params=params, timeout=20)
            response.raise_for_status()
            data = response.json()
            results = data.get("items", [])
            if not results:
                break
            all_results.extend(results)
            time.sleep(0.5)
        except requests.RequestException as e:
            if e.response and e.response.status_code == 400:
                break
            print(f"✗ Google Search API error for query '{query}': {e}")
            break
            
    return all_results

def get_monthly_data(year: int, month: int) -> Dict[str, List[Dict[str, Any]]]:
    """Fetches both general news and regulatory actions for a given month."""
    last_day = calendar.monthrange(year, month)[1]
    start_date, end_date = f"{year}{month:02d}01", f"{year}{month:02d}{last_day:02d}"
    sort_by_date_range = f"date:r:{start_date}:{end_date}"

    news_query = '"US bank" OR "banking" scandal OR failure OR fine OR lawsuit "customer impact"'
    news_results = run_google_search(news_query, sort_by_date_range, pages=3)

    regulatory_query = (
        '("enforcement action" OR "press release" OR "consent order") '
        '(site:fdic.gov OR site:occ.gov OR site:federalreserve.gov OR site:consumerfinance.gov)'
    )
    regulatory_results = run_google_search(regulatory_query, sort_by_date_range, pages=2)
    
    return {"news": news_results, "regulatory": regulatory_results}

# --- AI Processing Function ---

def process_events_with_ai(monthly_data: Dict[str, List[Dict[str, Any]]]) -> ProcessedEvents:
    """Uses an AI model to filter, deduplicate, and enrich search results from multiple sources."""
    # FIX: Added a specific instruction for creating the event_id.
    system_prompt = """
You are an expert financial research analyst. Your task is to analyze search results from two sources—general news and official regulatory websites—to identify major negative US banking events.

**Source Prioritization Strategy:**
1.  **Trust Regulatory Sources First:** For facts like dates, penalty amounts, and official charges, data from regulatory sites is the ground truth.
2.  **Use News for Context:** Use news articles to understand public reaction and customer impact.

**Strict Filtering Rules:**
-   **Extract:** Focus only on major bank failures, widespread scandals, large government fines (>$1M), and significant data breaches.
-   **Ignore:** Aggressively discard minor administrative actions, routine announcements, and market commentary.

**Instructions:**
1.  Review all provided search results from both `news` and `regulatory` sources.
2.  Deduplicate entries that refer to the same core event.
3.  For each unique event, create a single, comprehensive record.
4.  **Crucially, create a unique `event_id` for each event using the format: `YYYY-MM-DD-primary-institution-event-keyword` (e.g., `2023-03-10-silicon-valley-bank-failure`).**
5.  Return a Pydantic `ProcessedEvents` object. Do not include any other text.
"""
    
    news_string = "\n---\n".join([f"Title: {r.get('title')}\nSnippet: {r.get('snippet')}\nLink: {r.get('link')}" for r in monthly_data['news']])
    regulatory_string = "\n---\n".join([f"Title: {r.get('title')}\nSnippet: {r.get('snippet')}\nLink: {r.get('link')}" for r in monthly_data['regulatory']])
    
    user_prompt = f"### Regulatory Sources:\n{regulatory_string}\n\n### General News Sources:\n{news_string}"

    try:
        return client.chat.completions.create(
            model="gpt-4o",
            response_model=ProcessedEvents,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.0
        )
    except Exception as e:
        print(f"✗ AI processing error: {e}")
        return ProcessedEvents(events=[], notes=f"Error during processing: {e}")

# --- Data Handling and Output ---

def json_to_bank_events_table(events: List[BankEvent]) -> pd.DataFrame:
    records = [event.model_dump() for event in events]
    df = pd.json_normalize(records, sep='_')
    
    if 'reputational_damage_drivers' in df.columns:
        df.drop(columns=['reputational_damage_drivers'], inplace=True)

    if "date_first_public" in df.columns:
        df["date_first_public"] = pd.to_datetime(df["date_first_public"], errors='coerce')
    
    return df

def save_to_excel(df: pd.DataFrame, filename: str) -> None:
    with pd.ExcelWriter(filename, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='Bank Events', index=False)
        worksheet = writer.sheets['Bank Events']
        for col_idx, column in enumerate(df.columns, 1):
            column_letter = get_column_letter(col_idx)
            max_len = max(df[column].astype(str).map(len).max(), len(column))
            worksheet.column_dimensions[column_letter].width = min(max_len + 2, 60)
    print(f"✓ Data saved to '{filename}'.")

def print_summary_stats(df: pd.DataFrame) -> None:
    print("\n=== FINAL DATA SUMMARY ===\n")
    print(f"Total unique events found: {len(df)}")
    if not df.empty:
        print(f"Date range of events: {df['date_first_public'].min():%Y-%m-%d} to {df['date_first_public'].max():%Y-%m-%d}")
        print("\n--- Top 5 Event Categories ---")
        all_categories = df['categories'].explode().dropna()
        print(all_categories.value_counts().head(5))
        print("\n--- Materiality Score Distribution ---")
        print(df['reputational_damage_materiality_score'].value_counts().sort_index())
    else:
        print("No events were found or processed.")

# --- Main Execution Block ---
if __name__ == "__main__":
    start_year, start_month = 2005, 1
    today = datetime.now()
    end_year, end_month = today.year, today.month
    
    print(f"Beginning data collection from {start_year}-{start_month:02d} to {end_year}-{end_month:02d}...")
    
    all_events_by_year = {}
    current_year, current_month = start_year, start_month
    
    while (current_year, current_month) <= (end_year, end_month):
        print(f"Searching for data in {current_year}-{current_month:02d}...", end=" ")
        
        monthly_data = get_monthly_data(current_year, current_month)
        total_found = len(monthly_data['news']) + len(monthly_data['regulatory'])
        print(f"Found {total_found} total results.")
        
        if total_found > 0:
            if current_year not in all_events_by_year:
                all_events_by_year[current_year] = {'news': [], 'regulatory': []}
            all_events_by_year[current_year]['news'].extend(monthly_data['news'])
            all_events_by_year[current_year]['regulatory'].extend(monthly_data['regulatory'])
        
        if current_month == 12:
            current_month, current_year = 1, current_year + 1
        else:
            current_month += 1
        time.sleep(1)
        
    print("\nRaw data collection complete.")
    
    final_processed_events = []
    for year, yearly_data in all_events_by_year.items():
        total_yearly_results = len(yearly_data['news']) + len(yearly_data['regulatory'])
        print(f"\nStarting AI processing for {year} ({total_yearly_results} results)...")
        
        processed_data = process_events_with_ai(yearly_data)
        print(f"AI processing for {year} complete. Notes: '{processed_data.notes}'")
        final_processed_events.extend(processed_data.events)

    print("\nConsolidating all processed events...")
    if final_processed_events:
        events_df = json_to_bank_events_table(final_processed_events)
        print_summary_stats(events_df)
        save_to_excel(events_df, filename="us_bank_reputation_events_final.xlsx")
    else:
        print("No significant events were found after processing.")
    
    print("\n✓ Script execution finished.")