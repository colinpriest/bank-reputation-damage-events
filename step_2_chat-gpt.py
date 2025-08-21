import os
import json
import time
from typing import List, Optional
from enum import Enum

import pandas as pd
from openai import OpenAI, APIError
from pydantic import BaseModel, HttpUrl, field_validator, ValidationError

# --- Setup and Pydantic Models (Unchanged) ---

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    print("dotenv not installed, skipping. Ensure OPENAI_API_KEY is set in your environment.")

class MediaDataError(RuntimeError):
    """Custom exception for errors during media data fetching or validation."""

class Sentiment(str, Enum):
    positive = "positive"
    neutral = "neutral"
    negative = "negative"

class MediaArticle(BaseModel):
    title: str
    url: HttpUrl
    source: str
    date: str
    summary: str
    sentiment: Sentiment
    sentiment_score: float

    @field_validator("sentiment_score")
    @classmethod
    def score_in_range(cls, v: float) -> float:
        if not -1.0 <= v <= 1.0:
            raise ValueError("sentiment_score must be between -1.0 and 1.0")
        return v

class MediaArticleResponse(BaseModel):
    articles: List[MediaArticle]

# --- Main Function (With Cleaning Fix) ---

def fetch_bank_event_media_coverage(
    bank_name: str,
    year: str,
    case_study_summary: str,
    *,
    model: str = "gpt-4o",
    timeout_seconds: int = 120,
) -> List[MediaArticle]:
    """
    Fetches media coverage using the new, simplified OpenAI Responses API with built-in web search.
    """
    client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"), timeout=timeout_seconds)

    prompt = (
        f"Find media articles from US-based news publishers about the {bank_name} "
        f"scandal in {year} related to: '{case_study_summary}'. Exclude non-news aggregators and Wikipedia."
    )

    instructions = (
        "Perform a web search to find relevant articles. "
        "Your output must be ONLY a single, valid JSON object with a key 'articles', "
        "which contains an array of article objects. Each article object must have the fields: "
        "'title', 'url', 'source', 'date', 'summary', 'sentiment', and 'sentiment_score'. "
        "Do not include any prose, markdown, or other text in your response."
    )

    try:
        response = client.responses.create(
            model=model,
            input=prompt,
            instructions=instructions,
        )
        raw_text = response.output_text

    except APIError as e:
        raise MediaDataError(f"OpenAI API error: {e}") from e
    except Exception as e:
        raise MediaDataError(f"An unexpected error occurred during the API call: {e}") from e

    if not raw_text or not raw_text.strip():
        raise MediaDataError("The model returned an empty response.")
        
    # ✅ FIX: Clean the string to remove markdown fences before parsing.
    json_str = raw_text.strip()
    if json_str.startswith("```json"):
        json_str = json_str[7:]
    if json_str.startswith("```"):
        json_str = json_str[3:]
    if json_str.endswith("```"):
        json_str = json_str[:-3]
    json_str = json_str.strip()
        
    try:
        validated_response = MediaArticleResponse.model_validate_json(json_str)
        return validated_response.articles
    except ValidationError as e:
        snippet = json_str[:500].replace("\n", " ")
        raise MediaDataError(f"Pydantic validation failed: {e}\nModel returned: {snippet}")

# --- CLI Entrypoint (Unchanged) ---

if __name__ == "__main__":
    try:
        print("Loading Excel files...")
        events_df = pd.read_excel("./event_spreadsheets/US_bank_Reputational_Damage_2015_2025 - Claude.xlsx")

        iRow = 0
        bank_name = str(events_df.iloc[iRow]["Bank"])
        year = str(events_df.iloc[iRow]["Case Name + Year"])[-4:]
        case_study_summary = str(events_df.iloc[iRow]["Event Summary"])

        print("\n" + "=" * 80)
        print("Searching for media articles using the new Responses API:")
        print(f"  Bank:    {bank_name}")
        print(f"  Year:    {year}")
        print(f"  Summary: {case_study_summary[:150]}...")
        print("=" * 80 + "\n")

        articles = fetch_bank_event_media_coverage(
            bank_name,
            year,
            case_study_summary,
            model="gpt-4o",
        )
        
        print(f"\n✅ Success! Found {len(articles)} articles.\n")
        pretty_json = json.dumps([a.model_dump(mode='json') for a in articles], indent=2)
        print(pretty_json)

    except (MediaDataError, ValidationError, ValueError, FileNotFoundError) as e:
        print(f"❌ Error: {e}")
    except Exception as e:
        print(f"An unexpected application error occurred: {e}")

    print("\n" + "=" * 80)
    print("Completed")