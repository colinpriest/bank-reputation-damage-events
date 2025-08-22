import os
import json
import instructor
from typing import List
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

# --- Main Function (Refactored with Instructor) ---

def fetch_bank_event_media_coverage(
    bank_name: str,
    year: str,
    case_study_summary: str,
    *,
    model: str = "gpt-4o",
    timeout_seconds: int = 120,
) -> List[MediaArticle]:
    """
    Fetches media coverage using the OpenAI API and the Instructor library
    to directly return a validated Pydantic model.
    """
    # ✅ 1. Patch the OpenAI client with Instructor
    client = instructor.patch(
        OpenAI(api_key=os.environ.get("OPENAI_API_KEY"), timeout=timeout_seconds)
    )

    # ✅ 2. The prompt now only focuses on the task, not the output format.
    prompt = (
        f"Find media articles from US-based news publishers about the {bank_name} "
        f"scandal in {year} related to: '{case_study_summary}'. Exclude non-news aggregators and Wikipedia. "
        "Perform a web search to find the most relevant articles."
    )

    try:
        # ✅ 3. Call the API with `response_model` to get a structured Pydantic object back.
        # No more manual JSON cleaning or parsing is needed.
        validated_response = client.chat.completions.create(
            model=model,
            response_model=MediaArticleResponse,
            messages=[
                {
                    "role": "user",
                    "content": prompt,
                }
            ],
        )
        return validated_response.articles
    except (APIError, ValidationError) as e:
        # Instructor will raise a ValidationError if the model's output doesn't match the schema
        raise MediaDataError(f"API or validation error: {e}") from e
    except Exception as e:
        raise MediaDataError(f"An unexpected error occurred: {e}") from e


# --- CLI Entrypoint (Largely Unchanged) ---

if __name__ == "__main__":
    try:
        print("Loading Excel files...")
        events_df = pd.read_excel("./event_spreadsheets/US_bank_Reputational_Damage_2015_2025 - Claude.xlsx")

        iRow = 3
        bank_name = str(events_df.iloc[iRow]["Bank"])
        year = str(events_df.iloc[iRow]["Case Name + Year"])[-4:]
        case_study_summary = str(events_df.iloc[iRow]["Event Summary"])

        print("\n" + "=" * 80)
        # Updated print statement to reflect the use of Instructor
        print("Searching for media articles using OpenAI with Instructor:")
        print(f"  Bank:     {bank_name}")
        print(f"  Year:     {year}")
        print(f"  Summary:  {case_study_summary[:150]}...")
        print("=" * 80 + "\n")

        articles = fetch_bank_event_media_coverage(
            bank_name,
            year,
            case_study_summary,
            model="gpt-4o",
        )
        
        if not articles:
             print("\n⚠️ Warning: No articles were found for the given criteria.")
        else:
            print(f"\n✅ Success! Found {len(articles)} articles.\n")
            pretty_json = json.dumps([a.model_dump(mode='json') for a in articles], indent=2)
            print(pretty_json)

    except (MediaDataError, FileNotFoundError) as e:
        print(f"❌ Error: {e}")
    except Exception as e:
        print(f"An unexpected application error occurred: {e}")

    print("\n" + "=" * 80)
    print("Completed")