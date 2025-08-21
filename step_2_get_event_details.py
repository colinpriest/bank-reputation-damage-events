

import os
import re
import json
import time
import calendar
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
try:
    from dotenv import load_dotenv  # type: ignore
except Exception:
    load_dotenv = None  # type: ignore

# Minimal .env loader (fallback if python-dotenv is not installed)
def _load_env_file(path: str = ".env") -> None:
    """Load key=value pairs from a .env file into os.environ if not already set.

    - Ignores blank lines and lines starting with '#'
    - Supports optional leading 'export '
    - Trims surrounding single or double quotes around values
    - Does not overwrite variables that are already set in the environment
    """
    try:
        with open(path, "r", encoding="utf-8") as fp:
            for raw_line in fp:
                line = raw_line.strip()
                if not line or line.startswith("#"):
                    continue
                if line.startswith("export "):
                    line = line[len("export ") :].strip()
                if "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip()
                if (value.startswith("\"") and value.endswith("\"")) or (
                    value.startswith("'") and value.endswith("'")
                ):
                    value = value[1:-1]
                if key and key not in os.environ:
                    os.environ[key] = value
    except FileNotFoundError:
        pass

# Load environment variables from .env file if python-dotenv is available
if load_dotenv:
    load_dotenv()
else:
    _load_env_file()


class PerplexityAPIError(RuntimeError):
    """Raised when the Perplexity API returns an HTTP or API-level error."""


class JSONStructureError(ValueError):
    """Raised when the model's response is not valid JSON or fails schema checks."""


def _validate_and_repair_json(content: str) -> Tuple[str, bool]:
    """
    Attempt to extract and repair JSON from the model's response.
    
    Returns:
        Tuple of (json_text, was_repaired)
    """
    # First, try to parse as-is
    try:
        json.loads(content)
        return content, False
    except json.JSONDecodeError:
        pass
    
    # Try to extract JSON from markdown code blocks
    json_match = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', content, re.DOTALL)
    if json_match:
        try:
            json.loads(json_match.group(1))
            return json_match.group(1), True
        except json.JSONDecodeError:
            pass
    
    # Try to find JSON object boundaries
    brace_count = 0
    start_idx = -1
    for i, char in enumerate(content):
        if char == '{':
            if brace_count == 0:
                start_idx = i
            brace_count += 1
        elif char == '}':
            brace_count -= 1
            if brace_count == 0 and start_idx != -1:
                json_text = content[start_idx:i+1]
                try:
                    json.loads(json_text)
                    return json_text, True
                except json.JSONDecodeError:
                    pass
    
    # If all else fails, return the original content
    return content, False


def event_media_coverage_prompt(bank_name: str, year: str, case_study_summary: str) -> List[str]:
    """
    Generate a prompt for the event media coverage.
    """
    print("="*100)
    prompt = f"""
    Give me a list of 50 media articles WITH URL LINKS to each article, from US-based news media publishers that covered the {bank_name} scandal in {year} involving {case_study_summary}.
    Note that wikipedia is not a news media publisher.
    If you cannot find 50 articles, give me as many as you can find.
    Provide:
    title: the title of the article
    url: the url of the article
    source: the name of the news media publisher
    date: the date of the article
    summary: a short summary of the article
    sentiment: the sentiment of the article
    sentiment_score: the sentiment score of the article
    """
    print(prompt)
    print("="*100)
    return prompt

def fetch_bank_event_media_coverage(bank_name: str, year: str, case_study_summary: str, api_key: str = None, model: str = "sonar-pro", timeout: int = 30, max_retries: int = 3) -> Dict[str, Any]:
    """
    Query Perplexity's Chat Completions API to retrieve structured JSON of media coverage of a bank event.

    Parameters
    ----------
    bank_name : str
        Name of the bank.
    year : str
        Year of the event.
    case_study_summary : str
        Summary of the case study.


    Returns
    -------
    Dict[str, Any]
        Parsed JSON object conforming to the enforced schema.

    Raises
    ------
    ValueError
        If parameters are invalid.
    PerplexityAPIError
        For HTTP or API-level errors (non-200 response, missing choices, etc.).
    JSONStructureError
        If the response content is not valid JSON or fails schema checks.

    Notes
    -----
    - The function instructs the model to return strict JSON only.
    - The function attempts to extract a JSON block if minor formatting slips occur.
    - Set your API key in the environment as PERPLEXITY_API_KEY (preferred) or PPLX_API_KEY, or pass `api_key=...`.
    """
    api_key = api_key or os.getenv("PERPLEXITY_API_KEY") or os.getenv("PPLX_API_KEY")
    if not api_key:
        raise ValueError("Missing API key. Set PERPLEXITY_API_KEY or PPLX_API_KEY, or pass api_key=...")

    prompt = event_media_coverage_prompt(bank_name, year, case_study_summary)

    url = "https://api.perplexity.ai/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        # Perplexity is OpenAI-compatible; keep UA minimal & clear for traceability.
        "User-Agent": "research-client/1.0",
    }
    
    model = "sonar-deep-research"
    timeout = 900   # because we are using deep research model
    backoff = 1.5
    max_retries = 2
    
    model = "sonar-reasoning-pro"
    timeout = 300   # because we are using just the pro
    backoff = 1.5
    max_retries = 2
    
    model = "sonar-pro"
    timeout = 30   # because we are using just the pro
    backoff = 1.5
    max_retries = 2

    # We send both a System and a User message to maximize JSON-only compliance.
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": "You are a research assistant. Respond **only** with a valid JSON array (no prose, no markdown)."},
            {"role": "user", "content": prompt},
        ],
        # Zero temperature to reduce creative drift; keep defaults conservative.
        "temperature": 0,
        "top_p": 1,
        # Helpful for source auditing if your plan supports it:
        "return_citations": True,
        "stream": False,
        # Guard rails in case the result is long.
        "max_tokens": 4096,
    }

    # Simple retry loop with exponential backoff for transient errors.
    backoff = 1.5
    attempt = 0
    while True:
        attempt += 1
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=timeout)
        except requests.Timeout as e:
            if attempt < max_retries:
                time.sleep(backoff ** attempt)
                continue
            raise PerplexityAPIError(f"Request timed out after {attempt} attempt(s): {e}") from e
        except requests.RequestException as e:
            # Network-level errors (DNS, SSL, connection reset, etc.)
            raise PerplexityAPIError(f"Network error calling Perplexity API: {e}") from e

        # HTTP status handling
        if resp.status_code == 429 and attempt < max_retries:
            # Rate limited: honor Retry-After if present, else exponential backoff.
            retry_after = float(resp.headers.get("Retry-After", backoff ** attempt))
            time.sleep(retry_after)
            continue
        if resp.status_code >= 500 and attempt < max_retries:
            time.sleep(backoff ** attempt)
            continue
        if resp.status_code != 200:
            try:
                detail = resp.json()
            except Exception:
                detail = resp.text
            raise PerplexityAPIError(
                f"Perplexity API error (HTTP {resp.status_code}). Detail: {detail}"
            )

        # Parse the Perplexity envelope
        try:
            envelope = resp.json()
        except json.JSONDecodeError as e:
            raise PerplexityAPIError(f"Non-JSON HTTP body from API: {e}") from e

        choices = envelope.get("choices")
        if not choices or not isinstance(choices, list):
            raise PerplexityAPIError("API response missing 'choices' list.")

        message = choices[0].get("message", {})
        content = message.get("content")
        if not content or not isinstance(content, str):
            raise PerplexityAPIError("API response missing message.content string.")

        # Try to parse and repair JSON if needed
        json_text, was_repaired = _validate_and_repair_json(content)
        
        try:
            data = json.loads(json_text)
            if was_repaired:
                print(f"  (JSON repaired successfully)")
        except json.JSONDecodeError as e:
            # If repair failed, this is a JSONStructureError
            raise JSONStructureError(f"Returned content is not valid JSON and could not be repaired: {e}") from e

        # Minimal schema validation
        #_validate_min_schema(data)

        return data


####################################################################################################################################

# read "./event_spreadsheets/US_bank_reputation_cases_2015_2025_ChatGPT_enhanced.xlsx" into a pandas dataframe called events_chatGPT_df

import pandas as pd

# Read the Excel file into a pandas dataframe
events_chatGPT_df = pd.read_excel("./event_spreadsheets/US_bank_reputation_cases_2015_2025_ChatGPT_enhanced.xlsx")

# Display basic information about the dataframe
print(f"Dataframe shape: {events_chatGPT_df.shape}")
print(f"Columns: {list(events_chatGPT_df.columns)}")
print("\nFirst few rows:")
print(events_chatGPT_df.head())


# Read the Excel file into a pandas dataframe
events_claude_df = pd.read_excel("./event_spreadsheets/US_bank_Reputational_Damage_2015_2025 - Claude.xlsx")

# Display basic information about the dataframe
print(f"Dataframe shape: {events_claude_df.shape}")
print(f"Columns: {list(events_claude_df.columns)}")
print("\nFirst few rows:")
print(events_claude_df.head())

iRow = 0
bank_name = events_claude_df.iloc[iRow]["Bank"]
year = events_claude_df.iloc[iRow]["Case Name + Year"][-4:]
case_study_summary = events_claude_df.iloc[iRow]["Event Summary"]

print("="*100)
print("Searching for media articles about the event...")
result = fetch_bank_event_media_coverage(bank_name, year, case_study_summary)
print(json.dumps(result, indent=2))
print("="*100)
print("Completed")







