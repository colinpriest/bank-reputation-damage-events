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


def _last_day_of_month(year: int, month: int) -> int:
    """Return the last day number (28-31) for the given month/year."""
    return calendar.monthrange(year, month)[1]


def _build_prompt(year: int, month: int) -> str:
    """
    Build the strict JSON-only research prompt, parameterized by the given month/year.
    The prompt enforces scope, sourcing, de-duplication, and output schema.
    """
    start_date = f"{year:04d}-{month:02d}-01"
    end_date = f"{year:04d}-{month:02d}-{_last_day_of_month(year, month):02d}"

    # Important: Keep this aligned with the expected output schema the caller will validate.
    return f"""
You are a meticulous research assistant. Search credible, publicly verifiable sources and return ONLY valid UTF-8 JSON (no prose, no markdown). Your job is to list every major negative event involving banks operating in the USA during {start_date}–{end_date} inclusive.

## Scope & definitions
- "Banks operating in the USA" includes U.S.-chartered banks and foreign banks with U.S. operations/subsidiaries (e.g., branches, broker-dealers, or bank holding companies). Include credit unions only if a U.S. federal regulator (NCUA, CFPB, DOJ, etc.) took action that received national coverage.
- "Major negative event" includes: 
  * **Regulatory/Legal**: regulatory actions (orders, consent orders, MRAs), formal enforcement, lawsuits (significant, filed/settled), fines/penalties, sanctions/AML/BSA failures, investigations announced or concluded
  * **Operational**: customer data breaches (material), fraud (internal/external with bank exposure), operational outages materially affecting customers, major technology failures, cybersecurity incidents
  * **Conduct/Ethics**: scandals/misconduct, discriminatory practices, executive misconduct/resignation under controversy, predatory lending/product practices, market manipulation allegations
  * **ESG/Social**: environmental controversies (financing harmful projects, greenwashing), mass layoffs (>500 employees or >5% workforce), labor disputes/strikes, branch closure controversies affecting underserved communities
  * **Customer/Market**: systemic customer service failures with media coverage, viral social media incidents damaging reputation, product recalls or withdrawals, controversial fee changes
  * **Financial Performance**: bank failures or receivership, significant credit rating downgrades, or public declarations of a liquidity crisis
  * **Brand & Marketing**: widespread backlash to a marketing campaign or advertisement, mismanaged public relations crises, or a major brand identity controversy
  * **Third-party**: partner/vendor scandals reflecting on the bank, failed fintech partnerships, outsourcing failures
  * **Governance**: board controversies, shareholder activism with negative focus, proxy battle disputes, internal control weaknesses disclosed
- Exclude: routine earnings misses without an associated public controversy or investigation, and unconfirmed rumors. Vendor breaches without clear bank implication, events outside this month (unless reported within this month and squarely about conduct within this window), and purely positive ESG initiatives. Notable exception: a bank failure, while a financial event, is a major reputation event and should be included.

## Time & geography filters
- Event window: {start_date}T00:00:00 to {end_date}T23:59:59 America/New_York.
- Cover U.S. federal and state actions (DOJ, SEC, CFPB, OCC, FDIC, FRB, NYDFS, state AGs), and reputable national media.

## Sourcing rules
- Provide 2+ independent sources per event when available; prefer at least one regulator or court document if applicable.
- Capture each source's title, publisher, URL, and publication date (ISO 8601).
- If only a single primary source exists, return it and set "source_count" accordingly.

## Materiality heuristic (for `materiality_score`, 1–5)
- 5: ≥ USD 1B penalty, breach ≥ 5M customers, severe AML/sanctions failures with national attention, CEO/C-suite resignation under scandal, Congressional hearing/investigation, public calls for boycott or significant long-term negative press coverage.
- 4: USD 100M–<1B penalty, breach ≥ 1M, multiyear misconduct, senior management fallout, mass layoffs >5000 employees, major ESG controversy with boycott threats, or sustained long-term negative press coverage.
- 3: USD 10M–<100M penalty, breach ≥ 100k, significant outage with national coverage, workforce reduction >1000 employees, multiple state AG investigations, or sustained negative social media sentiment trend (e.g., >10,000 mentions in 24h with a net negative sentiment score).
- 2: USD 1M–<10M penalty, breach ≥ 10k, localized but widely reported harm, executive misconduct, controversial branch closures, fintech partnership failure, or sustained negative social media sentiment trend (e.g., >10,000 mentions in 24h with a net negative sentiment score).
- 1: Below the above thresholds but still broadly newsworthy, including investigation announcements, discrimination allegations, customer service failures trending on social media.

## De-duplication & normalization
- Merge duplicates across subsidiaries/parents; report the legal entity and parent (if applicable).
- Normalize currency to USD; also include the original amount string in "amounts.original_text".
- Use ISO dates. Summaries ≤ 80 words, factual and neutral.

## Output format (STRICT JSON ONLY)
Return a single JSON object with this exact structure:

{{
  "query": {{
    "timeframe": {{ "start": "{start_date}", "end": "{end_date}", "timezone": "America/New_York" }},
    "scope_note": "Banks operating in the USA; major negative events per definitions"
  }},
  "events": [
    {{
      "event_id": "kebab-case-unique-id",
      "title": "Concise event title",
      "institutions": ["Legal Entity Name"],
      "parent_company": "Parent Name or null",
      "us_operations": true,
      "jurisdictions": ["USA","State or District"],
      "categories": ["regulatory_action" | "lawsuit" | "fine" | "data_breach" | "fraud" | "operational_outage" | "discrimination" | "sanctions_aml" | "executive_misconduct" | "esg_controversy" | "labor_dispute" | "customer_service_crisis" | "technology_failure" | "partnership_failure" | "governance_issue" | "market_manipulation" | "predatory_practices" | "investigation" | "financial_performance" | "brand_marketing" | "other"],
      "event_date": "YYYY-MM-DD",
      "reported_dates": ["YYYY-MM-DD"],
      "summary": "≤80 words.",
      "reputational_damage": {{
        "nature": ["compliance_failure","customer_trust","governance","operational_resilience","data_security","fairness_discrimination","market_integrity","executive_conduct","environmental_social","labor_relations","product_controversy","partner_reputation","other"],
        "materiality_score": 1,
        "drivers": {{
          "fine_usd": 0,
          "customers_affected": null,
          "service_disruption_hours": null,
          "executive_changes": false,
          "litigation_status": "none|filed|settled|ongoing",
          "regulator_involved": ["OCC","FDIC","FRB","SEC","CFPB","DOJ","State AG","NYDFS","NCUA","Other"]
        }}
      }},
      "amounts": {{
        "penalties_usd": 0,
        "settlements_usd": 0,
        "other_amounts_usd": 0,
        "original_text": "verbatim amounts and currencies as reported, if any"
      }},
      "sources": [
        {{
          "title": "Source headline",
          "publisher": "Outlet/Regulator",
          "url": "https://...",
          "date_published": "YYYY-MM-DD",
          "source_type": "regulator|media|court"
        }}
      ],
      "source_count": 1,
      "confidence": "high|medium|low"
    }}
  ],
  "dedupe_note": "Describe any entity/source deduping performed.",
  "coverage_notes": "Call out notable exclusions or limited sourcing, if any.",
  "last_updated": "YYYY-MM-DDTHH:MM:SSZ"
}}

## Validation
- Output MUST be valid JSON and parse without errors.
- Do not include markdown, backticks, comments, or explanations—JSON only.
- If no qualifying events are found, return:
{{
  "query": {{
    "timeframe": {{ "start": "{start_date}", "end": "{end_date}", "timezone": "America/New_York" }},
    "scope_note": "Banks operating in the USA; major negative events per definitions"
  }},
  "events": [],
  "dedupe_note": "",
  "coverage_notes": "No qualifying events located in the month given the criteria.",
  "last_updated": "ISO timestamp"
}}
""".strip()


def _extract_json_block(text: str) -> str:
    """
    Attempts to extract the largest JSON object substring from a text blob.
    Useful if the model prepends/appends stray prose despite instructions.
    """
    # Greedy match from first "{" to last "}".
    m = re.search(r"\{.*\}", text, re.DOTALL)
    return m.group(0) if m else ""


def _validate_params(year: int, month: int) -> None:
    """Validate year and month inputs with informative errors."""
    if not isinstance(year, int) or not (1900 <= year <= 2100):
        raise ValueError("year must be an int between 1900 and 2100.")
    if not isinstance(month, int) or not (1 <= month <= 12):
        raise ValueError("month must be an int between 1 and 12.")


def _validate_min_schema(payload: Dict[str, Any]) -> None:
    """
    Minimal structural validation of the returned JSON.
    Raises JSONStructureError with a helpful message on failure.
    """
    required_top = ["query", "events", "dedupe_note", "coverage_notes", "last_updated"]
    for k in required_top:
        if k not in payload:
            raise JSONStructureError(f"Missing top-level key: '{k}'.")

    q = payload.get("query", {})
    if "timeframe" not in q or not isinstance(q["timeframe"], dict):
        raise JSONStructureError("query.timeframe missing or not an object.")
    tf = q["timeframe"]
    for dkey in ("start", "end", "timezone"):
        if dkey not in tf:
            raise JSONStructureError(f"query.timeframe.{dkey} is required.")

    if not isinstance(payload["events"], list):
        raise JSONStructureError("'events' must be a list.")

    # Spot-check event fields if events exist
    if payload["events"]:
        sample = payload["events"][0]
        for f in (
            "event_id",
            "title",
            "institutions",
            "categories",
            "event_date",
            "summary",
            "reputational_damage",
            "sources",
            "source_count",
            "confidence",
        ):
            if f not in sample:
                raise JSONStructureError(f"events[*].{f} is required (missing in first event).")
        if not isinstance(sample["sources"], list) or not sample["sources"]:
            raise JSONStructureError("events[*].sources must be a non-empty list.")


def fetch_negative_bank_events(
    year: int,
    month: int,
    *,
    model: str = "sonar-pro",
    timeout: int = 90,
    max_retries: int = 3,
    api_key: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Query Perplexity's Chat Completions API to retrieve structured JSON of major
    negative banking events in the USA for the given year/month.

    Parameters
    ----------
    year : int
        Four-digit year (1900–2100).
    month : int
        Month number (1–12).
    model : str, optional
        Perplexity model name (e.g., "sonar-pro", "sonar-reasoning", "sonar").
    timeout : int, optional
        Per-request timeout in seconds.
    max_retries : int, optional
        Number of automatic retries for transient 429/5xx responses.
    api_key : str, optional
        Perplexity API key. If None, reads from env var PERPLEXITY_API_KEY or PPLX_API_KEY.

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
    _validate_params(year, month)
    api_key = api_key or os.getenv("PERPLEXITY_API_KEY") or os.getenv("PPLX_API_KEY")
    if not api_key:
        raise ValueError("Missing API key. Set PERPLEXITY_API_KEY or PPLX_API_KEY, or pass api_key=...")

    prompt = _build_prompt(year, month)

    url = "https://api.perplexity.ai/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        # Perplexity is OpenAI-compatible; keep UA minimal & clear for traceability.
        "User-Agent": "research-client/1.0",
    }

    # We send both a System and a User message to maximize JSON-only compliance.
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": "You are a meticulous research assistant. Output valid JSON only."},
            {"role": "user", "content": prompt},
        ],
        # Zero temperature to reduce creative drift; keep defaults conservative.
        "temperature": 0,
        "top_p": 1,
        # REMOVED response_format as Perplexity doesn't support OpenAI's json_object type
        # The JSON-only instruction in the prompt should be sufficient
        # "response_format": {"type": "json_object"},  # <-- REMOVED THIS LINE
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

        # Try strict parse first; fall back to extracting a JSON block if needed.
        json_text = content
        try:
            data = json.loads(json_text)
        except json.JSONDecodeError:
            json_text = _extract_json_block(content)
            if not json_text:
                raise JSONStructureError(
                    "Model did not return JSON or a parsable JSON block. "
                    "Consider tightening the prompt."
                )
            try:
                data = json.loads(json_text)
            except json.JSONDecodeError as e:
                raise JSONStructureError(f"Returned content is not valid JSON: {e}") from e

        # Minimal schema validation
        _validate_min_schema(data)

        return data

import json
import pandas as pd
from typing import Dict, Any, List, Optional
from datetime import datetime

def json_to_bank_events_table(json_data: Dict[str, Any]) -> pd.DataFrame:
    """
    Convert the bank events JSON structure to a pandas DataFrame.
    
    Parameters
    ----------
    json_data : Dict[str, Any]
        The JSON data structure containing bank events information
    
    Returns
    -------
    pd.DataFrame
        A structured table with one row per event containing all relevant fields
    """
    
    # Extract metadata
    query_info = json_data.get("query", {})
    timeframe = query_info.get("timeframe", {})
    last_updated = json_data.get("last_updated", "")
    
    # Initialize list to store flattened records
    records = []
    
    # Process each event
    for event in json_data.get("events", []):
        record = {}
        
        # Query metadata (same for all events in a single query)
        record["query_start_date"] = timeframe.get("start", "")
        record["query_end_date"] = timeframe.get("end", "")
        record["query_timezone"] = timeframe.get("timezone", "")
        record["data_last_updated"] = last_updated
        
        # Event basic information
        record["event_id"] = event.get("event_id", "")
        record["title"] = event.get("title", "")
        record["event_date"] = event.get("event_date", "")
        
        # Institutions (join multiple with semicolon)
        institutions = event.get("institutions", [])
        record["institutions"] = "; ".join(institutions) if institutions else ""
        record["parent_company"] = event.get("parent_company", "")
        record["us_operations"] = event.get("us_operations", False)
        
        # Jurisdictions and categories (join multiple with semicolon)
        jurisdictions = event.get("jurisdictions", [])
        record["jurisdictions"] = "; ".join(jurisdictions) if jurisdictions else ""
        
        categories = event.get("categories", [])
        record["categories"] = "; ".join(categories) if categories else ""
        
        # Reported dates (join multiple with semicolon)
        reported_dates = event.get("reported_dates", [])
        record["reported_dates"] = "; ".join(reported_dates) if reported_dates else ""
        
        # Summary
        record["summary"] = event.get("summary", "")
        
        # Reputational damage
        rep_damage = event.get("reputational_damage", {})
        
        # Nature of damage (join multiple with semicolon)
        nature = rep_damage.get("nature", [])
        record["damage_nature"] = "; ".join(nature) if nature else ""
        record["materiality_score"] = rep_damage.get("materiality_score", 0)
        
        # Drivers
        drivers = rep_damage.get("drivers", {})
        record["fine_usd"] = drivers.get("fine_usd", 0)
        record["customers_affected"] = drivers.get("customers_affected", None)
        record["service_disruption_hours"] = drivers.get("service_disruption_hours", None)
        record["executive_changes"] = drivers.get("executive_changes", False)
        record["litigation_status"] = drivers.get("litigation_status", "")
        
        # Regulators (join multiple with semicolon)
        regulators = drivers.get("regulator_involved", [])
        record["regulators_involved"] = "; ".join(regulators) if regulators else ""
        
        # Amounts
        amounts = event.get("amounts", {})
        record["penalties_usd"] = amounts.get("penalties_usd", 0)
        record["settlements_usd"] = amounts.get("settlements_usd", 0)
        record["other_amounts_usd"] = amounts.get("other_amounts_usd", 0)
        record["amounts_original_text"] = amounts.get("original_text", "")
        
        # Calculate total financial impact
        record["total_financial_impact_usd"] = (
            record["penalties_usd"] + 
            record["settlements_usd"] + 
            record["other_amounts_usd"]
        )
        
        # Sources information
        sources = event.get("sources", [])
        record["source_count"] = event.get("source_count", 0)
        
        # Aggregate source information
        if sources:
            # Primary source (first one)
            primary_source = sources[0]
            record["primary_source_title"] = primary_source.get("title", "")
            record["primary_source_publisher"] = primary_source.get("publisher", "")
            record["primary_source_url"] = primary_source.get("url", "")
            record["primary_source_date"] = primary_source.get("date_published", "")
            record["primary_source_type"] = primary_source.get("source_type", "")
            
            # All source URLs (for reference)
            all_urls = [s.get("url", "") for s in sources]
            record["all_source_urls"] = "; ".join(all_urls)
            
            # Source types present
            source_types = list(set([s.get("source_type", "") for s in sources if s.get("source_type")]))
            record["source_types"] = "; ".join(source_types)
        else:
            record["primary_source_title"] = ""
            record["primary_source_publisher"] = ""
            record["primary_source_url"] = ""
            record["primary_source_date"] = ""
            record["primary_source_type"] = ""
            record["all_source_urls"] = ""
            record["source_types"] = ""
        
        # Confidence
        record["confidence"] = event.get("confidence", "")
        
        # Add record to list
        records.append(record)
    
    # Create DataFrame
    df = pd.DataFrame(records)
    
    # If no events, create empty DataFrame with all columns
    if len(records) == 0:
        columns = [
            "query_start_date", "query_end_date", "query_timezone", "data_last_updated",
            "event_id", "title", "event_date", "institutions", "parent_company", 
            "us_operations", "jurisdictions", "categories", "reported_dates", "summary",
            "damage_nature", "materiality_score", "fine_usd", "customers_affected",
            "service_disruption_hours", "executive_changes", "litigation_status",
            "regulators_involved", "penalties_usd", "settlements_usd", "other_amounts_usd",
            "amounts_original_text", "total_financial_impact_usd", "source_count",
            "primary_source_title", "primary_source_publisher", "primary_source_url",
            "primary_source_date", "primary_source_type", "all_source_urls", 
            "source_types", "confidence"
        ]
        df = pd.DataFrame(columns=columns)
    
    # Convert date columns to datetime (timezone-naive for Excel compatibility)
    date_columns = ["query_start_date", "query_end_date", "event_date", "primary_source_date"]
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.tz_localize(None)
    
    # Convert last_updated to datetime (timezone-naive for Excel compatibility)
    if "data_last_updated" in df.columns:
        df["data_last_updated"] = pd.to_datetime(df["data_last_updated"], errors='coerce').dt.tz_localize(None)
    
    return df


def save_to_excel(df: pd.DataFrame, filename: str = "bank_events.xlsx") -> None:
    """
    Save the DataFrame to an Excel file with formatting.
    
    Parameters
    ----------
    df : pd.DataFrame
        The bank events DataFrame
    filename : str
        Output filename (default: "bank_events.xlsx")
    """
    with pd.ExcelWriter(filename, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='Bank Events', index=False)
        
        # Auto-adjust column widths
        worksheet = writer.sheets['Bank Events']
        for column in df:
            column_width = max(df[column].astype(str).map(len).max(), len(column))
            column_width = min(column_width, 50)  # Cap at 50 characters
            col_idx = df.columns.get_loc(column)
            worksheet.column_dimensions[chr(65 + col_idx % 26)].width = column_width + 2

def print_summary_stats(df: pd.DataFrame) -> None:
    """
    Print summary statistics about the bank events data.
    
    Parameters
    ----------
    df : pd.DataFrame
        The bank events DataFrame
    """
    print("\n=== BANK EVENTS SUMMARY STATISTICS ===\n")
    print(f"Total events: {len(df)}")
    
    if len(df) > 0:
        print(f"\nDate range: {df['event_date'].min()} to {df['event_date'].max()}")
        
        print(f"\n--- Financial Impact ---")
        print(f"Total penalties: ${df['penalties_usd'].sum():,.2f}")
        print(f"Total settlements: ${df['settlements_usd'].sum():,.2f}")
        print(f"Total other amounts: ${df['other_amounts_usd'].sum():,.2f}")
        print(f"Total financial impact: ${df['total_financial_impact_usd'].sum():,.2f}")
        
        print(f"\n--- Materiality Distribution ---")
        materiality_counts = df['materiality_score'].value_counts().sort_index()
        for score, count in materiality_counts.items():
            print(f"  Score {score}: {count} event(s)")
        
        print(f"\n--- Categories ---")
        all_categories = []
        for cat_str in df['categories']:
            if cat_str:
                all_categories.extend(cat_str.split("; "))
        category_counts = pd.Series(all_categories).value_counts()
        for category, count in category_counts.items():
            print(f"  {category}: {count}")
        
        print(f"\n--- Regulators Involved ---")
        all_regulators = []
        for reg_str in df['regulators_involved']:
            if reg_str:
                all_regulators.extend(reg_str.split("; "))
        regulator_counts = pd.Series(all_regulators).value_counts()
        for regulator, count in regulator_counts.items():
            print(f"  {regulator}: {count}")
        
        print(f"\n--- Litigation Status ---")
        litigation_counts = df['litigation_status'].value_counts()
        for status, count in litigation_counts.items():
            if status:  # Skip empty strings
                print(f"  {status}: {count}")
                    
import json
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any
import time

def collect_all_bank_events(start_year: int = 2000, start_month: int = 1,
                           end_year: int = 2025, end_month: int = 7,
                           model: str = "sonar-pro",
                           delay_seconds: float = 1.0) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Collect bank events for all months from start to end date.
    
    Args:
        start_year: Starting year (default 2000)
        start_month: Starting month (default 1 for January)
        end_year: Ending year (default 2025)
        end_month: Ending month (default 7 for July)
        model: Perplexity model to use
        delay_seconds: Delay between API calls to avoid rate limiting
    
    Returns:
        Tuple of (monthly_df, events_df)
        - monthly_df: DataFrame with year, month, and JSON output columns
        - events_df: DataFrame with one row per event
    """
    
    # Initialize lists to store data
    monthly_data = []
    all_events = []
    
    # Generate list of year-month pairs to iterate through
    months_to_process = []
    current_year = start_year
    current_month = start_month
    
    while (current_year < end_year) or (current_year == end_year and current_month <= end_month):
        months_to_process.append((current_year, current_month))
        current_month += 1
        if current_month > 12:
            current_month = 1
            current_year += 1
    
    total_months = len(months_to_process)
    print(f"Processing {total_months} months from {start_year}-{start_month:02d} to {end_year}-{end_month:02d}")
    print("=" * 60)
    
    # Process each month
    for idx, (year, month) in enumerate(months_to_process, 1):
        print(f"Processing {year}-{month:02d} ({idx}/{total_months})...", end=" ")
        
        try:
            # Fetch data for current month
            result_json = fetch_negative_bank_events(year, month, model=model)
            
            # Store monthly JSON data
            monthly_data.append({
                'year': year,
                'month': month,
                'json_output': json.dumps(result_json)  # Store as JSON string
            })
            
            # Convert to events DataFrame and add to all_events
            month_events_df = json_to_bank_events_table(result_json)
            if not month_events_df.empty:
                # Add year and month columns for tracking
                month_events_df['year'] = year
                month_events_df['month'] = month
                all_events.append(month_events_df)
            
            print(f"✓ Found {len(month_events_df)} events")
            
            # Add delay between API calls to avoid rate limiting
            if idx < total_months:  # Don't delay after the last request
                time.sleep(delay_seconds)
                
        except (ValueError, PerplexityAPIError, JSONStructureError) as err:
            print(f"✗ Error: {err}")
            # Store empty/error entry for this month
            monthly_data.append({
                'year': year,
                'month': month,
                'json_output': json.dumps({"error": str(err), "events": []})
            })
        except Exception as err:
            print(f"✗ Unexpected error: {err}")
            monthly_data.append({
                'year': year,
                'month': month,
                'json_output': json.dumps({"error": str(err), "events": []})
            })
    
    print("=" * 60)
    print("Data collection complete!")
    
    # Create monthly DataFrame
    monthly_df = pd.DataFrame(monthly_data)
    
    # Create events DataFrame
    if all_events:
        events_df = pd.concat(all_events, ignore_index=True)
        # Reorder columns to put year and month first
        cols = ['year', 'month'] + [col for col in events_df.columns if col not in ['year', 'month']]
        events_df = events_df[cols]
    else:
        # Create empty DataFrame with expected columns if no events found
        events_df = pd.DataFrame(columns=['year', 'month', 'date', 'bank', 'event_type', 
                                         'description', 'severity', 'link'])
    
    return monthly_df, events_df


def save_tables_to_excel(monthly_df: pd.DataFrame, events_df: pd.DataFrame,
                         monthly_filename: str = "bank_reputation_monthly_JSON.xlsx",
                         events_filename: str = "bank_reputation_events.xlsx"):
    """
    Save the monthly and events DataFrames to Excel files.
    
    Args:
        monthly_df: DataFrame with monthly JSON data
        events_df: DataFrame with individual events
        monthly_filename: Filename for monthly data Excel file
        events_filename: Filename for events Excel file
    """
    
    # Save monthly table
    print(f"\nSaving monthly data to {monthly_filename}...")
    with pd.ExcelWriter(monthly_filename, engine='openpyxl') as writer:
        monthly_df.to_excel(writer, sheet_name='Monthly_JSON', index=False)
        
        # Add a summary sheet
        summary_df = pd.DataFrame({
            'Metric': ['Total Months', 'Start Date', 'End Date', 'Total JSON Records'],
            'Value': [
                len(monthly_df),
                f"{monthly_df.iloc[0]['year']}-{monthly_df.iloc[0]['month']:02d}",
                f"{monthly_df.iloc[-1]['year']}-{monthly_df.iloc[-1]['month']:02d}",
                len(monthly_df)
            ]
        })
        summary_df.to_excel(writer, sheet_name='Summary', index=False)
    
    print(f"✓ Monthly data saved: {len(monthly_df)} months")
    
    # Save events table
    print(f"\nSaving events data to {events_filename}...")
    with pd.ExcelWriter(events_filename, engine='openpyxl') as writer:
        events_df.to_excel(writer, sheet_name='Events', index=False)
        
        # Add summary statistics sheet
        if not events_df.empty:
            summary_stats = pd.DataFrame({
                'Metric': [
                    'Total Events',
                    'Unique Banks',
                    'Date Range',
                    'Most Common Event Type',
                    'Average Severity'
                ],
                'Value': [
                    len(events_df),
                    events_df['bank'].nunique() if 'bank' in events_df.columns else 0,
                    f"{events_df['date'].min()} to {events_df['date'].max()}" if 'date' in events_df.columns else "N/A",
                    events_df['event_type'].mode()[0] if 'event_type' in events_df.columns and not events_df.empty else "N/A",
                    f"{events_df['severity'].mean():.2f}" if 'severity' in events_df.columns else "N/A"
                ]
            })
            summary_stats.to_excel(writer, sheet_name='Summary', index=False)
    
    print(f"✓ Events data saved: {len(events_df)} total events")


def print_collection_summary(monthly_df: pd.DataFrame, events_df: pd.DataFrame):
    """
    Print summary statistics for the collected data.
    
    Args:
        monthly_df: DataFrame with monthly JSON data
        events_df: DataFrame with individual events
    """
    print("\n" + "=" * 60)
    print("COLLECTION SUMMARY")
    print("=" * 60)
    
    print(f"\nMonthly Table:")
    print(f"  - Total months processed: {len(monthly_df)}")
    print(f"  - Date range: {monthly_df.iloc[0]['year']}-{monthly_df.iloc[0]['month']:02d} to "
          f"{monthly_df.iloc[-1]['year']}-{monthly_df.iloc[-1]['month']:02d}")
    
    print(f"\nEvents Table:")
    print(f"  - Total events: {len(events_df)}")
    if not events_df.empty and 'bank' in events_df.columns:
        print(f"  - Unique banks: {events_df['bank'].nunique()}")
    if not events_df.empty and 'event_type' in events_df.columns:
        print(f"  - Event types: {events_df['event_type'].nunique()}")
        print("\n  Top 5 Event Types:")
        for event_type, count in events_df['event_type'].value_counts().head().items():
            print(f"    • {event_type}: {count}")
    if not events_df.empty and 'severity' in events_df.columns:
        print(f"\n  Severity Statistics:")
        print(f"    • Mean: {events_df['severity'].mean():.2f}")
        print(f"    • Min: {events_df['severity'].min()}")
        print(f"    • Max: {events_df['severity'].max()}")


if __name__ == "__main__":
    # Set environment variable before running:
    # PowerShell: $env:PERPLEXITY_API_KEY = "sk-..."
    # Bash: export PERPLEXITY_API_KEY="sk-..."
    
    try:
        # Collect all data from January 2000 to July 2025
        print("Starting data collection...")
        print("This may take a while due to API rate limits.\n")
        
        monthly_df, events_df = collect_all_bank_events(
            start_year=2000, 
            start_month=1,
            end_year=2025, 
            end_month=7,
            model="sonar-pro",
            delay_seconds=1.0  # Adjust based on API rate limits
        )
        
        # Print summary
        print_collection_summary(monthly_df, events_df)
        
        # Save to Excel files
        save_tables_to_excel(
            monthly_df, 
            events_df,
            monthly_filename="bank_reputation_monthly_JSON.xlsx",
            events_filename="bank_reputation_events.xlsx"
        )
        
        print("\n✓ Process completed successfully!")
        
    except Exception as err:
        print(f"Fatal error: {err}")
        import traceback
        traceback.print_exc()