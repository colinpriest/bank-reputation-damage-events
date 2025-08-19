## Bank Reputation Damage Events

A Python tool to collect and structure reputation-damaging events involving banks operating in the USA, using Perplexity's API. Exports clean tables to Excel for analysis and reporting.

### Features
- Structured JSON retrieval from Perplexity for a given month or a full historical sweep
- Rich event taxonomy (regulatory/legal, operational, conduct/ethics, ESG/social, customer/market, third‑party, governance, financial performance, brand & marketing)
- Materiality scoring guidance (includes social sentiment, boycott/press criteria)
- Clean pandas tables and Excel exports with helpful summaries

## Setup

### 1) Python environment
```bash
pip install -r requirements.txt
```

If you use conda:
```bash
conda create -n bankrep python=3.11 -y
conda activate bankrep
pip install -r requirements.txt
```

### 2) Environment variables
Create a `.env` file in the project root (already git-ignored) and add your keys:
```bash
PERPLEXITY_API_KEY=sk-...
# Optional
OPENAI_API_KEY=sk-...
```
Note: The code will also look for `PPLX_API_KEY` as a fallback. The `.env` file is excluded from Git via `.gitignore`.

## Usage

### A) Run the full collection (default main)
This collects monthly results from January 2000 through July 2025 and writes two Excel files.
```bash
python collect_event_data.py
```
Outputs:
- `bank_reputation_monthly_JSON.xlsx`: one row per month with raw JSON output
- `bank_reputation_events.xlsx`: one row per event across all months

Adjust the collection window by editing the `collect_all_bank_events(...)` call in `collect_event_data.py` (change `start_year`, `start_month`, `end_year`, `end_month`, and `delay_seconds`).

### B) Fetch a single month programmatically
```python
from collect_event_data import fetch_negative_bank_events

data = fetch_negative_bank_events(2025, 4, model="sonar-pro")
```

### C) Convert JSON to a flat table
```python
from collect_event_data import json_to_bank_events_table

df = json_to_bank_events_table(data)
```

### D) Save a table to Excel
```python
from collect_event_data import save_tables_to_excel
# or use save_to_excel(df, filename) for a single sheet
```

## Troubleshooting
- Missing API key: ensure `PERPLEXITY_API_KEY` (or `PPLX_API_KEY`) is set in your environment or `.env`.
- Character detection warning from `requests`: `charset-normalizer` is included in `requirements.txt`; install via `pip install -r requirements.txt`.
- Excel timezone error: the code converts datetime fields to timezone‑naive before writing. If you add new datetime fields, localize with `.dt.tz_localize(None)`.
- Rate limiting: increase `delay_seconds` in `collect_all_bank_events(...)`.

## Notes
- The prompt includes expanded categories: Financial Performance and Brand & Marketing.
- `.env` is excluded from Git. Never commit API keys.
