# Bank Reputation Damage Events Collection System

A comprehensive, object-oriented Python system for detecting, normalizing, and storing U.S. banking reputation-damage events from multiple authoritative sources including regulatory actions, lawsuits, data breaches, failures, and PR crises.

## 🏗️ Architecture

The system follows a modular, object-oriented architecture with clear separation of concerns:

```
bank-reputation-damage-events/
├── ingestion/
│   ├── connectors/          # Data source connectors
│   │   ├── base.py         # Base connector interface
│   │   ├── fdic_edo.py     # FDIC Enforcement Decisions & Orders
│   │   ├── occ_enforcement.py # OCC Enforcement Actions
│   │   ├── ffiec_bankfind.py  # BankFind Suite API
│   │   └── ...             # Additional connectors
│   ├── normalizers/
│   │   ├── events_model.py # Pydantic data models
│   │   └── mappings.py     # Category & nature mappings
│   └── orchestrators/
│       └── scheduler.py    # Collection orchestration
├── storage/
│   └── repository.py       # Data persistence layer
├── tests/                  # Test suite
├── main.py                 # Main application
├── collect_event_data.py   # Legacy Perplexity integration
└── requirements.txt        # Dependencies
```

## 🎯 Key Features

- **Multi-Source Integration**: 10+ authoritative data sources
- **Object-Oriented Design**: Clean, extensible connector architecture
- **Data Normalization**: Consistent event schema across all sources
- **Institution Enrichment**: BankFind Suite integration for entity resolution
- **Materiality Scoring**: Automated severity assessment (1-5 scale)
- **Idempotent Storage**: SQLite-based repository with upsert logic
- **Structured Logging**: Comprehensive observability
- **Async Processing**: High-performance HTTP client with retry logic

## 📊 Data Sources

### Implemented Sources

1. **FDIC Enforcement Decisions & Orders (ED&O)**
   - Monthly updated database of enforcement decisions
   - PDF parsing for penalty extraction
   - External IDs: Order/Notice numbers

2. **OCC Enforcement Actions & EASearch**
   - Monthly news releases and searchable database
   - Subject-matter tagging since 2012
   - External IDs: EA numbers (e.g., "AA-ENF-2025-13")

3. **FFIEC/FDIC BankFind Suite**
   - Institution identity and crosswalk
   - CERT, RSSD ID, LEI resolution
   - Historical names and mergers data

### Planned Sources

4. **Federal Reserve Board (FRB) Enforcement**
5. **FRB Legal "Orders Issued" (M&A, IBA, BHC)**
6. **FDIC Failed Bank List & "Bank Failures in Brief"**
7. **NY State Dept. of Financial Services (NYDFS)**
8. **State Data Breach Portals (CA & ME)**
9. **CourtListener / RECAP (federal litigation)**
10. **CFPB Enforcement Actions**

## 🚀 Quick Start

### Installation

```bash
# Clone the repository
git clone <repository-url>
cd bank-reputation-damage-events

# Install dependencies
pip install -r requirements.txt

# Set up environment variables
cp env.example .env
# Edit .env file with your API keys
```

### API Key Setup

**Required: FDIC BankFind Suite API Key**

This system uses the FDIC BankFind Suite API for institution enrichment and data access. You need to:

1. **Get an API Key**: Visit [https://banks.data.fdic.gov/developers/](https://banks.data.fdic.gov/developers/) and request an API key
2. **Create .env file**: Copy `env.example` to `.env` and add your API key:
   ```bash
   cp env.example .env
   ```
3. **Add your API key**: Edit the `.env` file and replace `your-fdic-api-key-here` with your actual API key:
   ```
   FDIC_API_KEY=your-actual-fdic-api-key
   ```

**Note**: Without the FDIC API key, institution enrichment will be limited and some features may not work properly.

**Optional: NewsAPI.org API Key**

For financial news monitoring and bank reputation event detection:

1. **Get an API Key**: Visit [https://newsapi.org/register](https://newsapi.org/register) and sign up for a free account
2. **Add to .env file**: Add your NewsAPI key to the `.env` file:
   ```
   NEWS_API_KEY=your-actual-newsapi-key
   ```

**Note**: The NewsAPI key enables monitoring of financial news for bank reputation events, regulatory actions, fines, and scandals.

**Optional: MediaStack API Key**

For additional financial news monitoring with broader coverage:

1. **Get an API Key**: Visit [https://mediastack.com/](https://mediastack.com/) and sign up for a free account
2. **Add to .env file**: Add your MediaStack key to the `.env` file:
   ```
   MEDIASTACK_API_KEY=your-actual-mediastack-key
   ```

**Note**: MediaStack provides additional news sources and can complement NewsAPI for more comprehensive coverage.

**Optional: OpenAI API Key**

For advanced news article analysis and reputation risk assessment:

1. **Get an API Key**: Visit [https://platform.openai.com/api-keys](https://platform.openai.com/api-keys) and create an account
2. **Add to .env file**: Add your OpenAI key to the `.env` file:
   ```
   OPENAI_API_KEY=your-actual-openai-key
   ```

**Note**: The OpenAI key enables ChatGPT-4o-mini analysis of news articles, including sentiment analysis, categorization, and reputation risk assessment.

**Optional: Advanced PDF Generation**

For high-quality PDF generation of scraped articles using Playwright (handles JavaScript, CSS, modern web pages):

1. **Install Playwright browsers**:
```bash
playwright install chromium
```

2. **Install Python dependencies**:
```bash
pip install playwright
```

**Features**:
- Renders JavaScript and dynamic content
- Handles modern CSS layouts
- Professional PDF formatting (A4, margins, backgrounds)
- Waits for page content to fully load
- Consistent rendering across different websites

**Note**: PDF generation is optional and the system will work without it. Playwright provides much better rendering than traditional HTML-to-PDF converters.

### Advanced News Analysis

The system includes a comprehensive news analysis module (`mediastack_search.py`) that provides:

- **MediaStackSearch Class**: Searches MediaStack API for banking news with filtering
- **NewsArticle Class**: Represents individual news articles with scraping and analysis capabilities
- **Automated Analysis**: Uses ChatGPT-4o-mini to analyze articles for:
  - Sentiment analysis (wording and factual content)
  - Event categorization (regulatory/legal, operational, conduct/ethics, etc.)
  - Stakeholder impact assessment
  - Reputation management steps identification
  - Specific event detection (optional parameter)

#### Example Usage:

```python
from mediastack_search import MediaStackSearch
from datetime import date, timedelta

# Initialize search
searcher = MediaStackSearch()

# Search for articles about a specific bank
success = await searcher.search(
    search_phrase="Wells Fargo",
    start_date=date.today() - timedelta(days=30),
    end_date=date.today(),
    exclude_keywords=["earnings", "quarterly", "profit"]
)

if success:
    print(f"Found {searcher.article_count()} articles")
    
    # Scrape and analyze all articles
    await searcher.scrape_all_articles()
    
    # Analyze all articles (with optional specific event)
    await searcher.analyse_all_articles("Wells Fargo", "regulatory fine")
    
    # Export results
    searcher.export_results("wells_fargo_analysis.json")
```

### Basic Usage

```python
import asyncio
from storage.repository import EventRepository
from ingestion.orchestrators.scheduler import EventScheduler

async def main():
    # Initialize repository and scheduler
    repository = EventRepository("bank_events.db")
    scheduler = EventScheduler(repository)
    
    # Run daily collection
    results = await scheduler.run_daily_collection()
    print(f"Collected {results['total_events']} events")
    
    # Run specific connector
    fdic_results = await scheduler.run_connector('fdic_edo', since=date.today() - timedelta(days=30))
    print(f"FDIC events: {fdic_results['events_stored']}")
    
    # Get statistics
    stats = scheduler.get_statistics()
    print(f"Total events in database: {stats['database']['total_events']}")

# Run the application
asyncio.run(main())
```

### Command Line Usage

```bash
# Run the main application
python main.py

# Run specific examples
python -c "from main import run_single_connector_example; run_single_connector_example()"
python -c "from main import run_statistics_example; run_statistics_example()"
```

## 📋 Data Model

### Event Schema

```python
class Event(BaseModel):
    event_id: str                    # kebab-case unique identifier
    title: str                       # Event title
    institutions: List[str]          # Affected institutions
    parent_company: Optional[str]    # Parent company if applicable
    us_operations: bool = True       # US operations flag
    jurisdictions: List[str]         # Geographic jurisdictions
    categories: List[str]            # Event categories
    event_date: date                 # When the event occurred
    reported_dates: List[date]       # When it was reported
    summary: str                     # Brief description (≤480 chars)
    reputational_damage: ReputationalDamage  # Damage assessment
    amounts: Dict[str, Any]          # Financial amounts
    sources: List[SourceRef]         # Source references
    source_count: int                # Number of sources
    confidence: Literal["high","medium","low"]  # Confidence level
```

### Categories

- `regulatory_action` - Consent orders, cease & desist
- `fine` - Civil money penalties
- `data_breach` - Security incidents
- `lawsuit` - Legal proceedings
- `financial_performance` - Bank failures
- `fraud` - Fraudulent activities
- `operational_outage` - Service disruptions
- `discrimination` - Fair lending violations
- `sanctions_aml` - BSA/AML violations
- `executive_misconduct` - Leadership issues
- And more...

### Materiality Scoring

- **5**: ≥$1B penalty, ≥5M customers affected, bank failure
- **4**: $100M-$1B penalty, ≥1M customers, mass layoffs
- **3**: $10M-$100M penalty, ≥100k customers, significant outages
- **2**: $1M-$10M penalty, ≥10k customers, executive misconduct
- **1**: Below thresholds but newsworthy

## 🔧 Configuration

### Environment Variables

The system uses environment variables for configuration. Create a `.env` file in the project root:

```bash
# Required: FDIC BankFind Suite API Key
# Get your key from: https://banks.data.fdic.gov/developers/
FDIC_API_KEY=your-fdic-api-key

# Optional: NewsAPI.org API Key (for financial news monitoring)
NEWS_API_KEY=your-newsapi-key

# Optional: MediaStack API Key (for additional financial news monitoring)
MEDIASTACK_API_KEY=your-mediastack-key

# Optional: CourtListener API Token (for future litigation data)
COURTLISTENER_TOKEN=your-courtlistener-token

# Optional: Logging level (DEBUG, INFO, WARNING, ERROR)
LOG_LEVEL=INFO
```

**Important**: The FDIC API key is required for:
- Institution enrichment and crosswalking
- BankFind Suite data access
- Historical institution data
- Failed bank information

Without this key, the system will still work but with limited institution enrichment capabilities.

### Schedule Configuration

The system supports flexible scheduling:

```python
schedules = {
    # Regulators: daily at 06:00 ET
    'fdic_edo': {'frequency': 'daily', 'time': '06:00', 'timezone': 'America/New_York'},
    'occ_enforcement': {'frequency': 'daily', 'time': '06:00', 'timezone': 'America/New_York'},
    
    # State breaches: daily at 07:00 PT/ET
    'state_breaches_ca': {'frequency': 'daily', 'time': '07:00', 'timezone': 'America/Los_Angeles'},
    
    # Bank failures: hourly on Fridays
    'fdic_failed_banks': {'frequency': 'friday_hourly', 'time': '09:00', 'timezone': 'America/New_York'},
}
```

## 🧪 Testing

```bash
# Run tests
python -m pytest tests/

# Run specific connector tests
python -m pytest tests/test_fdic_edo.py

# Run integration tests
python -m pytest tests/test_integration.py
```

## 📈 Monitoring & Observability

### Structured Logging

The system uses `structlog` for comprehensive logging:

```python
import structlog

logger = structlog.get_logger()
logger.info("Processing event", 
           event_id="fdic-123", 
           institution="Bank Name",
           penalty_amount=1000000)
```

### Health Checks

```python
# Run health check on all connectors
health = await scheduler.health_check()
print(f"Overall status: {health['overall_status']}")

# Check specific connector
connector_status = health['connectors']['fdic_edo']
print(f"FDIC status: {connector_status['status']}")
```

### Statistics

```python
# Get comprehensive statistics
stats = repository.get_statistics()
print(f"Total events: {stats['total_events']}")
print(f"Date range: {stats['date_range']}")
print(f"Category distribution: {stats['category_distribution']}")
```

## 🔄 Data Flow

1. **Discovery**: Connectors discover new items from data sources
2. **Fetch**: Detailed information is retrieved (HTML/PDF)
3. **Parse**: Raw data is parsed into structured format
4. **Normalize**: Data is mapped to standardized schema
5. **Enrich**: BankFind data is added for institution resolution
6. **Store**: Events are upserted to database with idempotency

## 🛡️ Security & Compliance

- **Rate Limiting**: Respects robots.txt and implements throttling
- **PII Protection**: No PII stored from breach letters
- **API Key Management**: Secure environment variable handling
- **Data Retention**: Configurable retention policies
- **Audit Trail**: Comprehensive logging for compliance

## 🚀 Deployment

### Docker

```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
CMD ["python", "main.py"]
```

### Cron Jobs

```bash
# Daily collection at 6 AM ET
0 6 * * * cd /path/to/app && python -c "import asyncio; from main import run_daily_collection; asyncio.run(run_daily_collection())"

# Monthly backfill on 3rd of month
0 2 3 * * cd /path/to/app && python -c "import asyncio; from main import run_monthly_backfill; asyncio.run(run_monthly_backfill(2025, 1))"
```

## 📚 API Reference

### EventRepository

```python
# Store events
repository.upsert_event(event)

# Query events
events = repository.get_events(
    start_date=date(2024, 1, 1),
    categories=['regulatory_action'],
    limit=100
)

# Get statistics
stats = repository.get_statistics()

# Institution management
repository.upsert_institution(cert="12345", name="Bank Name")
```

### EventScheduler

```python
# Run daily collection
results = await scheduler.run_daily_collection()

# Run monthly backfill
results = await scheduler.run_monthly_backfill(2024, 12)

# Run specific connector
results = await scheduler.run_connector('fdic_edo', since=date.today() - timedelta(days=7))

# Health check
health = await scheduler.health_check()
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Implement your changes
4. Add tests
5. Submit a pull request

### Adding New Connectors

1. Create a new connector class inheriting from `BaseConnector`
2. Implement the required abstract methods
3. Add the connector to the scheduler
4. Write tests
5. Update documentation

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

- FDIC for providing the BankFind Suite API
- All regulatory agencies for making enforcement data publicly available
- The open source community for the excellent libraries used in this project

## 📞 Support

For questions, issues, or contributions, please:

1. Check the documentation
2. Search existing issues
3. Create a new issue with detailed information
4. Contact the maintainers

---

**Note**: This system is designed for research and compliance purposes. Always verify data accuracy and consult official sources for critical decisions.
