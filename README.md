# Workday Job Scraper

A robust Python tool for scraping job listings from Workday-powered job sites, featuring optional BERT-based semantic search for intelligent job matching. This scraper can search across multiple companies, filter results by location and posting date, and save results to a centralized JSON file.

## Features

- **Optional BERT-based Semantic Search**: Toggle between BERT embeddings or simple keyword matching for job relevance scoring
- **Parallel and Sequential Processing Options**: Choose between faster parallel processing or more memory-efficient sequential processing
- **Real-time Parallel Processing**: Jobs are processed and scored in parallel as they are found
- **Robots.txt Compliance**: Automatically checks and respects robots.txt directives before scraping
- **Consolidated Results**: All jobs are automatically consolidated into a single file sorted by relevance score
- **Comprehensive Term Mappings**: Rich domain-specific vocabulary mappings for AI/ML fields
- **Intelligent Term Expansion**: Automatically expands search terms using domain-specific mappings
- **Unified Scraping**: Process multiple companies from a single configuration
- **Duplicate Prevention**: Automatically deduplicates jobs across companies
- **Time-based Filtering**: Filter jobs based on posting date
- **Location-based Filtering**: Option to filter for US-based jobs only

## Installation

1. Clone this repository
2. Install required packages:
   ```bash
   pip install -r requirements.txt
   ```

## Requirements

- Python 3.7+
- CUDA-capable GPU (optional, for faster BERT processing)
- Required Python packages are listed in requirements.txt

## Usage

### Available Scripts

This project includes two versions of the scraper:

- **scrape_jobs.py**: Parallel version that processes multiple companies simultaneously (faster but uses more memory)
- **scrape_jobs_sequential.py**: Sequential version that processes one company at a time (slower but more memory-efficient)

### Basic Usage

```bash
# Run the parallel version
python scrape_jobs.py

# Run the sequential version
python scrape_jobs_sequential.py
```

### Command Line Options

```
JOB_RESULT_PATH=./path     Path to store job results
LAST_PROCESSED_PATH=./path Path to store last processed information
EXTRACT_START=N            Starting index for extraction
EXTRACT_END=N              Ending index for extraction
```

Example:
```bash
python scrape_jobs.py JOB_RESULT_PATH=./my_jobs EXTRACT_START=0 EXTRACT_END=10
```

### Configuration

Create a `config.json` file with the following structure:

```json
{
  "JOB_RESULT_PATH": "./workday_jobs",
  "LAST_PROCESSED_PATH": "./last_processed",
  "EXTRACT_START": 0,
  "EXTRACT_END": 100,
  "DAYS_LOOKBACK": 5,
  "FILTER_US_ONLY": false,
  "MIN_RELEVANCE_SCORE": 0.5,
  "USE_BERT": true,
  "NUM_THREADS": 2,
  "MAX_RAM_PERCENT": 50,
  "CHECK_ROBOTS_TXT": true,
  "ROBOTS_TXT_USER_AGENT": "WorkdayJobScraper",
  "SEARCH_TERMS": [
    "Artificial Intelligence",
    "Machine Learning",
    "Deep Learning",
    "Applied AI"
  ],
  "term_mappings": {
    "ai": "artificial intelligence AI machine learning deep learning...",
    "ml": "machine learning ML deep learning neural networks...",
    "deep_learning": "neural networks CNN RNN LSTM...",
    // ... other mappings
  }
}
```

### Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| JOB_RESULT_PATH | string | "./workday_jobs" | Path to store job results |
| LAST_PROCESSED_PATH | string | "./last_processed" | Path to store last processed information |
| EXTRACT_START | integer | 0 | Starting index for extraction |
| EXTRACT_END | integer | 100 | Ending index for extraction |
| DAYS_LOOKBACK | integer | 7 | Number of days to look back for jobs |
| FILTER_US_ONLY | boolean | false | Filter for US-based jobs only |
| MIN_RELEVANCE_SCORE | float | 0.5 | Minimum relevance score for jobs |
| USE_BERT | boolean | true | Use BERT for semantic matching |
| NUM_THREADS | integer | CPU count | Number of threads for parallel processing |
| MAX_RAM_PERCENT | integer | 80 | Maximum RAM usage percentage |
| CHECK_ROBOTS_TXT | boolean | true | Whether to check and respect robots.txt |
| ROBOTS_TXT_USER_AGENT | string | "WorkdayJobScraper" | User agent for robots.txt requests |

## How It Works

1. **Initialization**:
   - Loads configuration from config.json
   - Sets up job processor with parallel processing queue
   - Initializes BERT model if USE_BERT is true

2. **Job Processing Pipeline**:
   - **Pre-scraping Checks**:
     - Checks robots.txt for each domain
     - Respects crawl directives and rate limits
     - Caches robots.txt results for efficiency

   - **Scraping Phase**:
     - Scrapes jobs from Workday sites (if allowed by robots.txt)
     - Applies basic filters (location, date)
     - Checks for duplicates
     - Adds valid jobs to processing queue

   - **Processing Phase** (runs in parallel):
     - If USE_BERT is true:
       - Jobs are processed by BERT in a separate thread
       - Calculates semantic similarity scores
     - If USE_BERT is false:
       - Uses simple keyword matching
       - Scores based on term presence in title/description
     - Jobs meeting MIN_RELEVANCE_SCORE are saved

   - **Storage Phase**:
     - Each company has its own JSON file
     - All jobs are consolidated into a single file sorted by relevance score
     - Located at `{JOB_RESULT_PATH}/consolidated_results.json`

3. **Relevance Scoring**:
   - **BERT Mode** (USE_BERT=true):
     - Uses BERT embeddings for semantic understanding
     - Title matches weighted 2x more than description
     - Expands search terms using domain mappings
     - Score range: 0.0-1.0 (semantic similarity)

   - **Simple Mode** (USE_BERT=false):
     - Basic keyword matching in title/description
     - Title matches worth 0.6 points
     - Description matches worth 0.4 points
     - Score range: 0.0-1.0 (presence based)

4. **Output Format**:
   Jobs are saved in JSON format with metadata:
   ```json
   {
     "company_acronym": "COMPANY",
     "title": "Job Title",
     "description": "Full job description...",
     "internal_id": "COMPANY_ID",
     "location": "Location",
     "remote_type": "Remote/Onsite",
     "country": "Country",
     "posted_date": "YYYY-MM-DD",
     "url": "Job URL",
     "search_term": "Matching search term",
     "found_date": "YYYY-MM-DDTHH:MM:SS.SSSSSS",
     "relevance_score": 0.85
   }
   ```

## Parallel vs. Sequential Version

- **When to use the parallel version (scrape_jobs.py)**:
  - Faster execution time is a priority
  - You have a system with adequate RAM
  - You need to process a large number of companies
  
- **When to use the sequential version (scrape_jobs_sequential.py)**:
  - Memory efficiency is a priority
  - Running on a system with limited resources
  - Troubleshooting issues with the parallel version

## Robots.txt Compliance

The scraper includes built-in support for respecting robots.txt directives, which can be configured in `config.json`:

```json
{
  "CHECK_ROBOTS_TXT": true,
  "ROBOTS_TXT_USER_AGENT": "WorkdayJobScraper"
}
```

- **CHECK_ROBOTS_TXT**: Enable/disable robots.txt checking
  - Set to `true` to respect robots.txt directives (recommended)
  - Set to `false` to bypass robots.txt checking (use with caution)

- **ROBOTS_TXT_USER_AGENT**: Customize the user agent
  - Default: "WorkdayJobScraper"
  - Used to match rules in robots.txt files
  - Some sites may have different rules for different user agents

When robots.txt checking is enabled:
- The scraper checks robots.txt before accessing any domain
- Caches results to minimize requests
- Skips domains that disallow scraping
- Logs all robots.txt related decisions

## Troubleshooting

- **Low Match Quality**: Try:
  - Enabling BERT mode (USE_BERT=true)
  - Adding more terms to term_mappings
  - Lowering MIN_RELEVANCE_SCORE
  - Using more specific search terms

- **Performance Issues**: Consider:
  - Disabling BERT (USE_BERT=false) for faster processing
  - Using GPU acceleration for BERT
  - Adjusting DAYS_LOOKBACK
  - Focusing on specific companies (EXTRACT_START/END)

- **Memory Usage**: If memory usage is too high:
  - Switch to the sequential version (scrape_jobs_sequential.py)
  - Disable BERT to use simple matching
  - Process fewer companies at once (EXTRACT_START/END)
  - Reduce DAYS_LOOKBACK
  - Adjust MAX_RAM_PERCENT in config.json

- **Access Denied Issues**: Check:
  - robots.txt permissions
  - Rate limiting settings
  - Network/proxy configuration
  - User agent restrictions

## License

[MIT License](LICENSE) 