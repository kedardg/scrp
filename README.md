# Workday Job Scraper

A robust Python tool for scraping job listings from Workday-powered job sites, featuring optional BERT-based semantic search for intelligent job matching. This scraper can search across multiple companies, filter results by location and posting date, and save results to a centralized JSON file.

## Features

- **Optional BERT-based Semantic Search**: Toggle between BERT embeddings or simple keyword matching for job relevance scoring
- **Real-time Parallel Processing**: Jobs are processed and scored in parallel as they are found
- **Centralized Results**: All jobs are stored in a single, sorted JSON file for easy access
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

### Basic Usage

```bash
python scrape_jobs.py
```

### Command Line Options

```
JOB_RESULT_PATH=./path     Path to store job results
LAST_PROCESSED_PATH=./path Path to store last processed information
EXTRACT_START=N           Starting index for extraction
EXTRACT_END=N            Ending index for extraction
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

## How It Works

1. **Initialization**:
   - Loads configuration from config.json
   - Sets up job processor with parallel processing queue
   - Initializes BERT model if USE_BERT is true

2. **Job Processing Pipeline**:
   - **Scraping Phase**:
     - Scrapes jobs from Workday sites
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
     - All jobs are stored in a single all_jobs.json file
     - Jobs are automatically sorted by:
       1. Date (newest first)
       2. Relevance score (highest first)

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
  - Disable BERT to use simple matching
  - Process fewer companies at once
  - Reduce DAYS_LOOKBACK

## License

[MIT License](LICENSE) 