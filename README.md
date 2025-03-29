# Workday Job Scraper

A robust Python tool for scraping job listings from Workday-powered job sites. This scraper can search across multiple companies, filter results by location and posting date, and save results to JSON files.

## Features

- **Unified Scraping**: Process multiple companies from a single YAML configuration
- **Multi-threaded Processing**: Parallel processing of companies for faster execution
- **Robust Error Handling**: Improved error detection and handling with intelligent retries
- **Search Term Filtering**: Filter jobs by specific search terms (with optional fuzzy matching)
- **Time-based Filtering**: Filter jobs based on posting date
- **Location-based Filtering**: Option to filter for US-based jobs only
- **Duplicate Prevention**: Automatically deduplicates jobs across primary and alternate URLs
- **YAML Configuration**: Simple company configuration with multiple URL support

## Installation

1. Clone this repository
2. Install required packages:
   ```
   pip install requests pyyaml
   ```

## Usage

### Basic Usage

```bash
python scrape_jobs.py
```

### Command Line Options

```
--job-result-path       Path to store job results
--last-processed-path   Path to store last processed information
--extract-start         Starting index for extraction
--extract-end           Ending index for extraction
--search-term           Single search term to use
--days-lookback, --days Number of days to look back
--threads               Number of parallel threads for processing companies
--companies-file        Path to YAML file containing companies and URLs
--throttle-wait         Throttle wait time in milliseconds (default: 10000)
--fuzzy                 Use fuzzy search for job matching
--no-fuzzy-search       Don't use fuzzy search for job matching
--global                Search jobs globally (disables US-only filter)
--all-time              Don't filter jobs by posting time
--convert-txt           Convert existing TXT files to YAML format
```

### Using Config File

Create a `config.json` file in the same directory:

```json
{
  "JOB_RESULT_PATH": "./workday_jobs",
  "LAST_PROCESSED_PATH": "./last_processed",
  "EXTRACT_START": 0,
  "EXTRACT_END": 100,
  "DAYS_LOOKBACK": 5,
  "FILTER_US_ONLY": false,
  "FILTER_BY_TIME": true,
  "FUZZY_SEARCH": false,
  "NUM_THREADS": 5,
  "THROTTLE_WAIT_TIME": 15000,
  "SEARCH_TERMS": [
    "Artificial Intelligence",
    "Machine Learning",
    "Data Scientist"
  ]
}
```

### Company Configuration

Create a `workday_companies.yaml` file:

```yaml
companies:
  - name: Company Name 1
    url: https://companyname.wd1.myworkdayjobs.com/careers
    alternate_urls:
      - https://companyname-alt.wd3.myworkdayjobs.com/jobs
  - name: Company Name 2
    url: https://company2.wd5.myworkdayjobs.com/careers
    alternate_urls: []
```

## How It Works

1. Loads companies from YAML configuration
2. For each company (including its alternate URLs):
   - Searches for jobs matching the specified search terms
   - Applies filters (time, location, search terms)
   - Deduplicates jobs from primary and alternate URLs
   - Saves results to a single JSON file per company
3. Generates a summary of all processed jobs

## Advanced Error Handling

The scraper uses specialized error handling to distinguish between different error types:
- 429 Too Many Requests errors (throttling)
- JSON parsing errors
- Connection errors
- Timeout errors
- HTTP errors

When throttling is detected, the scraper implements exponential backoff to reduce pressure on the target servers.

## Example Output

Output files are saved to the configured job result path (default: `./workday_jobs`) as JSON files.

## Migrating from Text Files

If you previously used TXT files to store company information, you can convert them to YAML format:

```bash
python scrape_jobs.py --convert-txt
```

## Troubleshooting

- **Throttle Errors**: Try increasing the `--throttle-wait` value
- **Connection Errors**: Check your network connection or try using a VPN
- **Empty Results**: Verify your search terms and try using the `--fuzzy` option

## License

[MIT License](LICENSE) 