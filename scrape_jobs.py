#!/usr/bin/env python3
"""
Workday Job Scraper - Unified Script with YAML Configuration

This script combines the functionality of run_scraper.py and scrape_jobs.py
and uses YAML for configuration and storing company/URL information.
Modified to generate a single file per organization instead of per search term.
"""

import requests
import json
import os
import re
import html
import time
import argparse
import asyncio
import sys
import yaml
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

# Handle exceptions
def handle_exception(exc_type, exc_value, exc_traceback):
    print(f"Unhandled Exception: {exc_type.__name__}: {exc_value}")
    sys.exit(1)

sys.excepthook = handle_exception

class WorkdayJobScraper:
    def __init__(self, config):
        # Core settings
        self.JOB_RESULT_PATH = config.get('JOB_RESULT_PATH', './workday_jobs')
        self.LAST_PROCESSED_PATH = config.get('LAST_PROCESSED_PATH', './last_processed')
        self.EXTRACT_START = config.get('EXTRACT_START', 0)
        self.EXTRACT_END = config.get('EXTRACT_END', sys.maxsize)
        self.SEARCH_TERMS = config.get('SEARCH_TERMS', [""])
        self.DAYS_LOOKBACK = config.get('DAYS_LOOKBACK', 1)
        self.FILTER_US_ONLY = config.get('FILTER_US_ONLY', False)
        self.FILTER_BY_TIME = config.get('FILTER_BY_TIME', True)
        self.FUZZY_SEARCH = config.get('FUZZY_SEARCH', False)
        self.NUM_THREADS = config.get('NUM_THREADS', 1)
        self.COMPANIES_FILE = config.get('COMPANIES_FILE', './workday_companies.yaml')
        
        # Internal settings
        self.LOGGING = True
        self.THROTTLE_WAIT_TIME = config.get('THROTTLE_WAIT_TIME', 10000)  # ms
        
        # Default headers for requests
        self.DEFAULT_HEADERS = {
            'Content-Type': 'application/json',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive'
        }
        
        # Regex patterns
        self.url_extraction_regex = r'https:\/\/([a-zA-Z0-9_]*).([a-zA-Z0-9_]*).[a-zA-Z0-9_]*.com\/([a-zA-Z0-9_]*)'
        self.html_replacement_regex = r'<[^>]*>'
        
        # Create necessary directories
        os.makedirs(self.JOB_RESULT_PATH, exist_ok=True)
        os.makedirs(self.LAST_PROCESSED_PATH, exist_ok=True)
        
        self.log(f"Configuration: {json.dumps(config, indent=2)}")
    
    def log(self, *args):
        if self.LOGGING:
            print(*args)
    
    def sanitize_html(self, text):
        """Remove HTML tags and decode HTML entities"""
        if not text:
            return ""
        return re.sub(self.html_replacement_regex, '', html.unescape(text))
    
    def wait(self, time_ms):
        """Wait for specified time in milliseconds"""
        time.sleep(time_ms / 1000)
    
    async def fetch_url(self, method, url, payload):
        """Fetch data from URL with retries"""
        headers = self.DEFAULT_HEADERS.copy()
        
        for i in range(5):  # Try 5 times
            try:
                if method.upper() == 'POST':
                    response = requests.post(url, json=payload, headers=headers, timeout=30)
                else:
                    response = requests.get(url, headers=headers, timeout=30)
                
                # Check specifically for throttling status codes
                if response.status_code == 429:  # Too Many Requests
                    self.log(f"THROTTLE DETECTED (429): {method} {url}")
                    # Exponential backoff instead of linear
                    wait_time = self.THROTTLE_WAIT_TIME * (1.5 ** i)
                    self.wait(wait_time)
                    continue
                
                response.raise_for_status()  # Raise exception for other HTTP errors
                
                try:
                    return response.json()
                except json.JSONDecodeError:
                    # This is likely a throttling issue (like in JS)
                    self.log(f"THROTTLE DETECTED (JSON Error): {method} {url}")
                    # Exponential backoff
                    wait_time = self.THROTTLE_WAIT_TIME * (1.5 ** i)
                    self.wait(wait_time)
            except requests.exceptions.Timeout:
                self.log(f"TIMEOUT: {method} {url} - Retrying...")
                wait_time = self.THROTTLE_WAIT_TIME * (1.5 ** i)
                self.wait(wait_time)
            except requests.exceptions.ConnectionError:
                self.log(f"CONNECTION ERROR: {method} {url} - Retrying...")
                wait_time = self.THROTTLE_WAIT_TIME * (1.5 ** i)
                self.wait(wait_time)
            except requests.exceptions.HTTPError as e:
                status_code = e.response.status_code if hasattr(e, 'response') else "unknown"
                self.log(f"HTTP ERROR ({status_code}): {method} {url} - {str(e)}")
                # Only retry 5xx errors, 4xx are client errors
                if status_code >= 500:
                    wait_time = self.THROTTLE_WAIT_TIME * (1.5 ** i)
                    self.wait(wait_time)
                else:
                    return {"error": True, "status": status_code, "message": str(e)}
            except Exception as e:
                # Other errors might not be throttling related
                self.log(f"ERROR (other): {method} {url} - {str(e)}")
                # Log the response content if available
                try:
                    if 'response' in locals():
                        self.log(f"Response content: {response.text[:200]}...")
                except:
                    pass
                # Still retry with backoff
                wait_time = self.THROTTLE_WAIT_TIME * (1.5 ** i)
                self.wait(wait_time)
        
        self.log(f"CRITICAL ERROR SKIPPING SITE: {method} {url} {payload}")
        return {"error": True}
    
    def passes_filter(self, job_obj, last_date, search_term):
        """Check if job passes our filtering criteria"""
        # Location filter (US only)
        if (self.FILTER_US_ONLY and job_obj.get('country') and 
            'united states of america' not in job_obj.get('country', '').lower()):
            self.log(f"REJECT LOCATION: {job_obj.get('title')}")
            return False
        
        # Time filter
        if self.FILTER_BY_TIME:
            try:
                job_date = datetime.fromisoformat(job_obj.get('posted_date', '').replace('Z', '+00:00'))
                if last_date > job_date:
                    self.log(f"REJECT TIME: {job_obj.get('title')}")
                    return False
            except (ValueError, TypeError):
                # If date parsing fails, assume it's valid
                self.log(f"WARNING: Invalid date format: {job_obj.get('posted_date')}")
        
        # Search term filter
        if search_term and search_term.strip():
            escaped_term = re.escape(search_term)
            
            if self.FUZZY_SEARCH:
                # Split into words and check if any match
                terms = [t for t in escaped_term.split() if len(t) > 2]
                matched = False
                
                for term in terms:
                    regex = re.compile(term, re.IGNORECASE)
                    if (regex.search(job_obj.get('title', '')) or 
                        regex.search(job_obj.get('description', ''))):
                        matched = True
                        self.log(f"MATCH TERM \"{term}\" in: {job_obj.get('title')}")
                        break
                
                if not matched:
                    self.log(f"REJECT SEARCH TERM: {job_obj.get('title')}")
                    return False
            else:
                # Strict search - entire term must match
                search_regex = re.compile(escaped_term, re.IGNORECASE)
                if (not search_regex.search(job_obj.get('title', '')) and 
                    not search_regex.search(job_obj.get('description', ''))):
                    self.log(f"REJECT SEARCH TERM: {job_obj.get('title')}")
                    return False
        
        # All filters passed
        self.log(f"ACCEPTED: {job_obj.get('title')}")
        return True
    
    async def get_job_info(self, job, specific_listing_url, last_scraped, search_term, found_jobs, URL_BASE, company_data):
        """Get detailed job information for a job posting"""
        if not job:
            self.log("No job info")
            return
        
        job_res = await self.fetch_url('GET', specific_listing_url + job.get('externalPath', ''), {})
        
        # Skip if error
        if job_res.get('error'):
            self.log("Error: Skipping")
            return
        
        job_info = job_res.get('jobPostingInfo')
        
        # Handle error in job info
        if not job_info:
            self.log(job_res)
            self.log(job_info)
            return
            
        # Validate that the job belongs to this company
        job_url = job_info.get('externalUrl', '')
        if not job_url or URL_BASE.lower() not in job_url.lower():
            self.log(f"REJECT COMPANY MISMATCH: Job URL {job_url} doesn't match company URL base {URL_BASE}")
            return
        
        # Create a safe company abbreviation from the company name
        company_abbrev = re.sub(r'[^\w\-_]', '', company_data['name'].lower()[:10])
        
        # Build job object
        job_obj = {
            'company_name': company_data['name'],
            'company_abbreviation': company_abbrev,
            'title': job_info.get('title'),
            'description': self.sanitize_html(job_info.get('jobDescription')),
            'internal_id': f"{URL_BASE}_{job_info.get('jobReqId')}",  # Make IDs unique across companies
            'location': job_info.get('location'),
            'remote_type': job_info.get('remote_type', 'Onsite'),
            'country': job_info.get('country', {}).get('descriptor'),
            'posted_date': job_info.get('startDate'),
            'url': job_url,
            'search_term': search_term  # Add the search term that found this job
        }
        
        if self.passes_filter(job_obj, last_scraped, search_term):
            # Check if this job already exists in found_jobs by internal_id
            job_id = job_obj.get('internal_id')
            if job_id:
                # Check if we already have this job from a different search term
                existing_jobs = [j for j in found_jobs if j.get('internal_id') == job_id]
                if existing_jobs:
                    # Job already exists, update the search_terms field instead of adding duplicate
                    for existing_job in existing_jobs:
                        if 'search_terms' not in existing_job:
                            existing_job['search_terms'] = [existing_job.get('search_term')]
                        
                        if search_term not in existing_job['search_terms']:
                            existing_job['search_terms'].append(search_term)
                    
                    self.log(f"DUPLICATE: {job_obj.get('title')} - Added search term")
                    return
            
            # New job, add it with search terms field
            job_obj['search_terms'] = [search_term]
            self.log(f"SUCCESS: {job_obj.get('title')}")
            found_jobs.append(job_obj)
    
    async def process_link(self, company_data, search_term, found_jobs, URL_BASE, WORKDAY_VERSION, END_PATH):
        """Process a single Workday URL with a specific search term"""
        url = company_data['url']
        company_name = company_data['name']
        
        # Set last scraped date based on days lookback
        last_scraped = datetime.now() - timedelta(days=self.DAYS_LOOKBACK)
        
        self.log(
            f"\nGetting Jobs for: {company_name} ({URL_BASE}.{WORKDAY_VERSION})",
            f"Last: {last_scraped.isoformat()}",
            f"Search Term: \"{search_term}\""
        )
        
        # Build URLs for the API requests
        bulk_listings_url = f"https://{URL_BASE}.{WORKDAY_VERSION}.myworkdayjobs.com/wday/cxs/{URL_BASE}/{END_PATH}/jobs"
        specific_listing_url = f"https://{URL_BASE}.{WORKDAY_VERSION}.myworkdayjobs.com/wday/cxs/{URL_BASE}/{END_PATH}"
        
        # Setup payload
        bulk_listings_payload = {
            "limit": 20,
            "offset": 0,
            "searchText": search_term,
            "appliedFacets": {
                "locationCountry": [""]
            }
        }
        
        # Add time filter if enabled
        if self.FILTER_BY_TIME:
            time_filter = "ANYTIME"
            if self.DAYS_LOOKBACK <= 1:
                time_filter = "PAST_DAY"
            elif self.DAYS_LOOKBACK <= 7:
                time_filter = "PAST_WEEK"
            elif self.DAYS_LOOKBACK <= 30:
                time_filter = "PAST_MONTH"
            
            bulk_listings_payload["appliedFacets"]["timePosted"] = [time_filter]
            self.log(f"Using time filter: {time_filter} ({self.DAYS_LOOKBACK} days lookback)")
        else:
            self.log("Time filtering disabled")
        
        # Get initial response to determine job count
        init_res = await self.fetch_url('POST', bulk_listings_url, bulk_listings_payload)
        
        # Track progress
        page = 0
        job_seen = 0
        job_total = init_res.get('total', 0)
        
        self.log(f"Total: {job_total} for search term |{search_term}|")
        
        # Fetch jobs in batches of 20
        while job_seen < job_total:
            bulk_listings_payload["offset"] = job_seen
            res = await self.fetch_url('POST', bulk_listings_url, bulk_listings_payload)
            
            if res.get('error'):
                self.log("PAGE ERROR")
                break
            
            jobs = res.get('jobPostings', [])
            job_seen += len(jobs)
            page += 1
            
            self.log(f"======= {job_seen - len(jobs)} - {job_seen} of {job_total} =======")
            
            # Process all jobs in this batch
            tasks = []
            for job in jobs:
                tasks.append(self.get_job_info(
                    job, specific_listing_url, last_scraped, search_term, found_jobs, URL_BASE, company_data
                ))
            
            # Wait for all job info requests to complete
            await asyncio.gather(*tasks)
        
        # Return search term stats
        return {
            'search_term': search_term,
            'jobs_found': job_total
        }
    
    async def _process_company(self, company_num, company_data):
        """Process a single company with all search terms"""
        self.log(f"Processing company {company_num}: {company_data['name']}")
        
        url = company_data['url']
        company_name = company_data['name']
        alternate_urls = company_data.get('alternate_urls', [])
        
        # Extract URL components using regex for primary URL
        match = re.search(self.url_extraction_regex, url)
        if not match:
            self.log(f"Invalid URL Format: {url}")
            return
        
        URL_BASE = match.group(1)
        WORKDAY_VERSION = match.group(2)
        END_PATH = match.group(3)
        
        # Initialize a list to store all jobs for this company
        found_jobs = []
        search_stats = []
        
        # Process the primary URL with all search terms
        for i, search_term in enumerate(self.SEARCH_TERMS):
            self.log(f"Company {company_num} primary URL: Search term {i+1}/{len(self.SEARCH_TERMS)}: \"{search_term}\"")
            
            try:
                result = await self.process_link(
                    company_data, search_term, found_jobs, URL_BASE, WORKDAY_VERSION, END_PATH
                )
                if result:
                    search_stats.append(result)
            except Exception as e:
                # Always log errors
                print(f"Error grabbing jobs for: {company_data['name']} with search term \"{search_term}\"\nError: {str(e)}")
        
        # Process each alternate URL with all search terms
        for alt_url_index, alt_url in enumerate(alternate_urls):
            alt_match = re.search(self.url_extraction_regex, alt_url)
            if not alt_match:
                self.log(f"Invalid alternate URL format: {alt_url}")
                continue
                
            ALT_URL_BASE = alt_match.group(1)
            ALT_WORKDAY_VERSION = alt_match.group(2)
            ALT_END_PATH = alt_match.group(3)
            
            self.log(f"Processing alternate URL {alt_url_index+1}/{len(alternate_urls)} for {company_name}")
            
            # Create alternate company data for processing
            alt_company_data = {
                'name': f"{company_name} (Alt URL {alt_url_index+1})",
                'url': alt_url
            }
            
            # Process this alternate URL with all search terms
            for i, search_term in enumerate(self.SEARCH_TERMS):
                self.log(f"Company {company_num} alt URL {alt_url_index+1}: Search term {i+1}/{len(self.SEARCH_TERMS)}: \"{search_term}\"")
                
                try:
                    result = await self.process_link(
                        alt_company_data, search_term, found_jobs, ALT_URL_BASE, ALT_WORKDAY_VERSION, ALT_END_PATH
                    )
                    if result:
                        search_stats.append(result)
                except Exception as e:
                    # Always log errors
                    print(f"Error grabbing jobs from alternate URL {alt_url_index+1} for: {company_data['name']} with search term \"{search_term}\"\nError: {str(e)}")
            
            # Write last processed time for alternate URL
            with open(f"{self.LAST_PROCESSED_PATH}/{ALT_URL_BASE}_{ALT_WORKDAY_VERSION}_{ALT_END_PATH}.txt", "w") as f:
                f.write(datetime.now().isoformat())
        
        # Write last processed time for primary URL
        with open(f"{self.LAST_PROCESSED_PATH}/{URL_BASE}_{WORKDAY_VERSION}_{END_PATH}.txt", "w") as f:
            f.write(datetime.now().isoformat())
        
        # Create a sanitized filename for this company (without search term)
        safe_company_name = re.sub(r'[^\w\-_]', '_', company_name)
        filename = f"{self.JOB_RESULT_PATH}/{company_num}_{safe_company_name}.json"
        
        # Deduplicate jobs by internal_id
        deduplicated_jobs = {}
        for job in found_jobs:
            job_id = job.get('internal_id')
            if job_id not in deduplicated_jobs:
                deduplicated_jobs[job_id] = job
            else:
                # Merge search terms if this job was already found
                existing_search_terms = deduplicated_jobs[job_id].get('search_terms', [])
                new_search_terms = job.get('search_terms', [])
                combined_terms = list(set(existing_search_terms + new_search_terms))
                deduplicated_jobs[job_id]['search_terms'] = combined_terms
        
        # Convert back to list
        final_jobs = list(deduplicated_jobs.values())
        
        # Write all found jobs to a single file for this company
        with open(filename, "w") as f:
            json.dump(final_jobs, f)
            
        self.log(f"Wrote {len(final_jobs)} jobs to {filename}")
        
        # Return job data for potential further processing
        return {
            'company': company_name,
            'url': url,
            'alternate_urls': alternate_urls,
            'search_stats': search_stats,
            'total_jobs_found': len(final_jobs),
            'timestamp': datetime.now().isoformat()
        }
    
    def load_companies(self):
        """Load companies from YAML file"""
        try:
            with open(self.COMPANIES_FILE, 'r') as f:
                data = yaml.safe_load(f)
                companies = data.get('companies', [])
                
                # Track URLs to prevent duplicates
                seen_urls = {}
                validated_companies = []
                
                for company in companies:
                    url = company.get('url', '')
                    name = company.get('name', '')
                    
                    # Skip invalid entries
                    if not url or not name:
                        self.log(f"Skipping invalid company entry: {company}")
                        continue
                        
                    # Extract URL base to check for duplicates
                    match = re.search(self.url_extraction_regex, url)
                    if not match:
                        self.log(f"Invalid URL format for company {name}: {url}")
                        continue
                        
                    url_base = match.group(1)
                    
                    # Check for duplicate URLs
                    if url_base in seen_urls:
                        self.log(f"WARNING: Company {name} uses same URL base as {seen_urls[url_base]}, skipping")
                        continue
                        
                    seen_urls[url_base] = name
                    
                    # Validate alternate URLs
                    valid_alternates = []
                    for alt_url in company.get('alternate_urls', []):
                        alt_match = re.search(self.url_extraction_regex, alt_url)
                        if not alt_match:
                            self.log(f"Invalid alternate URL format for company {name}: {alt_url}")
                            continue
                            
                        alt_base = alt_match.group(1)
                        if alt_base in seen_urls:
                            self.log(f"WARNING: Alternate URL for {name} uses same base as {seen_urls[alt_base]}, skipping")
                            continue
                            
                        seen_urls[alt_base] = f"{name} (alternate)"
                        valid_alternates.append(alt_url)
                    
                    # Add validated company
                    validated_company = company.copy()
                    validated_company['alternate_urls'] = valid_alternates
                    validated_companies.append(validated_company)
                
                self.log(f"Loaded {len(validated_companies)} valid companies from {len(companies)} entries")
                return validated_companies
                
        except FileNotFoundError:
            print(f"Companies file not found: {self.COMPANIES_FILE}")
            print("Creating a sample file structure. Please update it with real data.")
            
            # Create a sample YAML file
            sample_data = {
                'companies': [
                    {
                        'name': 'Example Company 1', 
                        'url': 'https://example.wd1.myworkdayjobs.com/careers',
                        'alternate_urls': [
                            'https://example-alt.wd5.myworkdayjobs.com/careers'
                        ]
                    },
                    {
                        'name': 'Example Company 2', 
                        'url': 'https://example2.wd3.myworkdayjobs.com/jobs',
                        'alternate_urls': []
                    }
                ]
            }
            
            with open(self.COMPANIES_FILE, 'w') as f:
                yaml.dump(sample_data, f, sort_keys=False, default_flow_style=False)
            
            return []
        except Exception as e:
            print(f"Error loading companies file: {str(e)}")
            return []
    
    async def extract_workday_jobs(self):
        """Extract jobs from all companies in the YAML file, with parallelization"""
        # Load companies from YAML
        all_companies = self.load_companies()
        
        if not all_companies:
            print(f"No companies found in {self.COMPANIES_FILE}. Please update the file with valid company data.")
            return
        
        # Filter companies based on extraction range
        companies = []
        for company_num, company_data in enumerate(all_companies):
            if self.EXTRACT_START <= company_num <= self.EXTRACT_END:
                companies.append((company_num, company_data))
        
        self.log(f"Found {len(companies)} companies to process with {len(self.SEARCH_TERMS)} search terms each")
        self.log(f"Using {self.NUM_THREADS} parallel threads")
        
        all_results = []
        
        # Process companies in parallel batches
        if self.NUM_THREADS <= 1:
            # Sequential processing if NUM_THREADS is 1 or less
            for company_num, company_data in companies:
                results = await self._process_company(company_num, company_data)
                all_results.append(results)
        else:
            # Process companies in parallel
            # Create batches of companies to process
            batch_size = max(1, len(companies) // self.NUM_THREADS)
            batches = [companies[i:i + batch_size] for i in range(0, len(companies), batch_size)]
            
            # Create tasks for each batch
            async def process_batch(batch):
                batch_results = []
                for company_num, company_data in batch:
                    results = await self._process_company(company_num, company_data)
                    batch_results.append(results)
                return batch_results
            
            tasks = [process_batch(batch) for batch in batches]
            
            # Run all batches concurrently and collect results
            results = await asyncio.gather(*tasks)
            for batch_result in results:
                all_results.extend(batch_result)
        
        # Write summary of results
        summary_file = Path(self.JOB_RESULT_PATH) / "scraping_summary.yaml"
        with open(summary_file, 'w') as f:
            yaml.dump({
                'timestamp': datetime.now().isoformat(),
                'companies_processed': len(companies),
                'search_terms': self.SEARCH_TERMS,
                'results': all_results
            }, f, sort_keys=False, default_flow_style=False)
        
        self.log(f"Wrote summary to {summary_file}")

    @staticmethod
    def convert_txt_to_yaml(companies_file='./workday_companies.yaml',
                        names_file='./all_myworkdayjobs_companies.txt',
                        urls_file='./all_myworkdayjobs_links.txt'):
        """
        Convert existing TXT files to the new YAML format.
        This is a utility method to help migrate from the old format.
        Properly handles multiple URLs for the same company.
        """
        # Check if files exist
        if not os.path.exists(names_file):
            print(f"Companies names file not found: {names_file}")
            return False
            
        if not os.path.exists(urls_file):
            print(f"URLs file not found: {urls_file}")
            return False
            
        # Read the files
        with open(names_file, 'r') as f:
            names = [line.strip() for line in f if line.strip()]
            
        with open(urls_file, 'r') as f:
            urls = [line.strip() for line in f if line.strip()]
        
        # Extract company identifier from URL
        url_patterns = {}
        for url in urls:
            match = re.search(r'https:\/\/([a-zA-Z0-9_]*).([a-zA-Z0-9_]*).[a-zA-Z0-9_]*.com', url)
            if match:
                company_id = match.group(1)
                if company_id not in url_patterns:
                    url_patterns[company_id] = []
                url_patterns[company_id].append(url)
        
        # Map company names to URLs based on matching patterns
        companies = []
        for name in names:
            # Sanitize name for matching
            clean_name = re.sub(r'[^\w\-]', '', name.lower())
            
            # Try direct match first
            found = False
            for company_id, company_urls in url_patterns.items():
                if company_id.lower() in clean_name or clean_name in company_id.lower():
                    # Found a match, add it to companies
                    companies.append({
                        'name': name,
                        'url': company_urls[0],  # Primary URL
                        'alternate_urls': company_urls[1:] if len(company_urls) > 1 else []
                    })
                    found = True
                    break
            
            # If no match found, check if we have any remaining URLs
            if not found and url_patterns:
                # Use the first available URL pattern
                company_id, company_urls = next(iter(url_patterns.items()))
                companies.append({
                    'name': name,
                    'url': company_urls[0],
                    'alternate_urls': company_urls[1:] if len(company_urls) > 1 else []
                })
                # Remove the used URL pattern
                del url_patterns[company_id]
        
        # Add any remaining URL patterns that didn't match any company
        for company_id, company_urls in url_patterns.items():
            companies.append({
                'name': f"Unknown Company ({company_id})",
                'url': company_urls[0],
                'alternate_urls': company_urls[1:] if len(company_urls) > 1 else []
            })
        
        # Create the YAML file
        with open(companies_file, 'w') as f:
            yaml.dump({'companies': companies}, f, sort_keys=False, default_flow_style=False)
            
        print(f"Created YAML file with {len(companies)} companies: {companies_file}")
        print(f"- {sum(1 for c in companies if c.get('alternate_urls'))} companies have alternate URLs")
        return True


def load_config():
    """Load configuration from file or use defaults"""
    config = {}
    try:
        with open('./config.json', 'r') as f:
            config = json.load(f)
            print('Loaded configuration from config.json')
    except Exception:
        print('No config file found or error reading it, using defaults')
    
    return config

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Workday Jobs Scraper')
    
    # Main options
    parser.add_argument('--job-result-path', dest='JOB_RESULT_PATH', 
                      help='Path to store job results')
    parser.add_argument('--last-processed-path', dest='LAST_PROCESSED_PATH',
                      help='Path to store last processed information')
    parser.add_argument('--extract-start', dest='EXTRACT_START', type=int,
                      help='Starting index for extraction')
    parser.add_argument('--extract-end', dest='EXTRACT_END', type=int,
                      help='Ending index for extraction')
    parser.add_argument('--search-term', dest='SEARCH_TERM',
                      help='Single search term to use')
    parser.add_argument('--days-lookback', '--days', dest='DAYS_LOOKBACK', type=int,
                      help='Number of days to look back')
    parser.add_argument('--threads', dest='NUM_THREADS', type=int, default=None,
                      help='Number of parallel threads for processing companies')
    parser.add_argument('--companies-file', dest='COMPANIES_FILE',
                      help='Path to YAML file containing companies and URLs')
    parser.add_argument('--throttle-wait', dest='THROTTLE_WAIT_TIME', type=int,
                      help='Throttle wait time in milliseconds (default: 10000)')
    
    # Flag options (boolean)
    parser.add_argument('--fuzzy', dest='FUZZY_SEARCH', action='store_true',
                      help='Use fuzzy search for job matching')
    parser.add_argument('--no-fuzzy-search', dest='FUZZY_SEARCH', action='store_false',
                      help='Don\'t use fuzzy search for job matching')
    
    parser.add_argument('--global', dest='GLOBAL_SEARCH', action='store_true',
                      help='Search jobs globally (disables US-only filter)')
    
    parser.add_argument('--all-time', dest='ALL_TIME', action='store_true',
                      help='Don\'t filter jobs by posting time')
                      
    # Utility commands
    parser.add_argument('--convert-txt', action='store_true',
                      help='Convert existing TXT files to YAML format')
    
    return parser.parse_args()

async def main():
    """Main function to run the scraper"""
    # Load config and parse arguments
    config = load_config()
    args = parse_args()
    
    # Handle utility commands first
    if args.convert_txt:
        # Get companies file from args or config
        companies_file = args.COMPANIES_FILE or config.get('COMPANIES_FILE', './workday_companies.yaml')
        WorkdayJobScraper.convert_txt_to_yaml(companies_file=companies_file)
        return
    
    # Update config with command line arguments
    for key, value in vars(args).items():
        if value is not None:
            if key == 'GLOBAL_SEARCH' and value is True:
                config['FILTER_US_ONLY'] = False
            elif key == 'ALL_TIME' and value is True:
                config['FILTER_BY_TIME'] = False
            else:
                config[key] = value
    
    # Convert single search term to list if provided
    if 'SEARCH_TERM' in config:
        config['SEARCH_TERMS'] = [config['SEARCH_TERM']]
        del config['SEARCH_TERM']
    
    # Display run configuration
    print("Run configuration:")
    if config.get('FUZZY_SEARCH'):
        print("- Using fuzzy search")
    if not config.get('FILTER_US_ONLY', True):
        print("- US-only filter disabled (global search)")
    if not config.get('FILTER_BY_TIME', True):
        print("- Time filtering disabled (all-time search)")
    if 'SEARCH_TERMS' in config and config['SEARCH_TERMS'] != [""]:
        print(f"- Search terms: {config['SEARCH_TERMS']}")
    if 'DAYS_LOOKBACK' in config:
        print(f"- Looking back {config['DAYS_LOOKBACK']} days")
    print(f"- Using {config.get('NUM_THREADS', 1)} parallel threads")
    print(f"- Companies file: {config.get('COMPANIES_FILE', './workday_companies.yaml')}")
    
    # Initialize and run the scraper
    scraper = WorkdayJobScraper(config)
    await scraper.extract_workday_jobs()
    
    print('DONE')

if __name__ == "__main__":
    asyncio.run(main())