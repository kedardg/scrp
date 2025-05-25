#!/usr/bin/env python3
"""
Workday Job Scraper - Simple sequential implementation that searches across all Workday job boards
using search terms from config.json, with optional BERT-based semantic similarity scoring
"""

import requests
import json
import os
import re
import html
import time
import sys
import psutil
from datetime import datetime, timedelta
from transformers import BertTokenizer, BertModel
import torch
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
from queue import Queue
from threading import Thread, Lock
import threading
from urllib.parse import urlparse, urljoin
from urllib.robotparser import RobotFileParser

# Global regex patterns
URL_EXTRACTION_REGEX = r'https://([a-zA-Z0-9_]*).([a-zA-Z0-9_]*).[a-zA-Z0-9_]*.com/([a-zA-Z0-9_]*)'
HTML_REPLACEMENT_REGEX = r'<[^>]*>'
OLD_JOBS_THRESHOLD = 3  # Stop if we find this many old jobs in a row

# Cache for robots.txt parsers
robots_parser_cache = {}
robots_parser_lock = Lock()

def check_robots_txt(url: str, user_agent: str = '*') -> bool:
    """
    Check if scraping is allowed for the given URL according to robots.txt
    
    Args:
        url (str): URL to check
        user_agent (str): User agent to check permissions for
        
    Returns:
        bool: True if scraping is allowed, False otherwise
    """
    try:
        # Parse the URL to get the base domain
        parsed_url = urlparse(url)
        base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
        
        # Check cache first
        with robots_parser_lock:
            if base_url in robots_parser_cache:
                rp = robots_parser_cache[base_url]
            else:
                # Create and configure the parser
                rp = RobotFileParser()
                rp.set_url(urljoin(base_url, '/robots.txt'))
                
                try:
                    rp.read()
                    # Cache the parser
                    robots_parser_cache[base_url] = rp
                except Exception as e:
                    print(f"Warning: Error reading robots.txt for {base_url}: {e}")
                    # If we can't read robots.txt, assume scraping is allowed
                    return True
        
        # Check if scraping is allowed
        can_fetch = rp.can_fetch(user_agent, url)
        if not can_fetch:
            print(f"Warning: Scraping not allowed for {url} according to robots.txt")
        return can_fetch
        
    except Exception as e:
        print(f"Error checking robots.txt for {url}: {e}")
        # If there's an error checking robots.txt, assume scraping is allowed
        return True

# Handle exceptions
def handle_exception(exc_type, exc_value, exc_traceback):
    print(f"Unhandled Exception: {exc_type.__name__}: {exc_value}")
    sys.exit(1)

sys.excepthook = handle_exception

# Load config from config.json
try:
    with open('./config.json', 'r') as f:
        config = json.load(f)
        SEARCH_TERMS = config.get('SEARCH_TERMS', [''])
        DAYS_LOOKBACK = config.get('DAYS_LOOKBACK', 7)
        FILTER_US_ONLY = config.get('FILTER_US_ONLY', False)
        MIN_RELEVANCE_SCORE = config.get('MIN_RELEVANCE_SCORE', 0.5)
        USE_BERT = config.get('USE_BERT', True)
        CHECK_ROBOTS_TXT = config.get('CHECK_ROBOTS_TXT', True)
        ROBOTS_TXT_USER_AGENT = config.get('ROBOTS_TXT_USER_AGENT', 'WorkdayJobScraper')
except Exception as e:
    print(f"Error loading config.json: {e}")
    SEARCH_TERMS = ['']
    DAYS_LOOKBACK = 7
    FILTER_US_ONLY = False
    MIN_RELEVANCE_SCORE = 0.5
    USE_BERT = True
    CHECK_ROBOTS_TXT = True
    ROBOTS_TXT_USER_AGENT = 'WorkdayJobScraper'

# Set default configs
JOB_RESULT_PATH = "./workday_jobs"
LAST_PROCESSED_PATH = "./last_processed"
EXTRACT_START = 0
EXTRACT_END = float('inf')

# Parse command line arguments
for arg in sys.argv[1:]:
    if '=' in arg:
        key, value = arg.split('=')
        if key == 'JOB_RESULT_PATH':
            JOB_RESULT_PATH = value
        elif key == 'LAST_PROCESSED_PATH':
            LAST_PROCESSED_PATH = value
        elif key == 'EXTRACT_START':
            EXTRACT_START = int(value)
        elif key == 'EXTRACT_END':
            EXTRACT_END = int(value)

# Make directories if they don't exist
os.makedirs(JOB_RESULT_PATH, exist_ok=True)
os.makedirs(LAST_PROCESSED_PATH, exist_ok=True)

print("Configuration (SEQUENTIAL VERSION):")
print(f"JOB_RESULT_PATH: {JOB_RESULT_PATH}")
print(f"LAST_PROCESSED_PATH: {LAST_PROCESSED_PATH}")
print(f"EXTRACT_START: {EXTRACT_START}")
print(f"EXTRACT_END: {EXTRACT_END}")
print(f"SEARCH_TERMS: {SEARCH_TERMS}")
print(f"DAYS_LOOKBACK: {DAYS_LOOKBACK}")
print(f"FILTER_US_ONLY: {FILTER_US_ONLY}")
print(f"MIN_RELEVANCE_SCORE: {MIN_RELEVANCE_SCORE}")
print(f"USE_BERT: {USE_BERT}")
print(f"CHECK_ROBOTS_TXT: {CHECK_ROBOTS_TXT}")
print(f"ROBOTS_TXT_USER_AGENT: {ROBOTS_TXT_USER_AGENT}")

def fetch_url(method, url, payload, max_retries=3, retry_delay=1):
    """
    Fetch data from URL with retries and error handling
    
    Args:
        method (str): HTTP method ('GET' or 'POST')
        url (str): URL to fetch
        payload (dict): Request payload/data
        max_retries (int): Maximum number of retry attempts
        retry_delay (int): Delay between retries in seconds
    
    Returns:
        dict: Response data or error dict
    """
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    }
    
    for attempt in range(max_retries):
        try:
            if method.upper() == 'GET':
                response = requests.get(url, headers=headers, timeout=10)
            else:
                response = requests.post(url, headers=headers, json=payload, timeout=10)
            
            response.raise_for_status()
            
            # Try to parse JSON directly
            try:
                return response.json()
            except json.JSONDecodeError as e:
                print(f"JSON parse error for {url}: {str(e)}", file=sys.stderr)
                
                # Attempt to fix the JSON by cleaning it
                try:
                    # First try to decode with error handling
                    content = response.content.decode('utf-8', errors='ignore')
                    
                    # Remove invalid control characters
                    cleaned = ''.join(c if (ord(c) >= 32 or c in '\n\r\t') else ' ' for c in content)
                    return json.loads(cleaned)
                except json.JSONDecodeError as e2:
                    print(f"Failed to clean JSON for {url}: {str(e2)}", file=sys.stderr)
                    
                    # Return a valid empty structure to prevent crashes
                    if url.endswith('/jobs'):
                        return {"jobPostings": [], "total": 0}
                    else:
                        return {"jobPostingInfo": {}}
                
        except requests.exceptions.RequestException as e:
            print(f"Request failed for {url} (attempt {attempt + 1}/{max_retries}): {e}", file=sys.stderr)
            if attempt == max_retries - 1:
                return {'error': str(e)}
            time.sleep(retry_delay)
            
    return {'error': 'Max retries exceeded'}

class BertSimilarity:
    def __init__(self):
        """Initialize BERT model and tokenizer"""
        self.device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
        self.tokenizer = BertTokenizer.from_pretrained('bert-base-uncased')
        self.model = BertModel.from_pretrained('bert-base-uncased')
        self.model.to(self.device)
        self.model.eval()

    def get_embedding(self, text):
        """Get BERT embedding for a piece of text"""
        # Tokenize and prepare input
        inputs = self.tokenizer(text, 
                              return_tensors="pt",
                              truncation=True,
                              max_length=512,
                              padding=True)
        inputs = {k: v.to(self.device) for k, v in inputs.items()}

        # Get BERT embedding
        with torch.no_grad():
            outputs = self.model(**inputs)
            # Use CLS token embedding as sentence representation
            embedding = outputs.last_hidden_state[:, 0, :].cpu().numpy()
        
        return embedding

    def calculate_similarity(self, text1, text2):
        """Calculate cosine similarity between two texts using BERT embeddings"""
        emb1 = self.get_embedding(text1)
        emb2 = self.get_embedding(text2)
        similarity = cosine_similarity(emb1, emb2)[0][0]
        return similarity

class JobProcessor:
    def __init__(self):
        self.job_queue = Queue()
        self.results_lock = Lock()
        self.results_file = os.path.join(JOB_RESULT_PATH, "all_jobs.json")
        self.temp_dir = os.path.join(JOB_RESULT_PATH, "temp")
        self.bert_similarity = BertSimilarity() if USE_BERT else None
        self.processor_thread = None
        self.is_running = False
        self.current_company = None
        self.company_jobs = {}  # Track jobs per company
        self.memory_threshold = 100  # Number of jobs before temp dump
        
        # Create temp directory
        os.makedirs(self.temp_dir, exist_ok=True)
        
        # Initialize or load results file
        if os.path.exists(self.results_file):
            with open(self.results_file, 'r') as f:
                self.all_jobs = json.load(f)
        else:
            self.all_jobs = []
            self._save_results()
            
        print(f"Initialized JobProcessor. Results will be saved to: {self.results_file}")
        print(f"Temporary files will be stored in: {self.temp_dir}")

    def set_current_company(self, company_acronym):
        """Set the current company being processed"""
        if self.current_company and self.current_company != company_acronym:
            # Dump previous company's jobs before switching
            self._dump_company_jobs(self.current_company)
        self.current_company = company_acronym
        if company_acronym not in self.company_jobs:
            self.company_jobs[company_acronym] = []

    def _dump_company_jobs(self, company_acronym):
        """Dump jobs for a specific company and clear from memory"""
        if company_acronym not in self.company_jobs:
            return
            
        jobs = self.company_jobs[company_acronym]
        if not jobs:
            return
            
        # Save to company file
        company_file = os.path.join(JOB_RESULT_PATH, f"{company_acronym}.json")
        try:
            # Load existing jobs if any
            existing_jobs = []
            if os.path.exists(company_file):
                with open(company_file, 'r') as f:
                    existing_jobs = json.load(f)
            
            # Merge and deduplicate
            all_jobs = existing_jobs + jobs
            unique_jobs = []
            seen_ids = set()
            
            for job in all_jobs:
                job_id = job.get('internal_id')
                if job_id and job_id not in seen_ids:
                    seen_ids.add(job_id)
                    unique_jobs.append(job)
            
            # Sort and save
            unique_jobs.sort(
                key=lambda x: (
                    datetime.fromisoformat(x.get('posted_date', '').replace('Z', '+00:00')),
                    x.get('relevance_score', 0)
                ),
                reverse=True
            )
            
            with open(company_file, 'w') as f:
                json.dump(unique_jobs, f, indent=2)
            print(f"Dumped {len(jobs)} jobs for {company_acronym}")
            
            # Clear from memory
            self.company_jobs[company_acronym] = []
            
        except Exception as e:
            print(f"Error dumping jobs for {company_acronym}: {e}")

    def _dump_temp_jobs(self):
        """Dump jobs to temporary file when memory threshold is reached"""
        if len(self.all_jobs) < self.memory_threshold:
            return
            
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        temp_file = os.path.join(self.temp_dir, f"temp_jobs_{timestamp}.json")
        
        try:
            # Sort by relevance score
            self.all_jobs.sort(key=lambda x: x.get('relevance_score', 0), reverse=True)
            
            # Keep top scoring jobs in memory, dump the rest
            keep_jobs = self.all_jobs[:50]  # Keep top 50 in memory
            dump_jobs = self.all_jobs[50:]
            
            if dump_jobs:
                with open(temp_file, 'w') as f:
                    json.dump(dump_jobs, f, indent=2)
                print(f"Dumped {len(dump_jobs)} jobs to temporary file: {temp_file}")
            
            # Keep only top scoring jobs in memory
            self.all_jobs = keep_jobs
            
        except Exception as e:
            print(f"Error creating temp dump: {e}")

    def _save_job(self, job):
        """Save a job to both company-specific and aggregated results files"""
        with self.results_lock:
            company_acronym = job['company_acronym']
            
            # Set current company if not set
            if self.current_company != company_acronym:
                self.set_current_company(company_acronym)
            
            # Add to company jobs
            if not any(existing['internal_id'] == job['internal_id'] for existing in self.company_jobs[company_acronym]):
                self.company_jobs[company_acronym].append(job)
            
            # Add to all jobs if not duplicate
            if not any(existing['internal_id'] == job['internal_id'] for existing in self.all_jobs):
                self.all_jobs.append(job)
                print(f"Added job: {job['title']} ({job.get('relevance_score', 0):.2f})")
                
                # Check if we need to dump to temp file
                self._dump_temp_jobs()

    def _merge_temp_files(self):
        """Merge all temporary files into final results"""
        all_temp_jobs = []
        
        # Collect all jobs from temp files
        for temp_file in os.listdir(self.temp_dir):
            if not temp_file.endswith('.json'):
                continue
                
            try:
                with open(os.path.join(self.temp_dir, temp_file), 'r') as f:
                    temp_jobs = json.load(f)
                all_temp_jobs.extend(temp_jobs)
            except Exception as e:
                print(f"Error reading temp file {temp_file}: {e}")
        
        # Merge with current jobs
        all_jobs = self.all_jobs + all_temp_jobs
        
        # Deduplicate
        unique_jobs = []
        seen_ids = set()
        
        for job in all_jobs:
            job_id = job.get('internal_id')
            if job_id and job_id not in seen_ids:
                seen_ids.add(job_id)
                unique_jobs.append(job)
        
        # Sort by date and score
        unique_jobs.sort(
            key=lambda x: (
                datetime.fromisoformat(x.get('posted_date', '').replace('Z', '+00:00')),
                x.get('relevance_score', 0)
            ),
            reverse=True
        )
        
        # Save final results
        with open(self.results_file, 'w') as f:
            json.dump(unique_jobs, f, indent=2)
        
        # Clean up temp directory
        for temp_file in os.listdir(self.temp_dir):
            os.remove(os.path.join(self.temp_dir, temp_file))
        
        print(f"Merged {len(all_temp_jobs)} jobs from temporary files")

    def stop(self):
        """Stop the job processor thread and cleanup"""
        self.is_running = False
        if self.processor_thread:
            self.job_queue.put(None)  # Sentinel to stop the thread
            self.processor_thread.join()
            self.processor_thread = None
        
        # Dump any remaining company jobs
        if self.current_company:
            self._dump_company_jobs(self.current_company)
        
        # Merge all temporary files
        self._merge_temp_files()

    def start(self):
        """Start the job processor thread"""
        if USE_BERT and not self.processor_thread:
            self.is_running = True
            self.processor_thread = Thread(target=self._process_jobs)
            self.processor_thread.daemon = True
            self.processor_thread.start()

    def add_job(self, job):
        """Add a job for processing"""
        if USE_BERT:
            self.job_queue.put(job)
        else:
            # If BERT is disabled, just save the job directly
            self._save_job(job)

    def _process_jobs(self):
        """Process jobs from the queue"""
        while self.is_running:
            job = self.job_queue.get()
            if job is None:  # Check for stop sentinel
                break
                
            # Calculate relevance score
            score = calculate_relevance_score(job, job['search_term'], self.bert_similarity)
            
            if score >= MIN_RELEVANCE_SCORE:
                job['relevance_score'] = float(score)
                self._save_job(job)
            
            self.job_queue.task_done()

    def _save_results(self):
        """Save all jobs to aggregated file"""
        with self.results_lock:
            try:
                # Sort by date and score
                self.all_jobs.sort(
                    key=lambda x: (
                        datetime.fromisoformat(x['posted_date'].replace('Z', '+00:00')),
                        x.get('relevance_score', 0)
                    ),
                    reverse=True
                )
                # Save to file
                with open(self.results_file, 'w') as f:
                    json.dump(self.all_jobs, f, indent=2)
            except Exception as e:
                print(f"Error saving to aggregated file {self.results_file}: {e}")

def calculate_relevance_score(job_obj, search_term, bert_similarity=None):
    """Calculate relevance score with or without BERT"""
    if not USE_BERT or not bert_similarity:
        # Simple keyword matching as fallback
        title = job_obj.get('title', '').lower()
        description = job_obj.get('description', '').lower()
        search_lower = search_term.lower()
        
        # Basic scoring: title matches worth more
        score = 0
        if search_lower in title:
            score += 0.6
        if search_lower in description:
            score += 0.4
            
        return score
    
    # BERT-based scoring
    term_mappings = config.get('term_mappings', {})
    
    # Prepare job text
    title = job_obj.get('title', '')
    description = job_obj.get('description', '')
    
    # Expand search term with related terms
    search_parts = search_term.lower().split()
    expanded_terms = []
    for part in search_parts:
        expanded_terms.append(part)
        part_key = part.replace(' ', '_').replace('-', '_').lower()
        if part_key in term_mappings:
            expanded_terms.append(term_mappings[part_key])
    expanded_search = ' '.join(expanded_terms)
    
    # Calculate similarity scores
    title_score = bert_similarity.calculate_similarity(title, expanded_search) if title else 0
    desc_score = bert_similarity.calculate_similarity(description, expanded_search) if description else 0
    
    # Weight title matches more heavily
    final_score = (title_score * 2 + desc_score) / 3 if (title or description) else 0
        
    return final_score

def sanitize_html(text):
    """Clean HTML from text"""
    if not text:
        return ""
    return re.sub(HTML_REPLACEMENT_REGEX, '', html.unescape(text))

def generate_acronym(url_base):
    """Generate an acronym from the URL base string"""
    parts = re.split(r'[_\-]', url_base)
    if len(parts) > 1:
        acronym = ''.join(part[0] for part in parts if part)
    else:
        word = parts[0]
        acronym = word[:3] if len(word) > 3 else word
    return acronym.upper()

def passes_filter(job_obj, last_date):
    """Check if job passes date and location filters"""
    # Filter: Job must be in US if FILTER_US_ONLY is True
    if (FILTER_US_ONLY and job_obj.get('country') and 
        'united states of america' not in job_obj.get('country', '').lower()):
        print('REJECT LOCATION:', job_obj.get('title'))
        return False
    
    # Filter: Job must be after last_date
    try:
        job_date = datetime.fromisoformat(job_obj.get('posted_date', '').replace('Z', '+00:00'))
        if last_date > job_date:
            print('REJECT TIME:', job_obj.get('title'))
            return False
    except (ValueError, TypeError):
        pass

    return True

def process_link(url, search_term, job_processor):
    """Process a company URL with a specific search term to extract jobs"""
    found_jobs = []
    old_jobs_count = 0  # Counter for consecutive old jobs

    # Extract URL components
    match = re.search(URL_EXTRACTION_REGEX, url)
    if not match:
        print(f"Invalid URL format: {url}")
        return
    
    URL_BASE = match.group(1)
    WORKDAY_VERSION = match.group(2)
    END_PATH = match.group(3)

    # Generate acronym for the company
    COMPANY_ACRONYM = generate_acronym(URL_BASE)

    # Build URLs for scraping
    bulk_listings_url = f"https://{URL_BASE}.{WORKDAY_VERSION}.myworkdayjobs.com/wday/cxs/{URL_BASE}/{END_PATH}/jobs"
    specific_listing_url = f"https://{URL_BASE}.{WORKDAY_VERSION}.myworkdayjobs.com/wday/cxs/{URL_BASE}/{END_PATH}"
    
    # Check robots.txt before proceeding if enabled
    if CHECK_ROBOTS_TXT and not check_robots_txt(bulk_listings_url, ROBOTS_TXT_USER_AGENT):
        print(f"Skipping {URL_BASE} - robots.txt disallows scraping")
        return

    # Try to load existing jobs for this company
    existing_jobs = []
    filename = f"{JOB_RESULT_PATH}/{COMPANY_ACRONYM}.json"
    try:
        with open(filename, 'r') as f:
            existing_jobs = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        existing_jobs = []

    # Set last scraped date based on DAYS_LOOKBACK
    last_scraped = datetime.now() - timedelta(days=DAYS_LOOKBACK)

    print(f'\nGetting Jobs for: {COMPANY_ACRONYM} ({URL_BASE}.{WORKDAY_VERSION}) Last: {last_scraped.isoformat()}')

    # Get initial response for job count
    init_res = fetch_url('POST', bulk_listings_url, {"limit": 20, "offset": 0, "searchText": search_term, "appliedFacets": {"locationCountry": [""] if not FILTER_US_ONLY else ["USA"]}})
    if init_res.get('error'):
        print(f"Error fetching initial job count: {init_res.get('error')}")
        return

    # Process jobs in batches
    job_seen = 0
    job_total = init_res.get('total', 0)
    page = 0
    
    print(f'Total: {job_total} for search term |{search_term}|')
    
    while job_seen < job_total:
        res = fetch_url('POST', bulk_listings_url, {"limit": 20, "offset": job_seen, "searchText": search_term, "appliedFacets": {"locationCountry": [""] if not FILTER_US_ONLY else ["USA"]}})
        
        if res.get('error'):
            print('PAGE ERROR')
            break
        
        jobs = res.get('jobPostings', [])
        job_seen += len(jobs)
        page += 1
        
        print(f'======= {job_seen - len(jobs)} - {job_seen} of {job_total} =======')
        
        # Get detailed info for each job
        for job in jobs:
            if not job:
                continue
            
            # Skip jobs without externalPath
            if not job.get('externalPath'):
                continue
                
            job_res = fetch_url('GET', specific_listing_url + job.get('externalPath', ''), {})
            if job_res.get('error'):
                continue
                    
            job_info = job_res.get('jobPostingInfo')
            if not job_info:
                continue
                    
            job_id = f"{COMPANY_ACRONYM}_{job_info.get('jobReqId')}"

            # Check if job already exists
            if any(existing_job['internal_id'] == job_id for existing_job in existing_jobs):
                print(f'DUPLICATE: {job_info.get("title")}')
                continue
                    
            job_obj = {
                'company_acronym': COMPANY_ACRONYM,
                'company_url': URL_BASE,
                'title': job_info.get('title', 'Unknown Title'),
                'description': sanitize_html(job_info.get('jobDescription', '')),
                'internal_id': job_id,
                'location': job_info.get('location', 'Unknown Location'),
                'remote_type': job_info.get('remote_type', 'Onsite'),
                'country': job_info.get('country', {}).get('descriptor', 'Unknown Country'),
                'posted_date': job_info.get('startDate', datetime.now().isoformat()),
                'url': job_info.get('externalUrl', f"https://{URL_BASE}.{WORKDAY_VERSION}.myworkdayjobs.com{job.get('externalPath', '')}"),
                'search_term': search_term,
                'found_date': datetime.now().isoformat()
            }

            if passes_filter(job_obj, last_scraped):
                print(f'SUCCESS: {job_obj["title"]}')
                job_processor.add_job(job_obj)  # Add job to processor queue
                old_jobs_count = 0
            else:
                old_jobs_count += 1
                if old_jobs_count >= OLD_JOBS_THRESHOLD:
                    print(f"Found {OLD_JOBS_THRESHOLD} old jobs in a row, stopping search")
                    return

def post_process_jobs(job_file):
    """
    Post-process a job file to clean up and deduplicate entries
    
    Args:
        job_file (str): Path to the job file to process
    """
    try:
        # Read existing jobs
        with open(job_file, 'r') as f:
            jobs = json.load(f)
            
        if not isinstance(jobs, list):
            return
            
        # Deduplicate based on internal_id
        seen_ids = set()
        unique_jobs = []
        
        for job in jobs:
            if not isinstance(job, dict):
                continue
                
            job_id = job.get('internal_id')
            if not job_id or job_id in seen_ids:
                continue
                
            seen_ids.add(job_id)
            unique_jobs.append(job)
            
        # Sort by date and score
        unique_jobs.sort(
            key=lambda x: (
                datetime.fromisoformat(x.get('posted_date', '').replace('Z', '+00:00')),
                x.get('relevance_score', 0)
            ),
            reverse=True
        )
        
        # Write back processed jobs
        with open(job_file, 'w') as f:
            json.dump(unique_jobs, f, indent=2)
            
    except Exception as e:
        print(f"Error processing {job_file}: {e}")

def consolidate_results(result_dir, output_file=None):
    """
    Consolidate all job JSON files in the result directory into a single file,
    sorted by relevance score in ascending order
    
    Args:
        result_dir (str): Directory containing job JSON files
        output_file (str, optional): Path for the consolidated output file
    """
    if not output_file:
        output_file = os.path.join(result_dir, "consolidated_results.json")
        
    print(f"Consolidating results into: {output_file}")
    
    # Collect all jobs from all files
    all_jobs = []
    job_files = [f for f in os.listdir(result_dir) if f.endswith('.json')]
    
    for job_file in job_files:
        try:
            with open(os.path.join(result_dir, job_file), 'r') as f:
                jobs = json.load(f)
                if isinstance(jobs, list):
                    all_jobs.extend(jobs)
        except Exception as e:
            print(f"Error reading {job_file}: {e}")
    
    # Deduplicate jobs based on internal_id
    unique_jobs = []
    seen_ids = set()
    
    for job in all_jobs:
        if not isinstance(job, dict):
            continue
            
        job_id = job.get('internal_id')
        if not job_id or job_id in seen_ids:
            continue
            
        seen_ids.add(job_id)
        unique_jobs.append(job)
    
    # Sort by relevance score (ascending order)
    unique_jobs.sort(key=lambda x: x.get('relevance_score', 0))
    
    # Save consolidated results
    try:
        with open(output_file, 'w') as f:
            json.dump(unique_jobs, f, indent=2)
        print(f"Saved {len(unique_jobs)} unique jobs to {output_file}")
    except Exception as e:
        print(f"Error saving consolidated results: {e}")
    
    return len(unique_jobs)

def extract_workday_jobs(job_file_path):
    """
    Main function to extract jobs from Workday sites - sequential version
    
    Args:
        job_file_path (str): Path to file with Workday URLs to scrape
    """
    # Initialize job processor
    job_processor = JobProcessor()
    job_processor.start()
    
    try:
        # Read company URLs
        with open(job_file_path, 'r') as f:
            all_companies = [line.strip() for line in f if line.strip()]

        # Filter based on EXTRACT_START and EXTRACT_END
        companies_to_process = [
            company for i, company in enumerate(all_companies)
            if EXTRACT_START <= i <= EXTRACT_END
        ]
        
        # Process each company sequentially
        total_companies = len(companies_to_process)
        print(f"Processing {total_companies} companies sequentially")
        
        for i, company_url in enumerate(companies_to_process):
            print(f"\nProcessing company {i+1}/{total_companies}: {company_url}")
            
            # Process each search term for this company
            for search_term in SEARCH_TERMS:
                print(f"  - Search term: '{search_term}'")
                try:
                    process_link(company_url, search_term, job_processor)
                except Exception as e:
                    print(f"Error processing {company_url} with '{search_term}': {e}")
    
    except Exception as e:
        print(f"Error in main process: {e}")
    
    finally:
        # Stop the job processor
        job_processor.stop()
        
        # Post-process all job files
        print("\nPost-processing job files...")
        for filename in os.listdir(JOB_RESULT_PATH):
            if filename.endswith('.json'):
                post_process_jobs(os.path.join(JOB_RESULT_PATH, filename))

if __name__ == "__main__":
    # Parse command line arguments before anything else
    for arg in sys.argv[1:]:
        if '=' in arg:
            key, value = arg.split('=')
            if key == 'JOB_RESULT_PATH':
                JOB_RESULT_PATH = value
            elif key == 'LAST_PROCESSED_PATH':
                LAST_PROCESSED_PATH = value
            elif key == 'EXTRACT_START':
                EXTRACT_START = int(value)
            elif key == 'EXTRACT_END':
                EXTRACT_END = int(value)
    
    # Make directories if they don't exist
    os.makedirs(JOB_RESULT_PATH, exist_ok=True)
    os.makedirs(LAST_PROCESSED_PATH, exist_ok=True)
    
    print("Configuration (SEQUENTIAL VERSION):")
    print(f"JOB_RESULT_PATH: {JOB_RESULT_PATH}")
    print(f"LAST_PROCESSED_PATH: {LAST_PROCESSED_PATH}")
    print(f"EXTRACT_START: {EXTRACT_START}")
    print(f"EXTRACT_END: {EXTRACT_END}")
    print(f"SEARCH_TERMS: {SEARCH_TERMS}")
    print(f"DAYS_LOOKBACK: {DAYS_LOOKBACK}")
    print(f"FILTER_US_ONLY: {FILTER_US_ONLY}")
    print(f"MIN_RELEVANCE_SCORE: {MIN_RELEVANCE_SCORE}")
    print(f"USE_BERT: {USE_BERT}")
    print(f"CHECK_ROBOTS_TXT: {CHECK_ROBOTS_TXT}")
    print(f"ROBOTS_TXT_USER_AGENT: {ROBOTS_TXT_USER_AGENT}")
    
    # Run the extraction
    extract_workday_jobs('./all_myworkdayjobs_links.txt')
    
    # Consolidate all results into a single file
    total_jobs = consolidate_results(JOB_RESULT_PATH)
    print(f'DONE - SEQUENTIAL VERSION - {total_jobs} unique jobs consolidated')