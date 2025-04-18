#!/usr/bin/env python3
"""
Workday Job Scraper - Simple implementation that searches across all Workday job boards
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
import multiprocessing
from multiprocessing import Process, Pipe, Manager
from typing import List, Tuple, Dict
import signal
import itertools
import concurrent.futures
import fcntl
import shutil
from collections import defaultdict
from contextlib import contextmanager
from concurrent.futures import ProcessPoolExecutor, as_completed
import logging
from urllib.parse import urlparse
import multiprocessing as mp

# Global regex patterns for use across all processes
URL_EXTRACTION_REGEX = r'https://([a-zA-Z0-9_]*).([a-zA-Z0-9_]*).[a-zA-Z0-9_]*.com/([a-zA-Z0-9_]*)'
HTML_REPLACEMENT_REGEX = r'<[^>]*>'
OLD_JOBS_THRESHOLD = 10  # Stop if we find this many old jobs in a row

# Domain throttling implementation
class DomainThrottler:
    def __init__(self, wait_time=2):  # seconds between requests
        self.domain_last_request = defaultdict(float)
        self.lock = Lock()
        self.wait_time = wait_time
        
    def wait_if_needed(self, domain):
        """Wait if a request was made to this domain recently"""
        with self.lock:
            last_time = self.domain_last_request.get(domain, 0)
            current_time = time.time()
            
            if current_time - last_time < self.wait_time:
                # Need to wait
                sleep_time = self.wait_time - (current_time - last_time)
                time.sleep(max(0, sleep_time))
            
            # Update last request time
            self.domain_last_request[domain] = time.time()

# Create global domain throttler
domain_throttler = None

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
        NUM_PROCESSES = config.get('NUM_THREADS', multiprocessing.cpu_count())  # Use NUM_THREADS from config for process count
        MAX_RAM_PERCENT = config.get('MAX_RAM_PERCENT', 80)  # Default to 80% RAM utilization
        CHECK_RAM_INTERVAL = config.get('CHECK_RAM_INTERVAL', 30)  # Check RAM every 30 seconds
        JOB_RESULT_PATH = config.get('JOB_RESULT_PATH', 'job_results')
        NUM_PROCESSES = config.get('NUM_PROCESSES', os.cpu_count())
        MAX_TASKS_PER_CHILD = config.get('MAX_TASKS_PER_CHILD', 100)
        NEGATIVE_TERMS = config.get('NEGATIVE_TERMS', [])
except Exception as e:
    print(f"Error loading config.json: {e}")
    SEARCH_TERMS = ['']
    DAYS_LOOKBACK = 7
    FILTER_US_ONLY = False
    MIN_RELEVANCE_SCORE = 0.5
    USE_BERT = True
    NUM_PROCESSES = multiprocessing.cpu_count()
    MAX_RAM_PERCENT = 80
    CHECK_RAM_INTERVAL = 30
    JOB_RESULT_PATH = 'job_results'
    MAX_TASKS_PER_CHILD = 100
    NEGATIVE_TERMS = []

# Set default configs
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

print("Configuration (PARALLEL VERSION):")
print(f"JOB_RESULT_PATH: {JOB_RESULT_PATH}")
print(f"LAST_PROCESSED_PATH: {LAST_PROCESSED_PATH}")
print(f"EXTRACT_START: {EXTRACT_START}")
print(f"EXTRACT_END: {EXTRACT_END}")
print(f"SEARCH_TERMS: {SEARCH_TERMS}")
print(f"DAYS_LOOKBACK: {DAYS_LOOKBACK}")
print(f"FILTER_US_ONLY: {FILTER_US_ONLY}")
print(f"MIN_RELEVANCE_SCORE: {MIN_RELEVANCE_SCORE}")
print(f"USE_BERT: {USE_BERT}")
print(f"NUM_PROCESSES: {NUM_PROCESSES}")
print(f"MAX_RAM_PERCENT: {MAX_RAM_PERCENT}")
print(f"CHECK_RAM_INTERVAL: {CHECK_RAM_INTERVAL}")
print(f"MAX_TASKS_PER_CHILD: {MAX_TASKS_PER_CHILD}")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def fetch_url(method, url, payload, max_retries=3, retry_delay=1):
    """
    Fetch data from URL with retries, error handling, and domain throttling
    
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
    
    # Extract domain from URL and apply throttling
    domain = url.split('/')[2]
    if domain_throttler:
        domain_throttler.wait_if_needed(domain)
    
    for attempt in range(max_retries):
        try:
            if method.upper() == 'GET':
                response = requests.get(url, headers=headers, timeout=10)
            else:
                response = requests.post(url, headers=headers, json=payload, timeout=10)
            
            response.raise_for_status()
            
            # Get raw content first
            content = response.content.decode('utf-8', errors='ignore')
            
            # Try parsing directly first
            try:
                return json.loads(content)
            except json.JSONDecodeError as e:
                print(f"Initial JSON parsing failed for {url}: {e}", file=sys.stderr)
                
                # First cleaning attempt - replace control characters
                cleaned = ''.join(c if (ord(c) >= 32 or c in '\n\r\t') else ' ' for c in content)
                try:
                    return json.loads(cleaned)
                except json.JSONDecodeError:
                    print(f"First cleaning attempt failed for {url}", file=sys.stderr)
                    
                    # Second cleaning attempt - more aggressive
                    try:
                        # Remove all non-printable characters
                        ultra_clean = ''.join(c for c in cleaned if c.isprintable() or c in '\n\r\t')
                        return json.loads(ultra_clean)
                    except json.JSONDecodeError:
                        print(f"All cleaning attempts failed for {url}", file=sys.stderr)
                        
                        # Return safe fallback structure
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
    def __init__(self, temp_dir=None):
        self.job_queue = Queue()
        self.results_lock = Lock()
        self.process_id = os.getpid()
        self.results_file = os.path.join(JOB_RESULT_PATH, f"all_jobs_{self.process_id}.json")
        self.temp_dir = temp_dir or os.path.join(JOB_RESULT_PATH, f"temp_{self.process_id}")
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
            
        print(f"Initialized JobProcessor {self.process_id}. Results will be saved to: {self.results_file}")
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
        """Dump jobs for a specific company with proper file locking"""
        if company_acronym not in self.company_jobs:
            return
            
        jobs = self.company_jobs[company_acronym]
        if not jobs:
            return
            
        # Save to company file with locking
        company_file = os.path.join(JOB_RESULT_PATH, f"{company_acronym}.json")
        try:
            with open(company_file, 'r+' if os.path.exists(company_file) else 'w+') as f:
                # Acquire exclusive lock
                fcntl.flock(f.fileno(), fcntl.LOCK_EX)
                try:
                    # Read existing jobs if any
                    f.seek(0)
                    existing_jobs = json.load(f) if os.path.getsize(company_file) > 0 else []
                except (json.JSONDecodeError, ValueError):
                    existing_jobs = []
                
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
                
                # Write back to file
                f.seek(0)
                f.truncate()
                json.dump(unique_jobs, f, indent=2)
                
                print(f"Process {self.process_id} dumped {len(jobs)} jobs for {company_acronym}")
                
                # Clear from memory
                self.company_jobs[company_acronym] = []
                
        except Exception as e:
            print(f"Process {self.process_id} error dumping jobs for {company_acronym}: {e}")
        finally:
            # Lock is automatically released when file is closed
            pass

    def _dump_temp_jobs(self):
        """Dump jobs to temporary file when memory threshold is reached"""
        if len(self.all_jobs) < self.memory_threshold:
            return
            
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        temp_file = os.path.join(self.temp_dir, f"temp_jobs_{self.process_id}_{timestamp}.json")
        
        try:
            # Sort by relevance score
            self.all_jobs.sort(key=lambda x: x.get('relevance_score', 0), reverse=True)
            
            # Keep top scoring jobs in memory, dump the rest
            keep_jobs = self.all_jobs[:50]  # Keep top 50 in memory
            dump_jobs = self.all_jobs[50:]
            
            if dump_jobs:
                with open(temp_file, 'w') as f:
                    json.dump(dump_jobs, f, indent=2)
                print(f"Process {self.process_id} dumped {len(dump_jobs)} jobs to temporary file: {temp_file}")
            
            # Keep only top scoring jobs in memory
            self.all_jobs = keep_jobs
            
        except Exception as e:
            print(f"Process {self.process_id} error creating temp dump: {e}")

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
            
            # Add to process-specific all jobs if not duplicate
            if not any(existing['internal_id'] == job['internal_id'] for existing in self.all_jobs):
                self.all_jobs.append(job)
                print(f"Process {self.process_id} added job: {job['title']} ({job.get('relevance_score', 0):.2f})")
                
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
        """Save all jobs to process-specific aggregated file"""
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
                print(f"Process {self.process_id} error saving to aggregated file {self.results_file}: {e}")

def calculate_relevance_score(job_obj, search_term, bert_similarity=None):
    """Calculate relevance score with or without BERT"""
    # Get negative terms from config
    negative_terms = NEGATIVE_TERMS
    
    # Get job text
    title = job_obj.get('title', '').lower()
    description = job_obj.get('description', '').lower()
    
    # Check for negative terms first
    negative_score = 0
    for term in negative_terms:
        term = term.lower()
        if term in title:
            negative_score -= 0.4  # Higher penalty for title matches
        if term in description:
            negative_score -= 0.2  # Lower penalty for description matches
    
    if not USE_BERT or not bert_similarity:
        # Simple keyword matching as fallback
        search_lower = search_term.lower()
        
        # Basic scoring: title matches worth more
        score = 0
        if search_lower in title:
            score += 0.6
        if search_lower in description:
            score += 0.4
            
        # Apply negative score
        score = max(0, score + negative_score)
        return score
    
    # BERT-based scoring
    term_mappings = config.get('term_mappings', {})
    
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
    
    # Weight title matches more heavily and apply negative score
    final_score = (title_score * 2 + desc_score) / 3 if (title or description) else 0
    final_score = max(0, final_score + negative_score)  # Apply negative score but don't go below 0
        
    return final_score

def get_memory_usage():
    """Get current memory usage as a percentage"""
    return psutil.virtual_memory().percent

def check_ram_usage(max_percent):
    """Check if RAM usage is above the specified percentage"""
    return get_memory_usage() > max_percent

def worker_process(company_urls: List[str], pipe_conn, process_id: int, ram_status_queue=None):
    """
    Worker process function that handles a subset of company URLs
    
    Args:
        company_urls (List[str]): List of company URLs to process
        pipe_conn: Pipe connection to send results back to main process
        process_id (int): ID of the worker process
        ram_status_queue: Queue to check for RAM status from main process
    """
    try:
        # Initialize job processor for this worker
        job_processor = JobProcessor()
        job_processor.start()
        
        for company_url in company_urls:
            if not company_url.strip():
                continue
                
            # Check if we should pause due to RAM usage
            if ram_status_queue and not ram_status_queue.empty():
                pause_status = ram_status_queue.get()
                if pause_status == 'PAUSE':
                    print(f"Process {process_id}: Pausing due to high RAM usage")
                    # Wait for resume signal
                    while True:
                        if not ram_status_queue.empty():
                            status = ram_status_queue.get()
                            if status == 'RESUME':
                                print(f"Process {process_id}: Resuming processing")
                                break
                        time.sleep(5)
                
            for search_term in SEARCH_TERMS:
                try:
                    print(f"Process {process_id}: Processing {company_url} with search term '{search_term}'")
                    process_link(company_url, search_term, job_processor)
                except Exception as error:
                    print(f'Process {process_id}: Error grabbing jobs for: {company_url}\nError: {error}')
        
        # Stop the job processor and send completion signal
        job_processor.stop()
        pipe_conn.send(('DONE', process_id))
        
    except Exception as e:
        pipe_conn.send(('ERROR', f"Process {process_id} failed: {str(e)}"))
    finally:
        pipe_conn.close()

def process_link(url, search_term, job_processor):
    """Modified process_link function that takes a job_processor instance"""
    found_jobs = []
    old_jobs_count = 0  # Counter for consecutive old jobs

    # Extract URL components
    match = re.search(URL_EXTRACTION_REGEX, url)
    if not match:
        return
    
    URL_BASE = match.group(1)
    WORKDAY_VERSION = match.group(2)
    END_PATH = match.group(3)

    # Generate acronym for the company
    COMPANY_ACRONYM = generate_acronym(URL_BASE)

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

    # Build URLs and payload
    bulk_listings_url = f"https://{URL_BASE}.{WORKDAY_VERSION}.myworkdayjobs.com/wday/cxs/{URL_BASE}/{END_PATH}/jobs"
    specific_listing_url = f"https://{URL_BASE}.{WORKDAY_VERSION}.myworkdayjobs.com/wday/cxs/{URL_BASE}/{END_PATH}"
    
    bulk_listings_payload = {
        "limit": 20,
        "offset": 0,
        "searchText": search_term,
        "appliedFacets": {"locationCountry": [""] if not FILTER_US_ONLY else ["USA"]}
    }

    # Get initial response for job count
    init_res = fetch_url('POST', bulk_listings_url, bulk_listings_payload)
    if init_res.get('error'):
        return

    # Process jobs in batches
    job_seen = 0
    job_total = init_res.get('total', 0)
    page = 0
    
    print(f'Total: {job_total} for search term |{search_term}|')
    
    while job_seen < job_total:
        bulk_listings_payload['offset'] = job_seen
        res = fetch_url('POST', bulk_listings_url, bulk_listings_payload)
        
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
                'title': job_info.get('title'),
                'description': sanitize_html(job_info.get('jobDescription')),
                'internal_id': job_id,
                'location': job_info.get('location'),
                'remote_type': job_info.get('remote_type', 'Onsite'),
                'country': job_info.get('country', {}).get('descriptor'),
                'posted_date': job_info.get('startDate'),
                'url': job_info.get('externalUrl'),
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

# Global reference to ram status queue for worker processes
ram_status_queue = None

def process_company(company_data):
    """
    Process a company's job listings using the process_link function
    
    Args:
        company_data (dict): Dictionary containing company information including URL
        
    Returns:
        dict: Status of the processing operation
    """
    if not isinstance(company_data, dict) or 'url' not in company_data:
        return {'error': 'Invalid company data format'}
        
    company_url = company_data['url']
    # Extract domain for reporting
    match = re.search(URL_EXTRACTION_REGEX, company_url)
    if not match:
        return {'error': f'Invalid URL format: {company_url}'}
        
    company_name = match.group(1)
    
    try:
        # Create a job processor for this company
        job_processor = JobProcessor()
        job_processor.start()
        
        # Process each search term
        processed_jobs = 0
        for search_term in SEARCH_TERMS:
            try:
                print(f"Processing {company_url} with search term '{search_term}'")
                # Use the existing process_link function to fetch and process jobs
                process_link(company_url, search_term, job_processor)
                processed_jobs += len(job_processor.all_jobs)
            except Exception as error:
                print(f'Error processing {company_url} with search term "{search_term}": {error}')
        
        # Stop the job processor and finalize
        job_processor.stop()
        
        return {
            'status': 'success',
            'message': f'Processed company: {company_name}',
            'jobs_count': processed_jobs
        }
            
    except Exception as e:
        return {'error': f'Failed to process {company_name}: {str(e)}'}

def extract_workday_jobs(job_file_path):
    """
    Extract jobs from companies listed in the job file using multiprocessing.
    
    Args:
        job_file_path (str): Path to the file containing company URLs (one URL per line)
    """
    try:
        # Read URLs from text file (one URL per line)
        with open(job_file_path, 'r') as f:
            company_urls = [line.strip() for line in f if line.strip()]
            
        total_companies = len(company_urls)
        logging.info(f"Processing {total_companies} companies")
        
        # Create company data objects
        companies = [{'url': url} for url in company_urls]
        
        # Initialize process pool with RAM monitoring
        max_workers = min(NUM_PROCESSES, len(companies))
        with ProcessPoolExecutor(max_workers=max_workers, 
                               max_tasks_per_child=MAX_TASKS_PER_CHILD) as executor:
            futures = []
            
            # Submit jobs to the pool
            for company in companies:
                futures.append(executor.submit(process_company, company))
                
            # Monitor and process results
            successful_companies = 0
            total_jobs = 0
            
            for future in as_completed(futures):
                try:
                    result = future.result()
                    if result.get('status') == 'success':
                        successful_companies += 1
                        total_jobs += result.get('jobs_count', 0)
                        logging.info(result['message'])
                    else:
                        logging.error(f"Failed to process company: {result.get('error', 'Unknown error')}")
                        
                    # Check RAM usage
                    ram_percent = psutil.virtual_memory().percent
                    if ram_percent > MAX_RAM_PERCENT:
                        logging.warning(f"High RAM usage detected: {ram_percent}%. Consider reducing max_workers.")
                        
                except Exception as e:
                    logging.error(f"Error processing company: {str(e)}")
                    
        # Log final statistics
        logging.info(f"Processing complete. Successfully processed {successful_companies}/{total_companies} companies.")
        logging.info(f"Total relevant jobs found: {total_jobs}")
        
    except Exception as e:
        logging.error(f"Failed to process job file {job_file_path}: {str(e)}")

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

def test_multiprocessing():
    """Test function to verify multiprocessing is working correctly"""
    print("Testing multiprocessing functionality...")
    
    def test_worker(i):
        print(f"Worker {i} started (PID: {os.getpid()})")
        time.sleep(2)
        return f"Worker {i} result from PID {os.getpid()}"
    
    with concurrent.futures.ProcessPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(test_worker, i) for i in range(5)]
        
        for future in concurrent.futures.as_completed(futures):
            print(future.result())
    
    print("Multiprocessing test completed successfully")

def _merge_all_result_files():
    """Merge all process-specific result files into a single result file"""
    all_jobs = []
    
    # Get all process-specific result files
    result_files = [f for f in os.listdir(JOB_RESULT_PATH) 
                   if f.startswith("all_jobs_") and f.endswith(".json")]
    
    # Read and merge all jobs
    for result_file in result_files:
        try:
            with open(os.path.join(JOB_RESULT_PATH, result_file), 'r') as f:
                jobs = json.load(f)
                all_jobs.extend(jobs)
        except Exception as e:
            print(f"Error reading {result_file}: {e}")
    
    # Deduplicate based on internal_id
    unique_jobs = []
    seen_ids = set()
    
    for job in all_jobs:
        job_id = job.get('internal_id')
        if job_id and job_id not in seen_ids:
            seen_ids.add(job_id)
            unique_jobs.append(job)
    
    # Sort and save final results
    unique_jobs.sort(
        key=lambda x: (
            datetime.fromisoformat(x.get('posted_date', '').replace('Z', '+00:00')),
            x.get('relevance_score', 0)
        ),
        reverse=True
    )
    
    # Write to final results file with proper locking
    final_file = os.path.join(JOB_RESULT_PATH, "all_jobs_final.json")
    with open(final_file, 'w') as f:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        try:
            json.dump(unique_jobs, f, indent=2)
        finally:
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)

def cleanup_temp_files():
    """Remove all temporary files and directories"""
    # Find all temp directories
    temp_dirs = [d for d in os.listdir(JOB_RESULT_PATH) 
                if os.path.isdir(os.path.join(JOB_RESULT_PATH, d)) and d.startswith("temp_")]
    
    # Remove each temp directory
    for temp_dir in temp_dirs:
        try:
            shutil.rmtree(os.path.join(JOB_RESULT_PATH, temp_dir))
        except Exception as e:
            print(f"Error removing temp directory {temp_dir}: {e}")
    
    # Remove process-specific result files
    result_files = [f for f in os.listdir(JOB_RESULT_PATH) 
                   if f.startswith("all_jobs_") and f.endswith(".json") and f != "all_jobs_final.json"]
    
    for result_file in result_files:
        try:
            os.remove(os.path.join(JOB_RESULT_PATH, result_file))
        except Exception as e:
            print(f"Error removing result file {result_file}: {e}")

def check_ram_status(process_id):
    """Check for RAM pause flag in a thread-safe way"""
    pause_file = os.path.join(JOB_RESULT_PATH, "PAUSE_FLAG")
    
    if os.path.exists(pause_file):
        print(f"Process {process_id}: Pausing due to high RAM usage")
        # Wait until flag is removed
        while os.path.exists(pause_file):
            time.sleep(5)
        print(f"Process {process_id}: Resuming processing")
        return True
    return False

class FileLock:
    """A file locking mechanism that has context-manager support with timeouts."""
    
    def __init__(self, file_path, timeout=10):
        self.file_path = file_path
        self.timeout = timeout
        self.lockfile = f"{file_path}.lock"
        self.fd = None
        
    @contextmanager
    def acquire(self):
        start_time = time.time()
        while True:
            try:
                self.fd = open(self.lockfile, 'w')
                fcntl.flock(self.fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except (IOError, OSError):
                if time.time() - start_time >= self.timeout:
                    raise TimeoutError(f"Could not acquire lock for {self.file_path} within {self.timeout} seconds")
                time.sleep(0.1)
        
        try:
            yield
        finally:
            if self.fd:
                fcntl.flock(self.fd.fileno(), fcntl.LOCK_UN)
                self.fd.close()
                try:
                    os.unlink(self.lockfile)
                except OSError:
                    pass

def save_jobs_atomic(jobs, output_file):
    """Save jobs to file atomically using file locking"""
    lock = FileLock(output_file)
    with lock.acquire():
        # Read existing jobs if file exists
        existing_jobs = []
        if os.path.exists(output_file):
            try:
                with open(output_file, 'r') as f:
                    existing_jobs = json.load(f)
            except json.JSONDecodeError:
                logging.error(f"Error reading existing jobs from {output_file}")
                existing_jobs = []
        
        # Merge new jobs with existing ones, avoiding duplicates
        all_jobs = existing_jobs + [job for job in jobs if job not in existing_jobs]
        
        # Write to temporary file first
        temp_file = f"{output_file}.tmp"
        with open(temp_file, 'w') as f:
            json.dump(all_jobs, f, indent=2)
        
        # Atomic rename
        os.rename(temp_file, output_file)

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
        
    logging.info(f"Consolidating results into: {output_file}")
    
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
            logging.error(f"Error reading {job_file}: {e}")
    
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
        logging.info(f"Saved {len(unique_jobs)} unique jobs to {output_file}")
    except Exception as e:
        logging.error(f"Error saving consolidated results: {e}")
    
    return len(unique_jobs)

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
    
    print("Configuration (PARALLEL VERSION):")
    print(f"JOB_RESULT_PATH: {JOB_RESULT_PATH}")
    print(f"LAST_PROCESSED_PATH: {LAST_PROCESSED_PATH}")
    print(f"EXTRACT_START: {EXTRACT_START}")
    print(f"EXTRACT_END: {EXTRACT_END}")
    print(f"SEARCH_TERMS: {SEARCH_TERMS}")
    print(f"DAYS_LOOKBACK: {DAYS_LOOKBACK}")
    print(f"FILTER_US_ONLY: {FILTER_US_ONLY}")
    print(f"MIN_RELEVANCE_SCORE: {MIN_RELEVANCE_SCORE}")
    print(f"USE_BERT: {USE_BERT}")
    print(f"NUM_PROCESSES: {NUM_PROCESSES}")
    print(f"MAX_RAM_PERCENT: {MAX_RAM_PERCENT}")
    print(f"CHECK_RAM_INTERVAL: {CHECK_RAM_INTERVAL}")
    print(f"MAX_TASKS_PER_CHILD: {MAX_TASKS_PER_CHILD}")
    
    # Initialize global domain throttler
    domain_throttler = DomainThrottler()
    
    # Run the extraction
    extract_workday_jobs('./all_myworkdayjobs_links.txt')
    
    # Consolidate all results into a single file
    total_jobs = consolidate_results(JOB_RESULT_PATH)
    print(f'DONE - PARALLEL VERSION - {total_jobs} unique jobs consolidated')