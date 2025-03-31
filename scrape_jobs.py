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
from datetime import datetime, timedelta
from transformers import BertTokenizer, BertModel
import torch
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
from queue import Queue
from threading import Thread, Lock
import threading

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
except Exception as e:
    print(f"Error loading config.json: {e}")
    SEARCH_TERMS = ['']
    DAYS_LOOKBACK = 7
    FILTER_US_ONLY = False
    MIN_RELEVANCE_SCORE = 0.5
    USE_BERT = True

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

print("Configuration:")
print(f"JOB_RESULT_PATH: {JOB_RESULT_PATH}")
print(f"LAST_PROCESSED_PATH: {LAST_PROCESSED_PATH}")
print(f"EXTRACT_START: {EXTRACT_START}")
print(f"EXTRACT_END: {EXTRACT_END}")
print(f"SEARCH_TERMS: {SEARCH_TERMS}")
print(f"DAYS_LOOKBACK: {DAYS_LOOKBACK}")
print(f"FILTER_US_ONLY: {FILTER_US_ONLY}")
print(f"MIN_RELEVANCE_SCORE: {MIN_RELEVANCE_SCORE}")
print(f"USE_BERT: {USE_BERT}")

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
        self.bert_similarity = BertSimilarity() if USE_BERT else None
        self.processor_thread = None
        self.is_running = False
        
        # Initialize or load results file
        if os.path.exists(self.results_file):
            with open(self.results_file, 'r') as f:
                self.all_jobs = json.load(f)
        else:
            self.all_jobs = []
            self._save_results()

    def start(self):
        """Start the job processor thread"""
        if USE_BERT and not self.processor_thread:
            self.is_running = True
            self.processor_thread = Thread(target=self._process_jobs)
            self.processor_thread.daemon = True
            self.processor_thread.start()

    def stop(self):
        """Stop the job processor thread"""
        self.is_running = False
        if self.processor_thread:
            self.job_queue.put(None)  # Sentinel to stop the thread
            self.processor_thread.join()
            self.processor_thread = None

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

    def _save_job(self, job):
        """Save a job to the results file"""
        with self.results_lock:
            # Check for duplicates
            if not any(existing['internal_id'] == job['internal_id'] for existing in self.all_jobs):
                self.all_jobs.append(job)
                self._save_results()

    def _save_results(self):
        """Save all jobs to file"""
        with self.results_lock:
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

def extract_workday_jobs(job_file_path):
    LOGGING = True
    THROTTLE_WAIT_TIME = 10000  # ms
    url_extraction_regex = r'https://([a-zA-Z0-9_]*).([a-zA-Z0-9_]*).[a-zA-Z0-9_]*.com/([a-zA-Z0-9_]*)'
    html_replacement_regex = r'<[^>]*>'
    company_num = -1
    OLD_JOBS_THRESHOLD = 3  # Stop if we find this many old jobs in a row

    # Initialize job processor
    job_processor = JobProcessor()
    job_processor.start()

    def log(*args):
        if LOGGING:
            print(*args)
    
    def wait(time_ms):
        time.sleep(time_ms / 1000)

    def sanitize_html(text):
        if not text:
            return ""
        return re.sub(html_replacement_regex, '', html.unescape(text))

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
            log('REJECT LOCATION:', job_obj.get('title'))
            return False
        
        # Filter: Job must be after last_date
        try:
            job_date = datetime.fromisoformat(job_obj.get('posted_date', '').replace('Z', '+00:00'))
            if last_date > job_date:
                log('REJECT TIME:', job_obj.get('title'))
                return False
        except (ValueError, TypeError):
            pass

        return True
    
    def process_link(url, search_term):
        found_jobs = []
        old_jobs_count = 0  # Counter for consecutive old jobs

        # Extract URL components
        match = re.search(url_extraction_regex, url)
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

        log('\nGetting Jobs for:', COMPANY_ACRONYM, f"({URL_BASE}.{WORKDAY_VERSION})",
            f"Last: {last_scraped.isoformat()}")

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
        
        log('Total:', job_total, 'for search term', f"|{search_term}|")
        
        while job_seen < job_total:
            bulk_listings_payload['offset'] = job_seen
            res = fetch_url('POST', bulk_listings_url, bulk_listings_payload)
            
            if res.get('error'):
                log('PAGE ERROR')
                break
            
            jobs = res.get('jobPostings', [])
            job_seen += len(jobs)
            page += 1
            
            log('=======', job_seen - len(jobs), '-', job_seen, 'of', job_total, '=======')
            
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
                    log('DUPLICATE:', job_info.get('title'))
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
                    log('SUCCESS:', job_obj['title'])
                    job_processor.add_job(job_obj)  # Add job to processor queue
                    old_jobs_count = 0
                else:
                    old_jobs_count += 1
                    if old_jobs_count >= OLD_JOBS_THRESHOLD:
                        log(f"Found {OLD_JOBS_THRESHOLD} old jobs in a row, stopping search")
                        return

    # Process each company URL
    with open(job_file_path, 'r') as f:
        for company_num, company in enumerate(f):
            company = company.strip()
            if not company:
                continue

            if company_num < EXTRACT_START or company_num > EXTRACT_END:
                continue

            for search_term in SEARCH_TERMS:
                log(f"\nProcessing company {company_num} with search term: '{search_term}'")
                try:
                    process_link(company, search_term)
                except Exception as error:
                    print('Error grabbing jobs for:', company, '\nError:', error)

    # Post-process all job files
    log("\nPost-processing job files...")
    for filename in os.listdir(JOB_RESULT_PATH):
        if filename.endswith('.json'):
            post_process_jobs(os.path.join(JOB_RESULT_PATH, filename))

    # Ensure processor thread is stopped
    job_processor.stop()

if __name__ == "__main__":
    extract_workday_jobs('./all_myworkdayjobs_links.txt')
    print('DONE')