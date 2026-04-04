#!/usr/bin/env python3
"""
Enrich Company — Scrape website via Jina and extract 4-pillar context via Gemini.
"""

import os
import sys
import json
import logging
import argparse
import requests
import time
from datetime import datetime
from google import genai
from typing import Optional, List, Dict

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dotenv import load_dotenv
from pathlib import Path

env_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(env_path)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')

def scrape_website_with_jina(url: str) -> Optional[str]:
    """Scrape the given URL using Jina Reader API."""
    if not url.startswith('http'):
        url = 'https://' + url
        
    try:
        jina_url = f"https://r.jina.ai/{url}"
        logger.info(f"    Scraping with Jina: {jina_url}")
        # Jina requires an Authorization header with a Bearer token if you have one, 
        # but it works primarily without one for basic usage. We will use a basic request.
        # Let's add a user-agent just in case
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(jina_url, headers=headers, timeout=20)
        
        if response.status_code == 200 and response.text:
            text = response.text
            # Keep it reasonable, max 15000 characters
            if len(text) > 15000:
                text = text[:15000]
            return text
        else:
            logger.warning(f"    Jina returned status {response.status_code}")
            return None
    except Exception as e:
        logger.error(f"    Jina scrape error: {e}")
        return None

def extract_company_context(markdown_text: str) -> Optional[Dict]:
    """Use Gemini to extract the 4-pillar context from the raw markdown."""
    if not GEMINI_API_KEY:
        logger.error("    No GEMINI_API_KEY configured.")
        return None

    system_instruction = """
Analyze the following company data and extract only verifiable information from the text.
Do not fabricate or infer missing details. If specific details are missing, state "Not mentioned in the text."

Output a clean JSON object exactly matching this structure, with NO markdown formatting outside of the JSON block (just raw '{...}'):
{
  "mission_and_about": "Company History, Mission Statement, Vision, Core Values",
  "offerings_and_positioning": "Core Products & Services, Differentiators, Target Customers",
  "process_and_differentiation": "Company Methodology, Step-by-Step Process, Unique Value Proposition",
  "proof_of_success": "Case Studies, Testimonials, Notable Clients, Awards, Metrics"
}
"""
    try:
        client = genai.Client(api_key=GEMINI_API_KEY)
        response = client.models.generate_content(
            model='gemini-2.5-flash',
            contents=system_instruction + "\n\nInput Markdown Data:\n" + markdown_text,
        )
        content = response.text.strip()
        
        # Clean up possible markdown enclosures
        if '```json' in content: 
            content = content.split('```json')[1].split('```')[0].strip()
        elif '```' in content: 
            content = content.split('```')[1].split('```')[0].strip()
            
        data = json.loads(content)
        return dict(data)
    except Exception as e:
        logger.error(f"    Gemini extraction error: {e}")
        return None

def enrich_companies_bulk(limit: int = 50, project_id: str = None, contact_ids: list = None, dry_run: bool = False) -> dict:
    from supabase import create_client
    
    supabase_url = os.getenv('SUPABASE_URL')
    supabase_key = os.getenv('SUPABASE_SERVICE_ROLE_KEY') or os.getenv('SUPABASE_KEY')
    
    if not supabase_url or not supabase_key:
        return {'error': 'No Supabase credentials'}

    supabase = create_client(supabase_url, supabase_key)
    
    query = supabase.table('contacts').select('*')
    if contact_ids and len(contact_ids) > 0:
        logger.info(f"  Fetching specific contact IDs: {len(contact_ids)}")
        query = query.in_('id', contact_ids).range(0, 1000)
    elif project_id:
        logger.info(f"  Fetching contacts for project {project_id} (limit {limit})")
        query = query.eq('project_id', project_id).limit(limit)
    else:
        query = query.limit(limit)
        
    result = query.execute()
    contacts = result.data or []
    
    logger.info(f"Found {len(contacts)} contacts to process for company enrichment")
    
    stats = {'processed': 0, 'enriched': 0, 'errors': 0, 'skipped': 0}
    
    for i, contact in enumerate(contacts):
        logger.info(f"[{i+1}/{len(contacts)}] Processing: {contact.get('name', 'Unknown')} at {contact.get('company', 'Unknown')}")
        try:
            # Parse existing enrichment_data
            ed = contact.get('enrichment_data')
            if isinstance(ed, str):
                try: ed = json.loads(ed)
                except: ed = {}
            elif not isinstance(ed, dict):
                ed = {}
                
            # Skip if already enriched
            if 'company_context' in ed and ed['company_context']:
                logger.info("    ⏭️ Skipping: company_context already exists.")
                stats['skipped'] += 1
                continue
                
            # Determine URL
            url = contact.get('website')
            if not url:
                email = contact.get('email')
                if email and '@' in email:
                    domain = email.split('@')[1]
                    skip_domains = ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'icloud.com']
                    if domain.lower() not in skip_domains:
                        url = domain
                        logger.info(f"    Derived URL from email: {url}")
            
            if not url:
                logger.warning("    ❌ No website or valid email domain found. Skipping.")
                stats['skipped'] += 1
                continue
                
            stats['processed'] += 1
            
            markdown_content = scrape_website_with_jina(url)
            if not markdown_content:
                logger.warning("    ❌ Failed to scrape website or website is empty.")
                continue
                
            logger.info("    Extracting 4-pillar context via Gemini...")
            context_json = extract_company_context(markdown_content)
            
            if context_json:
                logger.info("    ✅ Successfully extracted company context.")
                ed['company_context'] = context_json
                
                updates = {
                    'enrichment_data': ed,
                    'updated_at': datetime.utcnow().isoformat()
                }
                if not dry_run:
                    supabase.table('contacts').update(updates).eq('id', contact['id']).execute()
                stats['enriched'] += 1
            else:
                logger.warning("    ❌ Gemini failed to extract context.")
                stats['errors'] += 1
                
            time.sleep(1) # Mild rate limiting
            
        except Exception as e:
            logger.error(f"Error enriching company for {contact.get('id')}: {e}")
            stats['errors'] += 1
            
    logger.info(f"Company Enrichment Complete: {stats}")
    return stats

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--limit', type=int, default=50)
    parser.add_argument('--id', type=str, help="Specific contact ID to enrich")
    parser.add_argument('--project-id', type=str, help="Specific project ID")
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()
    
    ids = [args.id] if args.id else None
    res = enrich_companies_bulk(limit=args.limit, project_id=args.project_id, contact_ids=ids, dry_run=args.dry_run)
    print(json.dumps(res, indent=2))
