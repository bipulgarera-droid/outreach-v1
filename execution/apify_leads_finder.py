#!/usr/bin/env python3
"""
Apify Leads Finder — Find verified B2B leads via Apify actor.
Uses 'code_crafter/leads-finder'.
"""

import os
import sys
import json
import logging
import argparse
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from apify_client import ApifyClient
from supabase import create_client

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Load env variables
env_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(env_path)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_apify_leads_search(params: dict, project_id: str = None):
    """Run the Apify leads-finder actor and store results in Supabase."""
    api_key = os.getenv('APIFY_API_KEY')
    if not api_key:
        logger.error("No APIFY_API_KEY found in environment")
        return None

    client = ApifyClient(api_key)
    
    # Clean inputs: remove empty arrays/strings
    run_input = {}
    for k, v in params.items():
        if v:  # Only add if it has a value
            if isinstance(v, str) and (k.startswith("company_") or k.startswith("contact_") or k == "size" or k == "email_status"):
                # Split comma separated strings into lists
                run_input[k] = [x.strip() for x in v.split(",") if x.strip()]
            else:
                run_input[k] = v

    run_input.setdefault("email_status", ["validated"])
    run_input.setdefault("fetch_count", 100)
    
    # We always need a file_name or label
    query_label = "Leads Search"
    if run_input.get("contact_job_title"):
        titles = run_input["contact_job_title"]
        query_label = titles[0] if isinstance(titles, list) else titles

    logger.info(f"Starting Apify Actor: code_crafter/leads-finder...")
    logger.info(f"Parameters: {json.dumps(run_input)}")

    try:
        run = client.actor("code_crafter/leads-finder").call(run_input=run_input)
        logger.info(f"Actor run finished. ID: {run.get('id')}")
        
        dataset_id = run.get("defaultDatasetId")
        results = list(client.dataset(dataset_id).iterate_items())
        logger.info(f"Fetched {len(results)} leads from Apify.")

        if not results:
            return {'inserted': 0, 'skipped': 0, 'message': 'No leads found.'}

        return store_apify_results(results, query_label, project_id)

    except Exception as e:
        logger.error(f"Apify execution failed: {e}")
        return None

def store_apify_results(results: list[dict], query_label: str, project_id: str = None):
    """Clean and store Apify results in Supabase contacts table."""
    supabase_url = os.getenv('SUPABASE_URL')
    supabase_key = os.getenv('SUPABASE_SERVICE_ROLE_KEY') or os.getenv('SUPABASE_KEY')
    
    if not supabase_url or not supabase_key:
        logger.error("Supabase credentials missing")
        return None

    supabase = create_client(supabase_url, supabase_key)
    source_str = f"apify_leads: {query_label}"
    
    # Fetch existing for deduplication within project
    existing_emails = set()
    existing_linkedin = set()
    if project_id:
        # Proper pagination to fetch ALL current leads in this project for deduplication
        all_data = []
        offset = 0
        while True:
            res = supabase.table('contacts').select('email, linkedin_url').eq('project_id', project_id).range(offset, offset + 999).execute()
            if not res.data:
                break
            all_data.extend(res.data)
            if len(res.data) < 1000:
                break
            offset += 1000
            
        for r in all_data:
            if r.get('email'): existing_emails.add(r['email'].lower())
            if r.get('linkedin_url'): existing_linkedin.add(r['linkedin_url'].lower())

    leads_to_insert = []
    for item in results:
        person = item.get('Person', {}) or {}
        company = item.get('Company', {}) or {}
        context = item.get('Context', {}) or {}
        
        # Sometimes results aren't nested under Person/Company, so fallback
        first_name = person.get('first_name') or item.get('first_name') or ""
        last_name = person.get('last_name') or item.get('last_name') or ""
        name = person.get('full_name') or item.get('full_name') or f"{first_name} {last_name}".strip() or "Unknown Person"
        
        # Extracting email: check for all variations (personal, work, etc.)
        email = person.get('email') or item.get('email') or person.get('personal_email') or item.get('personal_email') or person.get('work_email') or item.get('work_email') or ""
        linkedin = person.get('linkedin') or item.get('linkedin') or ""
        
        company_name = company.get('company_name') or item.get('company_name') or "Unknown Company"
        domain = company.get('company_website') or company.get('company_domain') or item.get('company_website') or item.get('company_domain') or ""
        
        job_title = person.get('job_title') or item.get('job_title') or ""
        
        # Dedupe by email or linkedin
        email_clean = email.lower().strip() if email else ""
        li_clean = linkedin.lower().strip() if linkedin else ""
        
        if (email_clean and email_clean in existing_emails) or (li_clean and li_clean in existing_linkedin):
            continue
            
        if email_clean: existing_emails.add(email_clean)
        if li_clean: existing_linkedin.add(li_clean)

        city = person.get('city') or item.get('city') or ''
        state = person.get('state') or item.get('state') or ''
        country = person.get('country') or item.get('country') or ''
        location_parts = [p for p in [city, state, country] if p]
        location_str = ', '.join(location_parts) if location_parts else None

        leads_to_insert.append({
            'name': name,
            'email': email_clean if email_clean else None,
            'linkedin_url': li_clean if li_clean else None,
            'company': company_name,
            'phone': person.get('mobile_number') or item.get('mobile_number') or None,
            'location': location_str,
            'niche': company.get('industry') or item.get('industry') or None,
            'source_url': domain if domain else None,
            'project_id': project_id,
            'status': 'new',
            'source': source_str,
            'enrichment_data': {
                'job_title': job_title,
                'city': city or None,
                'state': state or None,
                'country': country or None,
                'industry': company.get('industry') or item.get('industry'),
                'website': company.get('company_website') or item.get('company_website'),
                'company_website': company.get('company_website') or item.get('company_website'),
                'company_size': company.get('company_size') or item.get('company_size'),
                'company_linkedin': company.get('company_linkedin') or item.get('company_linkedin'),
                'seniority_level': person.get('seniority_level') or item.get('seniority_level'),
                'functional_level': person.get('functional_level') or item.get('functional_level'),
                'personal_email': person.get('personal_email') or item.get('personal_email'),
                'headline': person.get('headline') or item.get('headline'),
                'company_founded_year': item.get('company_founded_year'),
                'company_annual_revenue': item.get('company_annual_revenue_clean'),
                'keywords': context.get('keywords') or item.get('keywords'),
                'technologies': context.get('company_technologies') or item.get('company_technologies')
            }
        })

    if leads_to_insert:
        logger.info(f"Inserting {len(leads_to_insert)} new leads from Apify...")
        for i in range(0, len(leads_to_insert), 100):
            batch = leads_to_insert[i:i+100]
            supabase.table('contacts').insert(batch).execute()
        return {'inserted': len(leads_to_insert), 'skipped': len(results) - len(leads_to_insert), 'message': f"Found and added {len(leads_to_insert)} new validated leads!"}
    
    logger.info("No new unique leads to insert.")
    return {'inserted': 0, 'skipped': len(results), 'message': f"Found {len(results)} leads, but all were duplicates."}

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Search Leads via Apify Leads Finder')
    parser.add_argument('--query', help='JSON payload of input arguments')
    parser.add_argument('--project_id', help='Supabase Project ID')

    args = parser.parse_args()
    
    if args.query:
        params = json.loads(args.query)
    else:
        # Default test
        params = {
            "contact_job_title": ["founder", "ceo"],
            "contact_location": ["india"],
            "email_status": ["validated"],
            "fetch_count": 5
        }
        
    pid = args.project_id
    if not pid:
        try:
            url = os.getenv('SUPABASE_URL')
            key = os.getenv('SUPABASE_SERVICE_ROLE_KEY') or os.getenv('SUPABASE_KEY')
            sb = create_client(url, key)
            res = sb.table('projects').select('id').limit(1).execute()
            if res.data: pid = res.data[0]['id']
        except: pass

    stats = run_apify_leads_search(params, pid)
    print(json.dumps(stats, indent=2))
