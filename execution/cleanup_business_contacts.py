#!/usr/bin/env python3
"""
Cleanup existing business contacts by applying stricter name cleaning and filtering.
Deletes 'fuckall' results (generic titles, listicles) and cleans trailing noise.
"""

import os
import sys
import re
import json
import logging
import argparse
from urllib.parse import urlparse
from dotenv import load_dotenv
from pathlib import Path

# Setup path to include project root
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from supabase import create_client, Client

env_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(env_path)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_SERVICE_ROLE_KEY') or os.getenv('SUPABASE_KEY')

if not SUPABASE_URL or not SUPABASE_KEY:
    logger.error("Supabase credentials not found in .env")
    sys.exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- Logic copied from business_search.py for standalone reliability ---

GENERIC_TITLES = {
    'home', 'homepage', 'about', 'about us', 'contact', 'contact us', 
    'services', 'pricing', 'login', 'register', 'sign up', 'careers',
    'jobs', 'portfolio', 'gallery', 'blog', 'news', 'privacy policy', 'terms'
}

REJECT_PATTERNS = [
    r'^top \d+', r'^best \d+', r'^\d+ best', r'^\d+ top',
    r'^how to', r'^why ', r'^what is', r'^where to',
    r'^find ', r'^get ', r'^compare ', r'^list of', r'^a guide'
]

def _clean_business_name(title: str) -> str:
    if not title: return ''
    if title.lower().strip() in GENERIC_TITLES: return ''
    
    name = title
    for sep in (' | ', ' - ', ' – ', ' — ', ' · ', ' • '):
        if sep in title:
            name = title.split(sep)[0].strip()
            if name.lower() in GENERIC_TITLES and len(title.split(sep)) > 1:
                name = title.split(sep)[1].strip()
            break
            
    name = name.strip(' -|–—.,;:"\' ')
    if len(name) < 3: return ''
    if name.lower() in GENERIC_TITLES: return ''
    if len(name) > 60: return ''
    
    name_lower = name.lower()
    for pattern in REJECT_PATTERNS:
        if re.search(pattern, name_lower): return ''
        
    if any(name_lower.startswith(s) for s in ('the best ', 'top ', 'best ', '10 ', '5 ', '7 ', '44 ')):
        return ''
    return name

def run_cleanup(project_id: str, dry_run: bool = False):
    logger.info(f"Starting cleanup for project: {project_id} (Dry Run: {dry_run})")
    
    # Fetch all contacts for the project
    result = supabase.table('contacts').select('*').eq('project_id', project_id).execute()
    contacts = result.data or []
    
    logger.info(f"Found {len(contacts)} contacts to process.")
    
    deleted = 0
    updated = 0
    skipped = 0
    
    for contact in contacts:
        cid = contact['id']
        old_name = contact['name']
        
        # 1. Check if it's a generic "fuckall" result
        new_name = _clean_business_name(old_name)
        
        # If the name is now empty, it means it's a generic title or listicle
        if not new_name:
            logger.info(f"  [DELETE] Generic/Listicle: '{old_name}'")
            if not dry_run:
                supabase.table('contacts').delete().eq('id', cid).execute()
            deleted += 1
            continue
            
        # 2. Check if name can be cleaned further
        if new_name != old_name:
            logger.info(f"  [UPDATE] '{old_name}' -> '{new_name}'")
            if not dry_run:
                supabase.table('contacts').update({'name': new_name}).eq('id', cid).execute()
            updated += 1
        else:
            skipped += 1
            
    logger.info(f"Cleanup complete: {deleted} deleted, {updated} updated, {skipped} kept unchanged.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Cleanup business contact names")
    parser.add_argument("--project_id", required=True, help="Project ID to cleanup")
    parser.add_argument("--dry_run", action="store_true", help="Don't apply changes")
    args = parser.parse_args()
    
    run_cleanup(args.project_id, args.dry_run)
