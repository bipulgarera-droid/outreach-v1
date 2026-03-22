#!/usr/bin/env python3
"""
Cleanup Duplicates — Merge and delete duplicate contact records.
Finds contacts in the same project with the same email address.
Keeps the 'best' one and reassigns all sequences to it.
"""

import os
import sys
import json
import logging
from collections import defaultdict

import re
from collections import defaultdict

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dotenv import load_dotenv
from pathlib import Path
from supabase import create_client

env_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(env_path)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def _normalize_name(name: str) -> str:
    """Normalize name for fuzzy matching (lowercase, no punctuation, remove common industry words)."""
    if not name: return ""
    # Remove punctuation and lowercase
    n = re.sub(r'[^\w\s]', '', name.lower())
    # Remove common industry fillers
    junk = ['productions', 'production', 'films', 'film', 'media', 'studios', 'studio', 'vfx', 'creative', 'agency', 'group', 'pvt', 'ltd', 'limited']
    parts = [p for p in n.split() if p not in junk and len(p) > 2]
    return ' '.join(parts)

def _get_domain(email: str, website: str) -> str:
    """Extract clean domain from email or website."""
    url = email or website or ""
    if not url: return ""
    
    # Extract domain part
    if '@' in url:
        domain = url.split('@')[-1]
    else:
        domain = re.sub(r'https?://(www\.)?', '', url).split('/')[0]
    
    # Remove common public suffixes including .live
    domain = re.sub(r'\.(com|net|org|in|biz|ai|co\.in|io|me|tv|us|ae|live)$', '', domain, flags=re.IGNORECASE)
    
    # NEW: Remove common industry fillers from domain itself
    industry_junk = ['productions', 'production', 'films', 'film', 'media', 'studios', 'studio', 'vfx', 'creative', 'agency', 'works']
    for word in industry_junk:
        if domain.endswith(word) and len(domain) > len(word) + 2:
            domain = domain[:-len(word)]
            break

    # Skip generic domains
    if domain in ['gmail', 'outlook', 'hotmail', 'yahoo', 'protonmail', 'icloud', 'msn']:
        return ""
    return domain.lower().strip()

def cleanup_duplicates(project_id=None):
    url = os.getenv('SUPABASE_URL')
    key = os.getenv('SUPABASE_SERVICE_ROLE_KEY') or os.getenv('SUPABASE_KEY')
    if not url or not key:
        logger.error("Supabase credentials not configured")
        return {'merged': 0, 'deleted': 0, 'errors': 0}

    supabase = create_client(url, key)

    # 1. Fetch all contacts
    logger.info("Fetching contacts for deduplication...")
    query = supabase.table('contacts').select('*')
    if project_id:
        query = query.eq('project_id', project_id)
    
    res = query.execute()
    contacts = res.data or []
    logger.info(f"Loaded {len(contacts)} contacts.")

    # Pass 1: Build name -> domain map
    name_to_domain = {}
    for c in contacts:
        ed = c.get('enrichment_data')
        if isinstance(ed, str):
            try: ed = json.loads(ed)
            except: ed = {}
        else: ed = ed or {}
        
        email = (c.get('email') or '').lower()
        website = (ed.get('website') if ed else '')
        domain = _get_domain(email, website)
        
        name = (c.get('name') or c.get('company') or '').strip()
        norm_name = _normalize_name(name)
        
        if domain and norm_name and len(norm_name) > 3:
            name_to_domain[norm_name] = domain

    # Pass 2: Grouping
    final_groups = defaultdict(list)
    for c in contacts:
        ed = c.get('enrichment_data')
        if isinstance(ed, str):
            try: ed = json.loads(ed)
            except: ed = {}
        else: ed = ed or {}
        
        email = (c.get('email') or '').lower()
        website = (ed.get('website') if ed else '')
        domain = _get_domain(email, website)
        
        name = (c.get('name') or c.get('company') or '').strip()
        norm_name = _normalize_name(name)
        
        # Use domain if available, otherwise check if we can map the name to a domain
        key = domain if domain else name_to_domain.get(norm_name, norm_name)
        if key:
            final_groups[key].append(c)

    stats = {'merged': 0, 'deleted': 0, 'errors': 0}
    
    for key, group in final_groups.items():
        if len(group) <= 1:
            continue
            
        # Priority: replied > in_sequence > enriched > new
        # Also prefer those with emails/websites
        def _score(c):
            s = 0
            status = c.get('status', 'new')
            if status == 'replied': s += 100
            elif status == 'in_sequence': s += 50
            elif status == 'enriched': s += 25
            
            if c.get('email'): s += 10
            ed = c.get('enrichment_data')
            if ed and (isinstance(ed, str) and 'website' in ed or isinstance(ed, dict) and ed.get('website')):
                s += 5
            
            # Prefer non-empty names
            if c.get('name'): s += 2
            return s

        group.sort(key=_score, reverse=True)
        primary = group[0]
        duplicates = group[1:]
        
        logger.info(f"Merging {len(duplicates)} into primary: '{primary.get('name') or primary.get('company')}' ({key})")
        
        try:
            # Merge fields into primary
            updates = {}
            p_ed = primary.get('enrichment_data')
            if isinstance(p_ed, str): p_ed = json.loads(p_ed)
            else: p_ed = p_ed or {}
            
            for dupe in duplicates:
                # Merge enrichment data
                d_ed = dupe.get('enrichment_data')
                if d_ed:
                    if isinstance(d_ed, str): d_ed = json.loads(d_ed)
                    for k, v in d_ed.items():
                        if v and not p_ed.get(k):
                            p_ed[k] = v
                
                # Merge basic fields
                for field in ['name', 'company', 'bio', 'instagram_handle', 'source_url']:
                    if dupe.get(field) and not primary.get(field):
                        updates[field] = dupe[field]
                        primary[field] = dupe[field]
                
                # Reassign email sequences
                supabase.table('email_sequences').update({'contact_id': primary['id']}).eq('contact_id', dupe['id']).execute()

            # Save primary updates
            updates['enrichment_data'] = json.dumps(p_ed)
            if updates:
                supabase.table('contacts').update(updates).eq('id', primary['id']).execute()
            
            # Delete duplicates
            dupe_ids = [d['id'] for d in duplicates]
            supabase.table('contacts').delete().in_('id', dupe_ids).execute()
            
            stats['merged'] += 1
            stats['deleted'] += len(duplicates)
            
        except Exception as e:
            logger.error(f"Error merging group {key}: {e}")
            stats['errors'] += 1

    logger.info(f"Cleanup complete: {stats}")
    return stats

    logger.info(f"Cleanup complete: {stats}")
    return stats

if __name__ == '__main__':
    cleanup_duplicates()
