#!/usr/bin/env python3
"""
Enrich Contacts — Find emails and Instagram handles via Serper Google Search.

Reads contacts from Supabase,
enriches with email and Instagram based on Name + Role.

Usage:
    python -m execution.enrich_contacts --limit 50
"""

import os
import sys
import json
import re
import argparse
import requests
import logging
import time
from datetime import datetime
from typing import Optional

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dotenv import load_dotenv
from pathlib import Path

from execution.verify_email import check_email

env_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(env_path)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SERPER_API_KEY = os.getenv('SERPER_API_KEY')
SERPER_URL = 'https://google.serper.dev/search'

# Common role keywords to help Google narrow results
ROLE_KEYWORDS_MAP = {
    'programmer': 'festival programmer',
    'curator': 'festival curator',
    'director': 'festival director',
    'producer': 'producer',
    'critic': 'film critic',
    'writer': 'writer',
    'journalist': 'journalist',
    'editor': 'editor',
    'ceo': 'ceo',
    'founder': 'founder',
    'marketing': 'marketing',
    'manager': 'manager',
}

# No Jina/Gemini needed for branding as per user request

def _sanitize_company_name(company: str) -> str:
    """Clean company name by removing common SEO junk and domain extensions."""
    if not company: return ""
    clean = company.strip()
    
    # ─── SMART SPACING ───
    # 1. CamelCase split
    clean = re.sub(r'([a-z])([A-Z])', r'\1 \2', clean)
    
    # 2. Multi-Pass Industry Split
    keywords = [
        'entertainment', 'motionpictures', 'productions', 'production', 
        'studios', 'studio', 'films', 'film', 'media', 'works', 'creative', 
        'solutions', 'digital', 'global', 'agency', 'group', 'services', 
        'official', 'vfx', 'corp', 'company', 'pictures', 'house', 'collective',
        'mantra', 'wadi', 'power', 'hour', 'baba', 'chillies',
        'stories', 'maverick', 'jugaad', 'zoom', 'cine', 'that', 'matter'
    ]
    prefixes = ['the', 'wild', 'magic', 'stories', 'red', 'zoom', 'cine', 'jugaad', 'maverick', 'goodfellas', 'star', 'grand', 'royal']
    
    for _ in range(3):
        n_clean = clean
        parts = n_clean.split()
        cleaned_parts = []
        for word in parts:
            if len(word) > 3:
                # Prefix split
                for p in prefixes:
                    if word.lower().startswith(p) and len(word) > len(p) + 2:
                        word = word[:len(p)] + ' ' + word[len(p):]
                        break
                # Keyword split
                for k in keywords:
                    low = word.lower()
                    if k in low:
                        idx = low.find(k)
                        if idx > 0 and word[idx-1] != ' ':
                            word = word[:idx] + ' ' + word[idx:]
                            break
            cleaned_parts.append(word)
        clean = ' '.join(cleaned_parts)
        if clean == n_clean: break

    # 3. Remove domain extensions
    clean = re.sub(r'\.(com|net|org|in|biz|ai|co\.in|io)$', '', clean, flags=re.IGNORECASE)
    
    # 4. Remove common junk words
    junk = ['ltd', 'pvt', 'limited', 'private', 'inc', 'corp', 'corporation', 'llp', 'llc']
    for j in junk:
        clean = re.sub(rf'\b{j}\b\.?', '', clean, flags=re.IGNORECASE)
        
    return clean.strip(' -|–—.,;:"\' ').title()

def _extract_brand_from_url(url: str) -> str:
    """Extract a clean brand name from a URL with robust word splitting."""
    if not url: return ""
    try:
        from urllib.parse import urlparse
        domain = urlparse(url).netloc.lower()
        if domain.startswith('www.'):
            domain = domain[4:]
        
        # 1. Remove common extensions
        brand = re.sub(r'\.(com|in|org|net|biz|co\.in|me|tv|us|ae|io|ai|live)$', '', domain, flags=re.IGNORECASE)
        
        # 2. Handle hyphens and underscores (direct separators)
        brand = brand.replace('-', ' ').replace('_', ' ')
        
        # 3. ─── SMART SPACING ───
        # A. CamelCase split (if any casing survived)
        brand = re.sub(r'([a-z])([A-Z])', r'\1 \2', brand)
        
        # B. Multi-Pass Industry Split (Split mashed words like 'laseraway' or 'framesinaction')
        keywords = [
            'entertainment', 'motionpictures', 'productions', 'production', 
            'studios', 'studio', 'films', 'film', 'media', 'works', 'creative', 
            'solutions', 'digital', 'global', 'agency', 'group', 'services', 
            'official', 'vfx', 'corp', 'company', 'pictures', 'house', 'collective',
            'mantra', 'wadi', 'power', 'hour', 'baba', 'chillies', 'view', 'point',
            'stories', 'maverick', 'jugaad', 'zoom', 'cine', 'that', 'matter', 'away', 'frames', 'action'
        ]
        prefixes = ['the', 'wild', 'magic', 'stories', 'red', 'zoom', 'cine', 'jugaad', 'maverick', 'goodfellas', 'star', 'grand', 'royal']
        
        for _ in range(3):
            old_brand = brand
            words = brand.split()
            cleaned_words = []
            for word in words:
                if len(word) > 3:
                    # Prefix split
                    for p in prefixes:
                        if word.lower().startswith(p) and len(word) > len(p) + 2:
                            word = word[:len(p)] + ' ' + word[len(p):]
                            break
                    # Keyword split
                    for k in keywords:
                        low = word.lower()
                        if k in low:
                            idx = low.find(k)
                            # Split if keyword is in the middle of a word AND not already separated
                            if idx > 0 and word[idx-1] != ' ':
                                word = word[:idx] + ' ' + word[idx:]
                                break
                            # Also split if keyword is at the start but followed by more characters
                            elif idx == 0 and len(word) > len(k) + 2:
                                word = word[:len(k)] + ' ' + word[len(k):]
                                break
                cleaned_words.append(word)
            brand = ' '.join(cleaned_words)
            if brand == old_brand: break

        return brand.title().strip()
    except:
        return ""

def _extract_human_name_from_email(email: str) -> Optional[str]:
    """
    Attempt to extract a human name from a personal-looking email address.
    e.g. 'tarun.gangwani@...' -> 'Tarun Gangwani'
    """
    if not email: return None
    local_part = email.split('@')[0].lower()
    
    # Skip generic prefixes
    generic = ['info', 'contact', 'hello', 'admin', 'support', 'office', 'team', 'press', 'mail', 'projects', 'careers', 'vfx', 'hr']
    if any(local_part == g or local_part.startswith(g + '.') or local_part.startswith(g + '_') for g in generic):
        return None
        
    # Handle patterns like first.last, first_last, or just first
    name_parts = re.split(r'[\._-]', local_part)
    if len(name_parts) >= 1:
        # We'll be strictly about real names. Skip if a part is only 1-2 chars long.
        # e.g. 'vs@...' -> None. 'tarun@...' -> 'Tarun'.
        clean_parts = [re.sub(r'\d+', '', p).title() for p in name_parts if len(re.sub(r'\d+', '', p)) > 2]
        
        if clean_parts:
            return ' '.join(clean_parts[:2])
            
    return None

def guess_role_keyword(contact: dict, project_info: dict = None) -> str:
    """
    Use the original source query or project niche to determine the best role keyword.
    """
    # 1. Try original source query (most specific)
    source = (contact.get('source') or '').lower()
    if source:
        cleaned = re.sub(r'site:\S+', '', source)
        parts = ' '.join(cleaned.split())
        if parts: return parts
    
    # 2. Try Bio match against map
    bio = (contact.get('bio') or '').lower()
    for keyword, role_phrase in ROLE_KEYWORDS_MAP.items():
        if keyword in bio:
            return role_phrase
            
    # 3. Dynamic Fallback based on Project Context
    if project_info:
        p_name = project_info.get('name', '').lower()
        p_desc = project_info.get('description', '').lower()
        
        if 'film' in p_name or 'movie' in p_name or 'festival' in p_name:
            return 'producer'
        if 'real estate' in p_name or 'realtor' in p_name:
            return 'realtor'
        if 'law' in p_name or 'attorney' in p_name:
            return 'attorney'
        if 'agency' in p_name or 'marketing' in p_name:
            return 'founder'
            
    # 4. Global fallback
    return 'owner'

def find_emails_serper(name: str, role_keyword: str = '') -> list[str]:
    """
    Find email addresses by searching Google via Serper.
    Query used: [Name] [Role] email "@"
    """
    if not SERPER_API_KEY:
        return []
        
    found_emails = []
    seen_emails = set()
    
    # 1. Broad search: name + role + email + "@" (Unquoted)
    query_parts = [name]
    if role_keyword:
        query_parts.append(role_keyword)
    # Adding "@" ensures Google prioritizes results with email addresses
    query_parts.extend(['email', '"@"'])
    
    query = ' '.join(query_parts)
    
    headers = {
        'X-API-KEY': SERPER_API_KEY,
        'Content-Type': 'application/json'
    }
    
    try:
        payload = {'q': query, 'num': 10}
        logger.info(f"  Email search query: {query}")
        
        response = requests.post(SERPER_URL, headers=headers, json=payload, timeout=15)
        data = response.json()
        
        # Check AI snippet
        ai_snippet = data.get('answerBox', {}).get('snippet', '') or ''
        ai_answer = data.get('answerBox', {}).get('answer', '') or ''
        knowledge_desc = data.get('knowledgeGraph', {}).get('description', '') or ''
        
        for text_block in [ai_snippet, ai_answer, knowledge_desc]:
            emails = re.findall(r'[\w.+-]+@[\w-]+\.[\w.-]+', text_block)
            for email in emails:
                email = email.strip().rstrip('.,;:)!% ]').strip()
                email_lower = email.lower()
                if email_lower not in seen_emails and _is_valid_email(email_lower):
                    found_emails.append(email)
                    seen_emails.add(email_lower)
        
        # Scan organic results
        for result in data.get('organic', []):
            text = f"{result.get('title', '')} {result.get('snippet', '')}"
            emails = re.findall(r'[\w.+-]+@[\w-]+\.[\w.-]+', text)
            for email in emails:
                email_lower = email.lower()
                if email_lower not in seen_emails and _is_valid_email(email_lower):
                    found_emails.append(email)
                    seen_emails.add(email_lower)

        if len(found_emails) > 0:
            logger.info(f"    -> Found {len(found_emails)} emails.")
            
    except Exception as e:
        logger.warning(f"Serper email search error for query '{query}': {e}")
        
    return found_emails


def _is_valid_email(email: str) -> bool:
    """Filter out junk/generic emails."""
    skip_patterns = [
        'example.com', 'email.com', 'mail.com', 'noreply', 'support@', 'info@',
        'admin@', 'webmaster@', 'no-reply', 'donotreply', 'test@',
        'sentry.io', 'github.com', 'placeholder', 'domain.com',
        'yourname@', 'name@', 'user@', 'sample'
    ]
    return not any(skip in email for skip in skip_patterns)


def find_instagram_serper(name: str, role_keyword: str = '') -> Optional[str]:
    """
    Find Instagram handle via Serper search.
    Query: [Name] [Role] instagram (unquoted)
    """
    if not SERPER_API_KEY:
        return None
        
    try:
        headers = {
            'X-API-KEY': SERPER_API_KEY,
            'Content-Type': 'application/json'
        }
        
        # Build query: name (unquoted) + role keyword + instagram
        query_parts = [name]
        if role_keyword:
            query_parts.append(role_keyword)
        query_parts.append('instagram')
        query = ' '.join(query_parts)
        
        payload = {
            'q': query,
            'num': 5
        }
        
        logger.info(f"  Instagram search query: {query}")
        response = requests.post(SERPER_URL, headers=headers, json=payload, timeout=15)
        data = response.json()
        
        skip_handles = {'explore', 'accounts', 'about', 'tags', 'locations', 'stories', 'directory'}
        
        for result in data.get('organic', []):
            url = result.get('link', '')
            
            # Post/Reel logic - retain URL
            if '/p/' in url or '/reel/' in url or '/tv/' in url:
                return url
                
            # Profile logic - Extract handle
            match = re.search(r'instagram\.com/([a-zA-Z0-9_.]+)', url)
            if match:
                handle = match.group(1)
                # Filter out generic Instagram pages
                if handle.lower() not in skip_handles and handle.lower() not in ['p', 'reel', 'tv']:
                    return f"@{handle}"
                    
    except Exception as e:
        logger.warning(f"Instagram search error for {name}: {e}")
        
    return None


def _score_email(email: str, name: str) -> int:
    """
    Score an email candidate on a 0-100 confidence scale.
    Since we only use Serper now, we just check name match and penalize generics.
    """
    score = 15 # Base score for Serper sourced emails
    email_lower = email.lower()
    local_part = email_lower.split('@')[0]
    
    # Name match bonus
    name_parts = name.lower().split()
    first_name = name_parts[0] if name_parts else ''
    last_name = name_parts[-1] if len(name_parts) > 1 else ''
    
    if first_name and last_name:
        if first_name in local_part and last_name in local_part:
            score += 35
        elif first_name in local_part or last_name in local_part:
            score += 20
        elif first_name[0] in local_part and last_name in local_part:
            score += 15
            
    # Generic email penalty
    generic_prefixes = ['info', 'contact', 'hello', 'admin', 'support', 
                        'submissions', 'general', 'office', 'team', 'press',
                        'media', 'marketing', 'sales', 'jobs', 'careers', 'hr']
    if any(local_part.startswith(g) for g in generic_prefixes):
        score -= 20
        
    return max(0, min(100, score))


def enrich_single_contact(contact: dict, project_info: dict = None) -> Optional[dict]:
    """
    Enrich contact via simple Serper queries using project context.
    """
    name = contact.get('name', '')
    role_keyword = guess_role_keyword(contact, project_info)
    
    existing_email = (contact.get('email') or '').strip()
    existing_instagram = (contact.get('instagram') or '').strip()
    
    existing_enrichment = contact.get('enrichment_data')
    if isinstance(existing_enrichment, str):
        try:
            existing_enrichment = json.loads(existing_enrichment)
        except Exception:
            existing_enrichment = {}
    elif not isinstance(existing_enrichment, dict):
        existing_enrichment = {}
        
    # ── SMART SKIP ──
    # If both are present AND we have verification data, skip.
    has_verif = existing_enrichment.get('verification_status') is not None
    if existing_email and existing_instagram and has_verif:
        logger.info(f"  ⏭️  Skipping entire enrichment — Email, Instagram, and Verification present.")
        return None

    updates = {
        'enrichment_data': existing_enrichment.copy(),
        'status': 'enriched',
        'updated_at': datetime.utcnow().isoformat()
    }

    # Determine if existing email has verification status
    has_verif = existing_enrichment.get('verification_status') is not None
    
    # ── SMART SKIP ──
    # If both are present AND we have verification data, skip.
    if existing_email and existing_instagram and has_verif:
        logger.info(f"  ⏭️  Skipping entire enrichment — Email, Instagram, and Verification present.")
        return None
        
    # ── EMAIL ENRICHMENT AND VERIFICATION ──
    if not existing_email:
        serper_emails = find_emails_serper(name, role_keyword)
        
        if serper_emails:
            scored = []
            for em in serper_emails:
                score = _score_email(em, name)
                scored.append((score, em))
                logger.info(f"    Email candidate: {em} (score={score})")
                
            scored.sort(key=lambda x: x[0], reverse=True)
            
            valid_found = False
            for best_score, best_email in scored:
                if best_score < 10:
                    logger.info("  Skipping remaining candidates (score < 10)")
                    break
                    
                logger.info(f"  Verifying candidate: {best_email} (score={best_score})...")
                v_status, v_reason = check_email(best_email)
                logger.info(f"  Verification result: {v_status} ({v_reason})")
                
                if v_status == 'invalid':
                    logger.warning(f"  ❌ Discarding invalid email: {best_email}")
                    continue  # Try next candidate
                    
                # Valid or risky email found -> save it
                updates['email'] = best_email
                logger.info(f"  ✅ Kept email: {best_email} (confidence={best_score}, status={v_status})")
                
                # ── HUMAN NAME EXTRACTION ──
                # If current name is likely a company placeholder, try to extract a real human name
                current_name_clean = name.lower().strip()
                current_company_clean = (contact.get('company') or '').lower().strip()
                if current_name_clean == current_company_clean or not current_name_clean:
                    human_name = _extract_human_name_from_email(best_email)
                    if human_name:
                        logger.info(f"  👤 Extracted human name from email: {human_name}")
                        updates['name'] = human_name
                    else:
                        # Clear robotic name/placeholder so template falls back to "there"
                        logger.info(f"  🤖 No human name found; clearing placeholder '{name}' for 'there' fallback.")
                        updates['name'] = ""

                updates['enrichment_data']['email_source'] = 'serper'
                updates['enrichment_data']['email_confidence'] = best_score
                updates['enrichment_data']['verification_status'] = v_status
                updates['enrichment_data']['verification_reason'] = v_reason
                valid_found = True
                break
                
            if not valid_found:
                logger.warning(f"  ❌ All email candidates were invalid or below score threshold.")
                updates['email'] = None
                
            updates['enrichment_data']['email_candidates'] = [
                {'email': em, 'source': 'serper', 'confidence': sc}
                for sc, em in scored
            ]
    else:
        # Check if we need to verify the existing email
        if not has_verif:
            logger.info(f"  Verifying EXISTING email: {existing_email}...")
            v_status, v_reason = check_email(existing_email)
            logger.info(f"  Verification result: {v_status} ({v_reason})")
            updates['enrichment_data']['verification_status'] = v_status
            updates['enrichment_data']['verification_reason'] = v_reason
        else:
            logger.info(f"  ⏭️  Skipping email search — contact already has verified email: {existing_email}")
        
    # ── INSTAGRAM ENRICHMENT ──
    if not existing_instagram:
        instagram = find_instagram_serper(name, role_keyword)
        if instagram:
            updates['instagram'] = instagram
            updates['enrichment_data']['instagram_source'] = 'serper'
            logger.info(f"  Found Instagram: {instagram}")
    else:
        logger.info(f"  ⏭️  Skipping Instagram search — contact already has: {existing_instagram}")
    # ── BRAND EXTRACTION FROM URL ──
    # User requested to use URL extraction to fix name and company for non-nurtured leads.
    if contact.get('status') in ['new', 'enriched']:
        website_url = contact.get('website') or existing_enrichment.get('website') or contact.get('source_url')
        if website_url:
            clean_brand = _extract_brand_from_url(website_url)
            if clean_brand and len(clean_brand) > 2:
                updates['name'] = updates.get('name') or clean_brand # Keep human name if found, else brand
                updates['company'] = clean_brand
                logger.info(f"  🏢 URL Brand Extraction: {clean_brand}")
            
    # Remove redundant json.dumps() - Supabase SDK handles dict -> jsonb automatically.
    # updates['enrichment_data'] = json.dumps(updates['enrichment_data'])
    
    return updates


def enrich_contacts(limit: int = 50, project_id: str = None, contact_ids: list = None, dry_run: bool = False) -> dict:
    from supabase import create_client
    
    supabase_url = os.getenv('SUPABASE_URL')
    supabase_key = os.getenv('SUPABASE_SERVICE_ROLE_KEY') or os.getenv('SUPABASE_KEY')
    
    if not supabase_url or not supabase_key:
        logger.error("Supabase credentials not configured")
        return {'error': 'No Supabase credentials'}
        
    supabase = create_client(supabase_url, supabase_key)
    
    # Fetch Project Info once for context
    project_info = {}
    if project_id:
        p_res = supabase.table('projects').select('name, description').eq('id', project_id).limit(1).execute()
        if p_res.data: project_info = p_res.data[0]
        
    query = supabase.table('contacts').select('*')
    if contact_ids and len(contact_ids) > 0:
        logger.info(f"  Fetching specific contact IDs: {len(contact_ids)}")
        # Use .range(0, 1000) to ensure we get everything even if PostgREST defaults to 100
        query = query.in_('id', contact_ids).range(0, 1000)
    elif project_id:
        logger.info(f"  Fetching pending contacts for project {project_id} (limit {limit})")
        query = query.eq('project_id', project_id).eq('status', 'new').limit(limit)
    else:
        logger.info(f"  Fetching pending contacts with limit: {limit}")
        query = query.eq('status', 'new').limit(limit)
        
    result = query.execute()
    contacts = result.data or []
    
    logger.info(f"Found {len(contacts)} contacts to enrich")
    
    stats = {'processed': 0, 'emails_found': 0, 'ig_found': 0, 'errors': 0}
    
    for i, contact in enumerate(contacts):
        try:
            # Determine project_id if not passed
            pid = project_id or contact.get('project_id')
            if pid and not project_info:
                p_res = supabase.table('projects').select('name, description').eq('id', pid).limit(1).execute()
                if p_res.data: project_info = p_res.data[0]

            logger.info(f"[{i+1}/{len(contacts)}] Enriching: {contact['name']}")
            updates = enrich_single_contact(contact, project_info)
            
            if updates and not dry_run:
                supabase.table('contacts').update(updates).eq('id', contact['id']).execute()
            
            if updates:
                if updates.get('email'):
                    stats['emails_found'] += 1
                if updates.get('instagram'):
                    stats['ig_found'] += 1
                stats['processed'] += 1
            
            time.sleep(1)
            
        except Exception as e:
            logger.error(f"Error enriching {contact.get('name', '?')}: {e}")
            stats['errors'] += 1
            
    logger.info(f"Enrichment complete: {stats}")
    return stats


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Enrich contacts logic")
    parser.add_argument('--limit', type=int, default=50)
    parser.add_argument('--id', type=str, help="Specific contact ID to enrich")
    args = parser.parse_args()
    
    ids = [args.id] if args.id else None
    res = enrich_contacts(limit=args.limit, contact_ids=ids)
    print(json.dumps(res, indent=2))
