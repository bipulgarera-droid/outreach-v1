#!/usr/bin/env python3
"""
Business Search — Find businesses via Serper and extract name + website.

Unlike scrape_contacts.py (which targets LinkedIn people), this script
targets general web results to extract business names and their websites.
Results are stored as contacts with enrichment_data.website populated, ready
for Camoufox to scrape emails from.

Usage:
    python -m execution.business_search --queries "SEO agency Ohio" --num 50
"""

import os
import sys
import re
import json
import logging
import argparse
from datetime import datetime
from urllib.parse import urlparse

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dotenv import load_dotenv
from pathlib import Path

env_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(env_path)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ─── Domains to skip — aggregators, directories, social platforms ─────────────
SKIP_DOMAINS = {
    'linkedin.com', 'facebook.com', 'instagram.com', 'twitter.com', 'x.com',
    'youtube.com', 'tiktok.com', 'pinterest.com',
    'yelp.com', 'clutch.co', 'g2.com', 'trustpilot.com', 'bbb.org',
    'google.com', 'googleusercontent.com',
    'yellowpages.com', 'superpages.com', 'manta.com', 'dun.com', 'dnb.com',
    'crunchbase.com', 'zoominfo.com', 'apollo.io', 'rocketreach.co',
    'wikipedia.org', 'wikimedia.org',
    'reddit.com', 'quora.com', 'medium.com',
    'indeed.com', 'glassdoor.com', 'ziprecruiter.com',
    'bloomberg.com', 'forbes.com', 'inc.com', 'entrepreneur.com',
    'hubspot.com', 'salesforce.com', 'mailchimp.com',
    'ahrefs.com', 'moz.com',
    'sortlist.com', 'upcity.com', 'expertise.com', 'goodfirms.co',
    'bark.com', 'thumbtack.com', 'angi.com', 'homeadvisor.com',
    'tripadvisor.com', 'eventbrite.com',
    'justdial.com', 'indiamart.com', 'sulekha.com', 'magicbricks.com', '99acres.com',
    'zomato.com', 'swiggy.com', 'expedia.com', 'booking.com', 'naukri.com',
    'shiksha.com', 'collegeunion.in', 'collegedekho.com',
    'amazon.com', 'ebay.com', 'etsy.com',
    'shopify.com', 'wix.com', 'squarespace.com', 'wordpress.com',
}

def _extract_name_from_domain(url: str) -> str:
    """Extract a clean brand name from a URL with robust word splitting."""
    if not url: return ""
    try:
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
            'stories', 'maverick', 'jugaad', 'zoom', 'cine', 'that', 'matter', 'away', 'frames', 'action', 'rodey'
        ]
        prefixes = ['the', 'wild', 'magic', 'stories', 'red', 'zoom', 'cine', 'jugaad', 'maverick', 'goodfellas', 'star', 'grand', 'royal', 'nishant']
        
        for _ in range(3):
            old_brand = brand
            words = brand.split()
            cleaned_words = []
            for word in words:
                if len(word) >= 3:
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
                            elif idx == 0 and len(word) > len(k) + 2:
                                word = word[:len(k)] + ' ' + word[len(k):]
                                break
                cleaned_words.append(word)
            brand = ' '.join(cleaned_words)
            if brand == old_brand: break

        return brand.title().strip()
    except:
        return ""


def _get_root_domain(url: str) -> str:
    """Extract root domain from a URL (e.g. 'sub.example.com' → 'example.com')."""
    try:
        host = urlparse(url).netloc.lower().lstrip('www.')
        parts = host.split('.')
        if len(parts) >= 2:
            return '.'.join(parts[-2:])
        return host
    except Exception:
        return ''


def _is_valid_business_url(url: str) -> bool:
    """Return True if this URL is a real business site (not a directory/social)."""
    if not url:
        return False
    domain = _get_root_domain(url)
    if not domain:
        return False
    # Skip known aggregators / social platforms
    if domain in SKIP_DOMAINS:
        return False
        
    # Skip obvious non-business paths or deep links that look like listicles/blogs
    # Business homepages usually have very short paths
    parsed = urlparse(url)
    path = parsed.path.lower().rstrip('/')
    
    # If it's a deep link with more than 2 segments and mentions 'best' or 'top', it's a listicle
    if path.count('/') >= 2 and any(x in path for x in ('best', 'top', 'list')):
        return False
        
    reject_paths = ['/jobs/', '/careers/', '/blog/', '/news/', '/press/', '/category/', '/tag/', '/author/']
    if any(p in path for p in reject_paths):
        return False
        
    return True


def _clean_business_name(title: str, url: str = "") -> str:
    """
    Extract a short business name from a page title.
    e.g. 'Acme SEO | Digital Marketing Agency in Ohio' → 'Acme SEO'
    """
    if not title:
        return _extract_name_from_domain(url) if url else ""
        
    # 1. Skip if it's a generic page title
    generic_titles = {
        'home', 'homepage', 'about', 'about us', 'contact', 'contact us', 
        'services', 'pricing', 'login', 'register', 'sign up', 'careers',
        'jobs', 'portfolio', 'gallery', 'blog', 'news', 'privacy policy', 'terms',
        'mumbai', 'india', 'usa', 'uk', 'dubai', 'london', 'new york',
        'film production', 'video production', 'production house', 'film and video production'
    }
    title_lower = title.lower().strip()
    if title_lower in generic_titles:
        return _extract_name_from_domain(url)

    # 2. Split on common separators
    name = title
    for sep in (' : ', ': ', ' | ', ' - ', ' – ', ' — ', ' · ', ' • '):
        if sep in title:
            name = title.split(sep)[0].strip()
            if name.lower() in generic_titles and len(title.split(sep)) > 1:
                name = title.split(sep)[1].strip()
            break
            
    # ─── SMART SPACING ───
    # A. CamelCase split
    name = re.sub(r'([a-z])([A-Z])', r'\1 \2', name)
    
    # B. Multi-Pass Industry Split
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
        new_name = name
        words = new_name.split()
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
                        if idx > 0 and word[idx-1] != ' ':
                            word = word[:idx] + ' ' + word[idx:]
                            break
            cleaned_words.append(word)
        name = ' '.join(cleaned_words)
        if name == new_name: break

    # 3. Final cleaning
    name = name.strip(' -|–—.,;:"\' ')
    
    # 4. Reject if too short
    if len(name) < 3:
        return _extract_name_from_domain(url)
        
    # 5. Reject generic placeholders
    if name.lower() in generic_titles:
        return _extract_name_from_domain(url)

    # 6. Truncate if unreasonably long
    if len(name) > 60:
        return _extract_name_from_domain(url)

    # 7. Aggressive Stop Phrases / SEO Junk
    # If the name is ONLY a category name or contains "near me", use domain fallback
    category_junk = {
        'film production', 'film production house', 'film production company',
        'ad film agency', 'advertising agency', 'film making', 'video production',
        'production house', 'digital marketing agency', 'seo agency',
        'line production', 'corporate film production', 'commercial photography',
        'creative agency', 'motion graphics designer', 'industrial film production house',
        'film and video production company', 'advertising agency in mumbai',
        'best film production', 'top film production', 'film production services'
    }
    name_lower = name.lower()
    
    # Check if the name is just a generic category
    if name_lower in category_junk:
         return _extract_name_from_domain(url)
         
    # Check if the title is SEO-stuffed or a listicle
    seo_indicators = [
        'near me', 'contact list', 'jobs in', 'companies in', 'houses in',
        'services in', 'best film', 'top film', 'find ', 'get ', 'compare ',
        'list of', 'a guide', 'official website', 'list of productions',
        'production house in', 'agency in', 'company in', 'top 10', 'top 5'
    ]
    
    if any(x in name_lower for x in seo_indicators):
        domain_name = _extract_name_from_domain(url)
        # If domain name is better (more specific), use it
        if domain_name and len(domain_name) > 3:
            return domain_name
            
    return name


def parse_business_results(results: list[dict], source_query: str = '') -> list[dict]:
    """
    Parse raw Serper results into business contact records.
    Returns list of dicts with: name, website (in enrichment_data), source, status.
    """
    contacts = []
    seen_domains = set()

    for result in results:
        url = result.get('link', '')
        title = result.get('title', '')
        snippet = result.get('snippet', '')

        if not _is_valid_business_url(url):
            logger.info(f"  Skipping (directory/social): {url}")
            continue

        domain = _get_root_domain(url)
        if domain in seen_domains:
            continue
        seen_domains.add(domain)

        name = _clean_business_name(title, url)
        if not name:
            logger.info(f"  Skipping (no clean name): '{title}'")
            continue

        # Normalise the website to just the homepage
        parsed = urlparse(url)
        website = f"{parsed.scheme}://{parsed.netloc}/"

        enrichment_data = {
            'website': website,
            'source': 'business_search',
        }

        contact = {
            'name': name,
            'company': name, # Populate the new company column
            'bio': snippet[:300] if snippet else '',
            'source': source_query,
            'source_url': url,
            'enrichment_data': json.dumps(enrichment_data),
            'status': 'new',
        }
        contacts.append(contact)
        logger.info(f"  ✅ Business: '{name}' → {website}")

    logger.info(f"Parsed {len(contacts)} businesses from {len(results)} results")
    return contacts


def store_businesses(contacts: list[dict], project_id: str = None) -> dict:
    """
    Store businesses in the contacts table.
    Deduplicates by business name (case-insensitive) and website domain.
    """
    from supabase import create_client

    supabase_url = os.getenv('SUPABASE_URL')
    supabase_key = os.getenv('SUPABASE_SERVICE_ROLE_KEY') or os.getenv('SUPABASE_KEY')

    if not supabase_url or not supabase_key:
        logger.error("Supabase credentials not configured")
        return {'inserted': 0, 'skipped': 0, 'errors': 0}

    supabase = create_client(supabase_url, supabase_key)
    stats = {'inserted': 0, 'skipped': 0, 'errors': 0}

    if not contacts:
        return stats

    try:
        # Fetch existing names for dedup
        logger.info("Fetching existing contacts for deduplication...")
        existing_names: set[str] = set()
        existing_websites: set[str] = set()
        existing_emails: set[str] = set()
        offset, limit = 0, 1000
        q = supabase.table('contacts').select('name, email, enrichment_data')
        if project_id:
            q = q.eq('project_id', project_id)
        while True:
            res = q.range(offset, offset + limit - 1).execute()
            if not res.data:
                break
            for row in res.data:
                if row.get('name'):
                    existing_names.add(row['name'].lower())
                if row.get('email'):
                    existing_emails.add(row['email'].lower())
                
                # Check website in enrichment_data
                ed = row.get('enrichment_data')
                if ed:
                    if isinstance(ed, str):
                        try: ed = json.loads(ed)
                        except: ed = {}
                    w = ed.get('website')
                    if w:
                        existing_websites.add(w.lower().rstrip('/'))
            if len(res.data) < limit:
                break
            offset += limit

        new_contacts = []
        for c in contacts:
            name_lower = (c.get('name') or '').lower()
            email_lower = (c.get('email') or '').lower()
            
            # Extract website from enrichment_data for this candidate
            ed = c.get('enrichment_data')
            if isinstance(ed, str): ed = json.loads(ed)
            website_lower = (ed.get('website') or '').lower().rstrip('/')
            
            is_dupe = (name_lower and name_lower in existing_names) or \
                      (website_lower and website_lower in existing_websites) or \
                      (email_lower and email_lower in existing_emails)

            if is_dupe:
                stats['skipped'] += 1
                logger.info(f"  Skipping duplicate: '{c.get('name')}' ({website_lower or email_lower})")
                continue
            
            if project_id:
                c['project_id'] = project_id
            new_contacts.append(c)
            
            if name_lower: existing_names.add(name_lower)
            if website_lower: existing_websites.add(website_lower)
            if email_lower: existing_emails.add(email_lower)

        if new_contacts:
            logger.info(f"Bulk inserting {len(new_contacts)} businesses...")
            for i in range(0, len(new_contacts), 500):
                batch = new_contacts[i:i + 500]
                supabase.table('contacts').insert(batch).execute()
                stats['inserted'] += len(batch)

    except Exception as e:
        logger.error(f"Error storing businesses: {e}")
        stats['errors'] = len(contacts) - stats['inserted'] - stats['skipped']

    logger.info(f"Business storage results: {stats}")
    return stats


def extract_and_store_businesses(results: list[dict], source_query: str = '', project_id: str = None) -> dict:
    """Full pipeline: parse Serper results → deduplicate → store businesses."""
    contacts = parse_business_results(results, source_query)
    return store_businesses(contacts, project_id=project_id)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Find businesses via Serper and store as contacts')
    parser.add_argument('--queries', nargs='+', required=True, help='Search queries')
    parser.add_argument('--num', type=int, default=50, help='Results per query')
    parser.add_argument('--project_id', type=str, help='Project ID to store contacts under')
    args = parser.parse_args()

    from execution.serper_search import run_search_pipeline
    
    # We pass the full list of queries to serper
    results = run_search_pipeline(args.queries, args.num)
    
    # results is a list of dicts from serper, which we need to split by original query if possible
    # but run_search_pipeline usually returns a flattened list.
    # We'll just pass the whole results list to extract and store.
    stats = extract_and_store_businesses(results, source_query=", ".join(args.queries), project_id=args.project_id)
    print(json.dumps(stats, indent=2))
