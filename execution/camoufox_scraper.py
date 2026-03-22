#!/usr/bin/env python3
"""
Camoufox Scraper — Stealth browser-based email and Instagram extractor.

Uses Camoufox (modified Firefox with anti-bot fingerprint spoofing) to scrape
contact pages for email addresses and Instagram handles. Bypasses Cloudflare
and other protection layers that block simple HTTP requests.

Falls back to a Serper search if no website URL is provided for the contact.

Usage (standalone):
    python -m execution.camoufox_scraper --contact-id <uuid>

Requires:
    pip install camoufox[geoip] playwright
    playwright install
"""

import os
import re
import sys
import json
import asyncio
import logging
import requests
from typing import Optional
from pathlib import Path

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dotenv import load_dotenv
env_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(env_path)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SERPER_API_KEY = os.getenv('SERPER_API_KEY')
SERPER_URL = 'https://google.serper.dev/search'

# Sub-pages to check for contact info beyond the homepage
CONTACT_SUBPAGES = ['/contact', '/contact-us', '/about', '/team', '/about-us', '/reach-us']

# Max pages to scrape per domain (homepage + these many subpages)
MAX_SUBPAGES = 3


# ── Helpers ──────────────────────────────────────────────────────────────────

def _extract_emails_from_text(text: str) -> list[str]:
    """Pull all plausible email addresses from plain text."""
    raw = re.findall(r'[\w.+\-]+@[\w\-]+\.[\w.\-]+', text)
    seen = set()
    clean = []

    skip_patterns = [
        'example.com', 'mail.com', 'noreply', 'support@', 'sentry.io',
        'admin@', 'webmaster@', 'no-reply', 'donotreply', 'test@',
        'placeholder', 'domain.com', 'yourname@', 'sample', 'github.com',
    ]

    for email in raw:
        email = email.strip().rstrip('.,;:)!% ]').strip()
        el = email.lower()
        if el in seen:
            continue
        if any(sk in el for sk in skip_patterns):
            continue
        if re.match(r'[\w.+\-]+@[\w\-]+\.[\w.\-]{2,}', el):
            clean.append(email)
            seen.add(el)

    return clean


def _extract_instagram_from_text(text: str) -> Optional[str]:
    """Extract first Instagram handle or profile URL from page text."""
    # Direct profile links like instagram.com/handle
    match = re.search(r'instagram\.com/([a-zA-Z0-9_.]{3,})', text)
    if match:
        handle = match.group(1)
        skip = {'explore', 'accounts', 'about', 'tags', 'locations', 'stories', 'directory', 'p', 'reel', 'tv'}
        if handle.lower() not in skip:
            return f"@{handle}"

    # @handle pattern in text
    match = re.search(r'@([a-zA-Z0-9_.]{3,30})', text)
    if match:
        return f"@{match.group(1)}"

    return None


def _find_website_serper(name: str, location: str = '') -> Optional[str]:
    """
    Use Serper to find the website for a business when no URL is stored.
    Query: "Business Name" + location
    """
    if not SERPER_API_KEY:
        logger.warning("No SERPER_API_KEY set — cannot search for website.")
        return None

    query_parts = [name]
    if location:
        query_parts.append(location)
    query = ' '.join(query_parts)

    try:
        headers = {'X-API-KEY': SERPER_API_KEY, 'Content-Type': 'application/json'}
        response = requests.post(SERPER_URL, headers=headers, json={'q': query, 'num': 5}, timeout=15)
        data = response.json()

        # Knowledge graph website is most reliable
        kg_website = data.get('knowledgeGraph', {}).get('website')
        if kg_website:
            logger.info(f"  Found website via knowledge graph: {kg_website}")
            return kg_website

        # First organic result whose URL looks like a homepage
        for result in data.get('organic', []):
            url = result.get('link', '')
            # If URL has no deep path (just a domain or /), it's a homepage
            path = url.split('/', 3)[-1] if url.count('/') >= 3 else ''
            if not path or path in ['', '#']:
                logger.info(f"  Found website via organic: {url}")
                return url

        # If nothing better, just take the first result
        if data.get('organic'):
            url = data['organic'][0]['link']
            logger.info(f"  Using first organic result as fallback: {url}")
            return url

    except Exception as e:
        logger.warning(f"Serper website search error: {e}")

    return None


# ── Async Scraper ─────────────────────────────────────────────────────────────

async def _scrape_with_camoufox(url: str) -> dict:
    """
    Use Camoufox stealth browser to scrape email and Instagram from a website.
    Checks the homepage plus CONTACT_SUBPAGES (up to MAX_SUBPAGES).

    Returns:
        { 'emails': [...], 'instagram': str or None }
    """
    try:
        from camoufox.async_api import AsyncCamoufox
    except ImportError:
        logger.error("camoufox not installed. Run: pip install 'camoufox[geoip]' && playwright install")
        return {'emails': [], 'instagram': None}

    all_emails = []
    instagram = None

    # Normalise base URL
    base_url = url.rstrip('/')
    if not base_url.startswith('http'):
        base_url = f"https://{base_url}"

    pages_to_check = [base_url]
    for sub in CONTACT_SUBPAGES[:MAX_SUBPAGES]:
        pages_to_check.append(f"{base_url}{sub}")

    async with AsyncCamoufox(headless=True, geoip=False) as browser:
        page = await browser.new_page()
        page.set_default_timeout(20_000)  # 20s per page

        for page_url in pages_to_check:
            try:
                logger.info(f"  Camoufox: scraping {page_url}")
                resp = await page.goto(page_url, wait_until='domcontentloaded')

                if resp and resp.status >= 400:
                    logger.debug(f"  -> HTTP {resp.status}, skipping.")
                    continue

                text = await page.inner_text('body')

                found_emails = _extract_emails_from_text(text)
                if found_emails:
                    all_emails.extend(found_emails)
                    logger.info(f"  -> Found {len(found_emails)} email(s) on {page_url}")

                if not instagram:
                    # Also check hrefs for social links
                    links = await page.eval_on_selector_all(
                        'a[href]',
                        'els => els.map(el => el.href)'
                    )
                    combined = text + ' ' + ' '.join(links)
                    instagram = _extract_instagram_from_text(combined)
                    if instagram:
                        logger.info(f"  -> Found Instagram: {instagram} on {page_url}")

                # Stop early if we found what we need
                if all_emails and instagram:
                    break

            except Exception as page_err:
                logger.warning(f"  Error on {page_url}: {page_err}")
                continue

    # Deduplicate emails preserving order
    seen = set()
    unique_emails = []
    for e in all_emails:
        if e.lower() not in seen:
            unique_emails.append(e)
            seen.add(e.lower())

    return {'emails': unique_emails, 'instagram': instagram}


# ── Public Sync API ───────────────────────────────────────────────────────────

def scrape_contact_info(contact: dict) -> dict:
    """
    Main entry point for camoufox enrichment of a single contact.

    Steps:
      1. Check enrichment_data.website or contact.website for a URL
      2. If none, use Serper to discover the website (name + location)
      3. Scrape the site with Camoufox for emails + Instagram
      4. Return structured result

    Args:
        contact: dict with keys: name, enrichment_data (dict or JSON str), location, niche

    Returns:
        {
            'website': str or None,
            'emails': list[str],
            'instagram': str or None,
            'source': 'direct' | 'serper_fallback' | 'no_website'
        }
    """
    name = contact.get('name', '')

    # Parse enrichment_data
    enrichment = contact.get('enrichment_data') or {}
    if isinstance(enrichment, str):
        try:
            enrichment = json.loads(enrichment)
        except Exception:
            enrichment = {}

    # Determine website
    website = (
        enrichment.get('website') or
        contact.get('website') or
        ''
    ).strip()

    source = 'direct'

    if not website:
        logger.info(f"No website for '{name}' — searching Serper...")
        location = enrichment.get('location') or contact.get('location') or ''
        website = _find_website_serper(name, location)
        source = 'serper_fallback' if website else 'no_website'

    if not website:
        logger.warning(f"Could not find website for '{name}'. Skipping.")
        return {'website': None, 'emails': [], 'instagram': None, 'source': 'no_website'}

    logger.info(f"Scraping '{name}' → {website}")

    result = asyncio.run(_scrape_with_camoufox(website))

    return {
        'website': website,
        'emails': result['emails'],
        'instagram': result['instagram'],
        'source': source,
    }


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    import argparse
    from supabase import create_client

    parser = argparse.ArgumentParser(description='Camoufox stealth enrichment for a single contact')
    parser.add_argument('--contact-id', type=str, help='Supabase contact UUID to enrich')
    parser.add_argument('--website', type=str, help='Website URL to scrape directly (for quick tests)')
    args = parser.parse_args()

    if args.website:
        result = asyncio.run(_scrape_with_camoufox(args.website))
        print(json.dumps(result, indent=2))
    elif args.contact_id:
        sb = create_client(os.getenv('SUPABASE_URL'), os.getenv('SUPABASE_SERVICE_ROLE_KEY') or os.getenv('SUPABASE_KEY'))
        row = sb.table('contacts').select('*').eq('id', args.contact_id).single().execute()
        if not row.data:
            print('Contact not found')
            sys.exit(1)
        result = scrape_contact_info(row.data)
        print(json.dumps(result, indent=2))
    else:
        parser.print_help()
