#!/usr/bin/env python3
"""
Cleanup Sequences — Deduplicate sequence steps for a contact.
Ensures each contact has exactly one row per step_number (1, 2, 3, etc.).
Prioritizes keeping 'sent' or 'replied' steps over 'pending' ones.
"""

import os
import sys
import logging
from collections import defaultdict

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dotenv import load_dotenv
from pathlib import Path
from supabase import create_client

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

env_path = Path(__file__).resolve().parent.parent / '.env'
logger.info(f"Loading env from: {env_path}")
load_dotenv(env_path)

def cleanup_sequences():
    url = os.getenv('SUPABASE_URL')
    key = os.getenv('SUPABASE_SERVICE_ROLE_KEY') or os.getenv('SUPABASE_KEY')
    
    logger.info(f"SUPABASE_URL found: {bool(url)}")
    logger.info(f"SUPABASE_KEY found: {bool(key)}")

    if not url or not key:
        logger.error("Supabase credentials missing")
        return

    supabase = create_client(url, key)

    # 1. Fetch all sequence steps
    logger.info("Fetching all sequence steps...")
    res = supabase.table('email_sequences').select('id, contact_id, step_number, status, created_at, sent_at').execute()
    sequences = res.data or []
    logger.info(f"Found {len(sequences)} sequence steps.")

    # 2. Group by (contact_id, step_number)
    groups = defaultdict(list)
    for s in sequences:
        key = (s['contact_id'], s['step_number'])
        groups[key].append(s)

    to_delete = []
    
    for key, group in groups.items():
        if len(group) <= 1:
            continue
            
        # Prioritize: replied > sent > opened > pending > cancelled/failed
        status_priority = {
            'replied': 10,
            'sent': 8,
            'opened': 6,
            'pending': 4,
            'cancelled': 2,
            'failed': 0,
            'bounced': 0
        }
        
        # Sort by priority desc, then by date of activity
        group.sort(key=lambda x: (status_priority.get(x['status'], 0), x['sent_at'] or '2999', x['created_at']), reverse=True)
        
        # Keep the FIRST one, delete the rest
        keep_id = group[0]['id']
        others = group[1:]
        
        for other in others:
            to_delete.append(other['id'])

    if not to_delete:
        logger.info("No duplicate sequence steps found.")
        return

    logger.info(f"Deleting {len(to_delete)} duplicate sequence steps...")
    
    # Bulk delete (batching)
    batch_size = 100
    deleted_count = 0
    for i in range(0, len(to_delete), batch_size):
        batch = to_delete[i:i + batch_size]
        supabase.table('email_sequences').delete().in_('id', batch).execute()
        deleted_count += len(batch)
        logger.info(f"  Deleted batch {i//batch_size + 1} ({deleted_count}/{len(to_delete)})")

    logger.info("Deduplication complete.")

if __name__ == '__main__':
    cleanup_sequences()
