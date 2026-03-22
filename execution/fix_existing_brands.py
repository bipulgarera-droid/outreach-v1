import os
import sys
import json
import re
import logging
from typing import Optional
from dotenv import load_dotenv
from pathlib import Path
from supabase import create_client

# Add parent dir to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from execution.enrich_contacts import _sanitize_company_name, _extract_brand_from_url, _extract_human_name_from_email

logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    load_dotenv(Path(__file__).resolve().parent.parent / '.env')
    
    supabase_url = os.getenv('SUPABASE_URL')
    supabase_key = os.getenv('SUPABASE_SERVICE_ROLE_KEY') or os.getenv('SUPABASE_KEY')
    
    if not supabase_url or not supabase_key:
        logger.error("Missing credentials")
        return
        
    supabase = create_client(supabase_url, supabase_key)
    
    # Target "Film work" project (the user mentions this project specifically)
    project_id = 'rbkrtmzqubwrvkrvcebr-film-work' # I'll need to double check the ID if possible
    
    # Better to find project by name if possible
    projects = supabase.table('projects').select('id').eq('name', 'Film work').execute()
    if not projects.data:
        logger.error("Project 'Film work' not found")
        return
    
    project_id = projects.data[0]['id']
    logger.info(f"Fixing contacts for project: {project_id}")
    
    # Fetch all contacts for this project
    contacts = supabase.table('contacts').select('*').eq('project_id', project_id).execute()
    
    if not contacts.data:
        logger.info("No contacts found.")
        return
        
    updated_count = 0
    for contact in contacts.data:
        updates = {}
        
        name = contact.get('name', '')
        company = contact.get('company', '')
        email = contact.get('email', '')
        website = contact.get('website', '')
        
        # 1. Brand Sync from Website
        if website:
            brand_name = _extract_brand_from_url(website)
            if brand_name and brand_name.lower() != (company or "").lower():
                logger.info(f"🔄 Syncing brand: '{company}' -> '{brand_name}'")
                updates['company'] = brand_name
                
                # If name was just the old company, update it too
                if (name or "").lower() == (company or "").lower():
                    updates['name'] = brand_name
        
        # 2. Human Name Extraction from Email
        if email:
            human_name = _extract_human_name_from_email(email)
            if human_name:
                # Only update if current name is just a company placeholder
                current_name = updates.get('name', name) or ""
                current_company = updates.get('company', company) or ""
                
                if current_name.lower() == current_company.lower() or not current_name:
                    logger.info(f"👤 Found human: '{current_name}' -> '{human_name}'")
                    updates['name'] = human_name
        
        # 3. Final Sanitization
        final_company = updates.get('company', company)
        if final_company:
            sanitized = _sanitize_company_name(final_company)
            if sanitized != final_company:
                updates['company'] = sanitized
                
        if updates:
            supabase.table('contacts').update(updates).eq('id', contact['id']).execute()
            updated_count += 1
            
    logger.info(f"Done! Fixed {updated_count} contacts.")

if __name__ == "__main__":
    main()
