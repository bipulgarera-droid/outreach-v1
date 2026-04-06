import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from supabase import create_client

env_path = Path(__file__).resolve().parent.parent / '.env'
try:
    load_dotenv(env_path)
except:
    pass

supabase_url = os.getenv('SUPABASE_URL')
supabase_key = os.getenv('SUPABASE_SERVICE_ROLE_KEY') or os.getenv('SUPABASE_KEY')

if not supabase_url or not supabase_key:
    print("No supabase credentials!")
    sys.exit(1)
    
sb = create_client(supabase_url, supabase_key)

print("Starting regeneration...")

# 1. First get the project 'seo agencies' or similar
projects_res = sb.table('projects').select('id, name').ilike('name', '%seo agencies%').execute()
for p in projects_res.data:
    print(f"Found Project: {p['name']} ({p['id']})")
    
if not projects_res.data:
    print("No project found!")
    sys.exit(1)
    
proj_id = projects_res.data[0]['id']

# 2. Get contacts in this project with company = 'Unknown'
contacts_res = sb.table('contacts').select('*').eq('project_id', proj_id).in_('company', ['Unknown', 'Unknown Company']).execute()
contacts = contacts_res.data

if not contacts:
    print("No unknown company contacts found.")
    sys.exit(0)

print(f"Found {len(contacts)} contacts with 'Unknown' company.")

updated_count = 0

for c in contacts:
    email = (c.get('email') or '').strip().lower()
    if not email or '@' not in email:
        continue
        
    domain = email.split('@')[-1]
    name_part = domain.split('.')[0]
    
    if name_part.lower() in ['gmail', 'yahoo', 'outlook', 'hotmail', 'icloud', 'aol']:
        new_company = 'your company'
    else:
        new_company = name_part.title()
        
    # Get pending sequences for this contact
    seq_res = sb.table('email_sequences').select('id, subject, body').eq('contact_id', c['id']).eq('status', 'pending').execute()
    
    for seq in seq_res.data:
        new_sub = seq['subject'].replace('Unknown Company', new_company).replace('Unknown', new_company)
        new_body = seq['body'].replace('Unknown Company', new_company).replace('Unknown', new_company)
        
        if new_sub != seq['subject'] or new_body != seq['body']:
            sb.table('email_sequences').update({
                'subject': new_sub,
                'body': new_body
            }).eq('id', seq['id']).execute()
            updated_count += 1
            print(f"Updated seq {seq['id']} for {email} -> {new_company}")

print(f"Successfully fixed {updated_count} sequence steps!")
