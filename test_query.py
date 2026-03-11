import os
import json
from supabase import create_client, Client
from dotenv import load_dotenv
from datetime import datetime, timedelta
load_dotenv('.env')

url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_SERVICE_KEY")
supabase = create_client(url, key)

now = datetime.utcnow() + timedelta(hours=5, minutes=30)
date_str = now.strftime('%Y-%m-%d')
print(f"Comparing against: {date_str}")

res = supabase.table('email_sequences').select('id, status, scheduled_at').eq('status', 'pending').lte('scheduled_at', date_str).execute()
print(f"Found {len(res.data)} items")
print(json.dumps(res.data, indent=2))
