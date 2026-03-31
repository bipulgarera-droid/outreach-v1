import csv
import os
import sys
from collections import defaultdict
from apify_client import ApifyClient

apify_key = os.environ.get('APIFY_API_TOKEN')
client = ApifyClient(apify_key)

TARGET_REASONS = [
    'domain_catch_all',
    'smtp_reject_550_safe_shield',
    'smtp_timeout'
]

def is_target_reason(reason):
    for tr in TARGET_REASONS:
        if tr in reason:
            return True
    return False

emails_to_test = []
reason_counts = defaultdict(int)

with open('blocked_leads_report.csv', 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        reason = row['Detailed Reason']
        if is_target_reason(reason):
            emails_to_test.append(row)
            reason_counts[reason] += 1

print(f"Total target leads to test: {len(emails_to_test)}")
for reason, count in reason_counts.items():
    print(f" - {reason}: {count}")
    
# Gather exact unique queries
unique_emails = list({row['Email'].strip() for row in emails_to_test if row['Email'].strip()})
lines = [f'"{e}"' for e in unique_emails]

print(f"\nSending {len(lines)} unique emails to Apify scraping actor...")
keyword_payload = "\n".join(lines)

run_input = {
    "keyword": keyword_payload,
    "include_merged": True,
    "limit": "10"
}

run = client.actor("scraperlink/google-search-results-serp-scraper").call(run_input=run_input, timeout_secs=600)
dataset_id = run.get("defaultDatasetId")

if not dataset_id:
    print("No dataset returned from Apify!")
    sys.exit(1)

verified_emails = set()
for item in client.dataset(dataset_id).iterate_items():
    query_val = str(item.get('query', '')).strip().strip('"')
    results = item.get('results', [])
    if isinstance(results, list) and len(results) > 0:
        verified_emails.add(query_val)
        
passed_by_reason = defaultdict(int)

for row in emails_to_test:
    e = row['Email'].strip()
    reason = row['Detailed Reason']
    # Match the prefix of the reason to our targets for clean grouping
    for tr in TARGET_REASONS:
        if tr in reason:
            if e in verified_emails:
                passed_by_reason[tr] += 1
            break

print("\n--- RESULTS ---")
total_passed = 0
for tr in TARGET_REASONS:
    count = reason_counts.get(tr, 0)
    passed = passed_by_reason.get(tr, 0)
    total_passed += passed
    pct = (passed / count) * 100 if count > 0 else 0
    print(f"{tr}: {passed} / {count} passed Google Index ({pct:.1f}%)")

total = len(emails_to_test)
ov_pct = (total_passed / total) * 100 if total > 0 else 0
print(f"\nOVERALL RECOVERY: {total_passed} / {total} ({ov_pct:.1f}%) become SENDABLE again.")
