import json
import csv
import concurrent.futures
import time
from execution.verify_email import check_email
from collections import Counter
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '.vendor')))

def main():
    try:
        data = json.load(open('dataset_leads-finder_2026-03-29_18-15-59-182.json'))
    except FileNotFoundError:
        print("JSON file not found.")
        return

    # Extract non-empty emails
    leads = []
    for d in data:
        e = d.get('email')
        if e and isinstance(e, str) and '@' in e:
            name = d.get('full_name') or f"{d.get('first_name', '')} {d.get('last_name', '')}".strip()
            leads.append((name, e))

    # Take 1000 leads
    leads = leads[:1000]
    print(f"Testing {len(leads)} unique emails using execution.verify_email.check_email...")

    counters = Counter()
    reasons = Counter()
    
    blocked_leads = []

    start = time.time()

    def worker(lead):
        name, email = lead
        time.sleep(0.1)
        try:
            status, reason = check_email(email)
            return name, email, status, reason
        except Exception as e:
            return name, email, 'error', str(e)

    with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
        futures = [executor.submit(worker, lead) for lead in leads]
        for idx, f in enumerate(concurrent.futures.as_completed(futures), 1):
            name, email, status, reason = f.result()
            counters[status] += 1
            reasons[f"{status} - {reason}"] += 1
            
            # If not perfectly Valid 'smtp_ok', log it to the CSV
            if status != 'valid' or reason == 'domain_catch_all':
                blocked_leads.append([name, email, status, reason])
            
            if idx % 100 == 0:
                print(f"  Processed {idx}/{len(leads)}...")

    # Write CSV
    csv_file = 'blocked_leads_report.csv'
    with open(csv_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Name', 'Email', 'Status', 'Detailed Reason'])
        writer.writerows(blocked_leads)

    print("\n" + "="*40)
    print(f"--- VERIFICATION REPORT ---")
    print("="*40)
    print(f"Total Processed: {len(leads)}")
    print(f"Time Elapsed: {time.time() - start:.2f} seconds")
    
    print("\n--- HIGH-LEVEL STATUS ---")
    total = len(leads)
    for k, v in counters.items():
        print(f"  {k.upper()}: {v} ({(v/total)*100:.1f}%)")

    print("\n--- DETAILED REASONS ---")
    for k, v in reasons.most_common():
        print(f"  {k}: {v}")
        
    print(f"\nCSV Exported to: {csv_file} with {len(blocked_leads)} blocked/risky/catch-all leads.")
    print("="*40)

if __name__ == "__main__":
    main()
