import json
import concurrent.futures
import time
from execution.verify_email import check_email
from collections import Counter
import sys

# Add specific path for dns.resolver if needed
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '.vendor')))

def main():
    try:
        data = json.load(open('dataset_leads-finder_2026-03-29_18-15-59-182.json'))
    except FileNotFoundError:
        print("JSON file not found. Please check the path.")
        return

    # Extract non-empty emails
    emails = []
    for d in data:
        e = d.get('email')
        if e and isinstance(e, str) and '@' in e:
            emails.append(e)

    # Take an exact batch of 300
    emails = emails[:300]

    print(f"Testing {len(emails)} unique emails using execution.verify_email.check_email...")

    counters = Counter()
    reasons = Counter()

    start = time.time()

    def worker(email):
        time.sleep(0.1)  # small buffer to prevent aggressive throttling on DNS
        try:
            status, reason = check_email(email)
            return email, status, reason
        except Exception as e:
            return email, 'error', str(e)

    with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
        futures = [executor.submit(worker, e) for e in emails]
        for idx, f in enumerate(concurrent.futures.as_completed(futures), 1):
            email, status, reason = f.result()
            counters[status] += 1
            reasons[f"{status} - {reason}"] += 1
            
            if idx % 100 == 0:
                print(f"  Processed {idx}/{len(emails)}...")

    print("\n" + "="*40)
    print(f"--- VERIFICATION REPORT ---")
    print("="*40)
    print(f"Total Processed: {len(emails)}")
    print(f"Time Elapsed: {time.time() - start:.2f} seconds")
    
    print("\n--- HIGH-LEVEL STATUS ---")
    total = len(emails)
    for k, v in counters.items():
        print(f"  {k.upper()}: {v} ({(v/total)*100:.1f}%)")

    print("\n--- DETAILED REASONS ---")
    for k, v in reasons.most_common():
        print(f"  {k}: {v}")
        
    print("\n" + "="*40)

if __name__ == "__main__":
    main()
