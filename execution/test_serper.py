import os
import sys
import json
import csv
import time
import requests
import concurrent.futures
from collections import defaultdict
SERPER_API_KEY = "e9d98e2eccdd68ff4916275f405608270f2acdf9"

TARGET_REASONS = [
    'domain_catch_all',
    'smtp_reject_550_safe_shield'
]

def is_target_reason(reason):
    for tr in TARGET_REASONS:
        if tr in reason:
            return True
    return False

def main():
    if not SERPER_API_KEY:
        print("Missing SERPER_API_KEY in .env!")
        return

    # Load JSON to get mapping of email -> company_name
    print("Loading original JSON dataset...")
    email_to_company = {}
    try:
        with open('dataset_leads-finder_2026-03-29_18-15-59-182.json', 'r') as f:
            data = json.load(f)
            for d in data:
                e = d.get('email')
                company = d.get('company_name', '').strip()
                if e and company:
                    email_to_company[e.strip()] = company
    except Exception as e:
        print(f"Error loading JSON: {e}")

    # Read blocked leads
    emails_to_test = []
    reason_counts = defaultdict(int)

    with open('blocked_leads_report.csv', 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            reason = row['Detailed Reason']
            email = row['Email'].strip()
            if is_target_reason(reason):
                company = email_to_company.get(email, '')
                if company:  # We need a company name for the user's requested query pattern
                    emails_to_test.append((email, company, reason))
                    reason_counts[reason] += 1

    print(f"Total target leads to test with Company Names: {len(emails_to_test)}")
    for reason, count in reason_counts.items():
        print(f" - {reason}: {count}")

    def test_serper(lead):
        email, company, reason = lead
        # A/B Test: Search ONLY the email in quotes
        query = f'"{email}"'
        
        payload = json.dumps({
            "q": query,
            "num": 10
        })
        headers = {
            'X-API-KEY': SERPER_API_KEY,
            'Content-Type': 'application/json'
        }
        
        for attempt in range(3):
            try:
                res = requests.request("POST", "https://google.serper.dev/search", headers=headers, data=payload, timeout=20)
                if res.status_code == 429:
                    time.sleep(1.0)
                    continue
                if res.status_code != 200:
                    print(f"API Error {res.status_code} for {email}: {res.text}")
                    return lead, False
                    
                res_json = res.json()
                organic = res_json.get('organic', [])
                email_lower = email.lower()
                
                found = False
                for org in organic:
                    snippet = org.get('snippet', '').lower()
                    title = org.get('title', '').lower()
                    if email_lower in snippet or email_lower in title:
                        found = True
                        break
                        
                return lead, found
            except Exception as ex:
                if attempt == 2:
                    print(f"Exception for {email}: {ex}")
                    return lead, False
                time.sleep(1.0)
        return lead, False

    start_time = time.time()
    
    passed_by_reason = defaultdict(int)
    passed_leads = []

    print("\nInitiating concurrent Serper.dev HTTP requests...")
    # Max workers 4 to comfortably stay under 5 TPS
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(test_serper, lead): lead for lead in emails_to_test}
        
        for idx, future in enumerate(concurrent.futures.as_completed(futures), 1):
            lead, found = future.result()
            email, company, reason = lead
            if found:
                passed_leads.append(email)
                for tr in TARGET_REASONS:
                    if tr in reason:
                        passed_by_reason[tr] += 1
                        break
            
            if idx % 50 == 0:
                print(f"  Processed {idx}/{len(emails_to_test)}...")

    print("\n--- SERPER VERIFICATION RESULTS ---")
    total_passed = 0
    for tr in TARGET_REASONS:
        count = reason_counts.get(tr, 0)
        passed = passed_by_reason.get(tr, 0)
        total_passed += passed
        pct = (passed / count) * 100 if count > 0 else 0
        print(f"{tr}: {passed} / {count} passed Serper Index ({pct:.1f}%)")

    total = len(emails_to_test)
    ov_pct = (total_passed / total) * 100 if total > 0 else 0
    print(f"\nOVERALL RECOVERY: {total_passed} / {total} ({ov_pct:.1f}%) become SENDABLE again.")
    print(f"Time Taken: {time.time() - start_time:.1f}s")


if __name__ == "__main__":
    main()
