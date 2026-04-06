import os
import sys
import json
import requests
import time
from pathlib import Path
from dotenv import load_dotenv

env_path = Path(__file__).resolve().parent.parent / '.env'
try:
    load_dotenv(env_path)
except:
    pass

PERPLEXITY_API_KEY = os.getenv('PERPLEXITY_API_KEY')
urls = [
    "https://www.goodadsmatterawards.com/b2b",
    "https://www.goodadsmatterawards.com/audio-radio",
    "https://www.goodadsmatterawards.com/press-outdoor-digital",
    "https://www.goodadsmatterawards.com/culture-and-collaboration",
    "https://www.goodadsmatterawards.com/films-for-good",
    "https://www.goodadsmatterawards.com/film",
    "https://www.goodadsmatterawards.com/young-artist-of-the-year",
    "https://www.goodadsmatterawards.com/costume-hair-makeup",
    "https://www.goodadsmatterawards.com/visual-effects-and-ai",
    "https://www.goodadsmatterawards.com/animation",
    "https://www.goodadsmatterawards.com/colour-grading",
    "https://www.goodadsmatterawards.com/editing",
    "https://www.goodadsmatterawards.com/casting-and-performance",
    "https://www.goodadsmatterawards.com/production-design",
    "https://www.goodadsmatterawards.com/music-and-sound-design",
    "https://www.goodadsmatterawards.com/writing-for-advertising-winners",
    "https://www.goodadsmatterawards.com/cinematography",
    "https://www.goodadsmatterawards.com/direction"
]

def scrape_url_with_jina(url):
    jina_url = f"https://r.jina.ai/{url}"
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        response = requests.get(jina_url, headers=headers, timeout=30)
        if response.status_code == 200:
            return response.text
    except Exception as e:
        print(f"Failed to scrape {url}: {e}")
    return None

def extract_with_perplexity(markdown_text, category_name):
    if not markdown_text: return []
    
    headers = {
        "Authorization": f"Bearer {PERPLEXITY_API_KEY}",
        "Content-Type": "application/json"
    }
    
    instruction = f"Extract a list of all awards listed in the markdown text. For each award, look for the Name of the ad/project and the company or person it was 'Entered by'. The category is {category_name}. Make sure you capture ALL entries on the page.\n\nOUTPUT ONLY RAW JSON in this exact structure: [{{\"ad_name\": \"...\", \"entered_by\": \"...\", \"category\": \"{category_name}\"}}]. Do not include any standard text, only the raw JSON block."
    
    payload = {
        "model": "sonar",
        "messages": [
            {"role": "system", "content": instruction},
            {"role": "user", "content": markdown_text[:15000]}
        ],
        "temperature": 0.1
    }
    
    try:
        print(f"  Sending {len(markdown_text[:15000])} chars to Perplexity API...")
        response = requests.post("https://api.perplexity.ai/chat/completions", headers=headers, json=payload, timeout=90)
        data = response.json()
        if "error" in data:
            print(f"Perplexity API limit/error: {data}")
            return []
            
        content = data['choices'][0]['message']['content'].strip()
        if '```json' in content: 
            content = content.split('```json')[1].split('```')[0].strip()
        elif '```' in content: 
            content = content.split('```')[1].split('```')[0].strip()
            
        entries = json.loads(content)
        return entries
    except Exception as e:
        print(f"Perplexity error for {category_name}: {e}")
        return []

all_entries = []

for url in urls:
    category = url.split('/')[-1]
    print(f"Scraping category: {category}...")
    md = scrape_url_with_jina(url)
    if md:
        print(f"  Fetched {len(md)} chars. Extracting with Perplexity...")
        entries = extract_with_perplexity(md, category)
        print(f"  Found {len(entries)} entries.")
        all_entries.extend(entries)
    time.sleep(1)

print("\n\n--- RESULTS TABLE ---\n")
print("| Category | Ad Name | Entered By |")
print("|---|---|---|")
for row in all_entries:
    print(f"| {row['category']} | {row['ad_name']} | {row['entered_by']} |")

with open("/Users/bipul/Downloads/ALL WORKSPACES/festivals outreach/awards_extracted.csv", 'w') as f:
    f.write("Category,Ad Name,Entered By\n")
    for row in all_entries:
        ad_name = row['ad_name'].replace('"', '""')
        entered_by = row['entered_by'].replace('"', '""')
        f.write(f"{row['category']},\"{ad_name}\",\"{entered_by}\"\n")
