#!/usr/bin/env python3
import os
import sys
import requests
from dotenv import load_dotenv
from pathlib import Path

# Add parent dir to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

env_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(env_path)

SERPER_API_KEY = os.getenv('SERPER_API_KEY')

def test_serper():
    if not SERPER_API_KEY:
        print("❌ SERPER_API_KEY not found in .env")
        return

    print(f"Testing Serper API with key: {SERPER_API_KEY[:4]}...{SERPER_API_KEY[-4:]}")
    
    url = "https://google.serper.dev/search"
    payload = {"q": "test"}
    headers = {
        'X-API-KEY': SERPER_API_KEY,
        'Content-Type': 'application/json'
    }

    try:
        response = requests.post(url, json=payload, headers=headers)
        if response.status_code == 200:
            print("✅ Serper API is working!")
        else:
            print(f"❌ Serper API failed with status {response.status_code}: {response.text}")
    except Exception as e:
        print(f"❌ Error testing Serper API: {str(e)}")

if __name__ == "__main__":
    test_serper()
