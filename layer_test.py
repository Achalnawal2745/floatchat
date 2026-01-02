import requests
import json

URL = "http://127.0.0.1:8000/query"

query = "path of float 2900565"
expected = "Trajectory path"

print(f"Testing: '{query}'")
print("="*70)

try:
    resp = requests.post(URL, json={"query": query, "session_id": "single_test"}, timeout=60)
    
    if resp.status_code == 200:
        data = resp.json()
        text = data.get("ai_synthesized_response", "")
        print(f"Response: {text}")
        
        if text and expected in text:
            print("✅ PASS")
        else:
            print(f"❌ FAIL - Expected '{expected}'")
    else:
        print(f"❌ ERROR: {resp.status_code}")
except Exception as e:
    print(f"❌ EXCEPTION: {e}")
