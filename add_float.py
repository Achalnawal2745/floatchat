"""
Add Float to Database via Backend API
Usage: python add_float.py FLOAT_ID
Example: python add_float.py 1902669
"""

import sys
import requests

if len(sys.argv) < 2:
    print("Usage: python add_float.py FLOAT_ID")
    print("Example: python add_float.py 1902669")
    sys.exit(1)

float_id = sys.argv[1]
base_url = "http://127.0.0.1:8000"

print(f"\n{'='*60}")
print(f"Adding Float {float_id} to Database")
print(f"{'='*60}\n")

# Step 1: Download
print(f"[1/2] Downloading NetCDF files for float {float_id}...")
try:
    response = requests.post(
        f"{base_url}/admin/download-float",
        json={"float_id": float_id},
        timeout=120
    )
    result = response.json()
    
    if result.get("success"):
        print(f"  [OK] {result['message']}")
        print(f"  Files: {', '.join(result['files_downloaded'])}")
    else:
        print(f"  [FAIL] {result.get('message', 'Download failed')}")
        sys.exit(1)
except Exception as e:
    print(f"  [ERROR] {e}")
    sys.exit(1)

# Step 2: Ingest
print(f"\n[2/2] Ingesting data into database...")
try:
    response = requests.post(
        f"{base_url}/admin/ingest-float",
        json={"float_id": float_id},
        timeout=120
    )
    result = response.json()
    
    if result.get("success"):
        print(f"  [OK] {result['message']}")
        print(f"  Profiles: {result['profiles_count']}")
        print(f"  Measurements: {result['measurements_count']}")
    else:
        print(f"  [FAIL] {result.get('message', 'Ingestion failed')}")
        sys.exit(1)
except Exception as e:
    print(f"  [ERROR] {e}")
    sys.exit(1)

print(f"\n{'='*60}")
print(f"[SUCCESS] Float {float_id} added to database!")
print(f"{'='*60}\n")
