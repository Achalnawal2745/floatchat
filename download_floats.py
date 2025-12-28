#!/usr/bin/env python3
"""
Download ARGO Float NetCDF Files (HTTP Version)
Downloads both *_meta.nc and *_prof.nc files for specified floats
"""

import os
import requests

# ============ EDIT THIS LIST ============
FLOAT_IDS = [ 
    '2903894'
]
# ========================================

# Configuration
BASE_URL = 'http://data-argo.ifremer.fr/dac/incois'
DATA_DIR = 'netcdf_data'

# Ensure data directory exists
os.makedirs(DATA_DIR, exist_ok=True)

def download_float(float_id):
    """Download meta and prof files for a float via HTTP"""
    print(f"\n[Float {float_id}]")
    
    files = [f'{float_id}_meta.nc', f'{float_id}_prof.nc']
    success_count = 0
    
    for filename in files:
        local_path = os.path.join(DATA_DIR, filename)
        
        # Skip if file exists and is not empty
        if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
            print(f"  [OK] {filename} (already exists)")
            success_count += 1
            continue
        
        # Download via HTTP
        url = f"{BASE_URL}/{float_id}/{filename}"
        print(f"  Downloading {filename}...")
        
        try:
            response = requests.get(url, timeout=60)
            
            if response.status_code == 200:
                with open(local_path, 'wb') as f:
                    f.write(response.content)
                
                size = os.path.getsize(local_path)
                print(f"  [OK] {filename} ({size:,} bytes)")
                success_count += 1
            else:
                print(f"  [FAIL] Error: HTTP {response.status_code} - File not found")
        except Exception as e:
            print(f"  [ERROR] {e}")
    
    return success_count == 2

if __name__ == '__main__':
    print("=" * 60)
    print(f"Downloading {len(FLOAT_IDS)} floats via HTTP...")
    print("=" * 60)
    
    success = 0
    for float_id in FLOAT_IDS:
        if download_float(float_id):
            success += 1
    
    print("\n" + "=" * 60)
    print(f"Complete: {success}/{len(FLOAT_IDS)} floats downloaded")
    print("=" * 60)
    print("\nNext step: python ingest_floats.py")
