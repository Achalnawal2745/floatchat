#!/usr/bin/env python3
"""
STEP 1: Download ARGO Float Files

Edit FLOAT_IDS below, then run: python download_floats.py
"""

from ftplib import FTP
import os

# ============ EDIT THIS LIST ============
FLOAT_IDS = [
    '1902669',
    '1902670', 
    '2902676',
]
# ========================================

FTP_SERVER = 'ftp.ifremer.fr'
FTP_PATH = '/ifremer/argo/dac/incois'
DATA_DIR = 'netcdf_data'

def download_float(float_id):
    """Download metadata and profile files"""
    print(f"\n[Float {float_id}]")
    
    try:
        ftp = FTP(FTP_SERVER, timeout=30)
        ftp.login()
        ftp.cwd(f"{FTP_PATH}/{float_id}")
        
        files = [f'{float_id}_meta.nc', f'{float_id}_prof.nc']
        
        for filename in files:
            local_path = os.path.join(DATA_DIR, filename)
            
            if os.path.exists(local_path):
                print(f"  ✓ {filename} (already exists)")
                continue
            
            print(f"  Downloading {filename}...")
            with open(local_path, 'wb') as f:
                ftp.retrbinary(f'RETR {filename}', f.write)
            
            size = os.path.getsize(local_path)
            print(f"  ✓ {filename} ({size:,} bytes)")
        
        ftp.quit()
        return True
        
    except Exception as e:
        print(f"  ✗ Error: {e}")
        return False

if __name__ == "__main__":
    os.makedirs(DATA_DIR, exist_ok=True)
    
    print("="*60)
    print(f"Downloading {len(FLOAT_IDS)} floats...")
    print("="*60)
    
    success = sum(download_float(fid) for fid in FLOAT_IDS)
    
    print(f"\n{'='*60}")
    print(f"Complete: {success}/{len(FLOAT_IDS)} floats downloaded")
    print(f"{'='*60}")
    print("\nNext step: python ingest_floats.py")
