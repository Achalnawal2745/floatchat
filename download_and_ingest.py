#!/usr/bin/env python3
"""
Easy ARGO Float Downloader & Ingester

HOW TO USE:
1. Edit the FLOAT_IDS list below with the float IDs you want to download
2. Run: python download_and_ingest.py
3. Wait for completion - it will download and ingest automatically

FINDING FLOAT IDs:
- Browse: ftp://ftp.ifremer.fr/ifremer/argo/dac/incois/
- Or use ARGO float tracker: https://www.ocean-ops.org/board
"""

from ftplib import FTP
import os
import asyncio
import sys

# ============================================================
# EDIT THIS LIST - Add float IDs you want to download
# ============================================================
FLOAT_IDS = [
    '1902669',  # INCOIS float
    '1902670',  # INCOIS float
    '2902676',  # INCOIS float
    # Add more float IDs here...
]
# ============================================================

FTP_SERVER = 'ftp.ifremer.fr'
FTP_PATH = '/ifremer/argo/dac/incois'
DATA_DIR = 'netcdf_data'

def download_float(float_id):
    """Download both metadata and profile files for a float"""
    
    print(f"\n{'='*60}")
    print(f"Downloading Float: {float_id}")
    print(f"{'='*60}")
    
    try:
        # Connect to FTP
        print("Connecting to ARGO FTP server...")
        ftp = FTP(FTP_SERVER, timeout=30)
        ftp.login()
        
        # Navigate to float directory
        float_path = f"{FTP_PATH}/{float_id}"
        ftp.cwd(float_path)
        
        items = ftp.nlst()
        
        # Files to download
        files_to_download = [
            f'{float_id}_meta.nc',  # Metadata (launch date, serial, etc.)
            f'{float_id}_prof.nc',  # Profiles (measurements, cycles)
        ]
        
        downloaded = []
        
        for filename in files_to_download:
            if filename not in items:
                print(f"  WARNING: {filename} not found")
                continue
            
            local_path = os.path.join(DATA_DIR, filename)
            
            if os.path.exists(local_path):
                print(f"  Already exists: {filename}")
                downloaded.append(filename)
                continue
            
            print(f"  Downloading {filename}...")
            with open(local_path, 'wb') as f:
                ftp.retrbinary(f'RETR {filename}', f.write)
            
            file_size = os.path.getsize(local_path)
            print(f"  ✓ {filename} ({file_size:,} bytes)")
            downloaded.append(filename)
        
        ftp.quit()
        
        if len(downloaded) > 0:
            print(f"  Downloaded {len(downloaded)}/2 files")
            return True
        else:
            print(f"  No files downloaded for {float_id}")
            return False
        
    except Exception as e:
        print(f"  ERROR downloading float {float_id}: {e}")
        return False

async def run_ingestion():
    """Run the enhanced ingestion script"""
    
    print(f"\n{'='*60}")
    print("Running Enhanced Ingestion Script")
    print(f"{'='*60}\n")
    
    # Import and run enhanced ingestion
    import subprocess
    result = subprocess.run([sys.executable, 'ingest_argo_enhanced.py'], 
                          capture_output=False, text=True)
    
    return result.returncode == 0

def main():
    """Main execution"""
    
    print("\n" + "="*60)
    print("ARGO FLOAT DOWNLOADER & INGESTER")
    print("="*60)
    
    # Create data directory
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
        print(f"Created directory: {DATA_DIR}")
    
    # Show float list
    print(f"\nFloats to download: {len(FLOAT_IDS)}")
    for fid in FLOAT_IDS:
        print(f"  - {fid}")
    
    input("\nPress ENTER to start downloading...")
    
    # Download each float
    success_count = 0
    for float_id in FLOAT_IDS:
        if download_float(float_id):
            success_count += 1
    
    print(f"\n{'='*60}")
    print(f"Download Summary: {success_count}/{len(FLOAT_IDS)} successful")
    print(f"{'='*60}")
    
    if success_count > 0:
        print("\nStarting ingestion...")
        asyncio.run(run_ingestion())
    else:
        print("\nNo files downloaded. Skipping ingestion.")
    
    print("\n" + "="*60)
    print("COMPLETE!")
    print("="*60)
    print("\nTo verify, run: python verify_ingestion.py")

if __name__ == "__main__":
    main()
