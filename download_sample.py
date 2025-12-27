#!/usr/bin/env python3
"""Download sample ARGO NetCDF file from FTP server"""

from ftplib import FTP
import os

def download_argo_file():
    """Download a sample ARGO NetCDF file"""
    
    print("Connecting to ARGO FTP server...")
    ftp = FTP('ftp.ifremer.fr', timeout=30)
    ftp.login()  # Anonymous login
    
    # Navigate to INCOIS data
    print("Navigating to INCOIS float data...")
    ftp.cwd('/ifremer/argo/dac/incois')
    
    # Get first float ID
    floats = ftp.nlst()
    float_id = floats[0]
    print(f"Selected float: {float_id}")
    
    # Navigate to float directory
    ftp.cwd(float_id)
    
    # Check what's available
    items = ftp.nlst()
    print(f"Available items: {items}")
    
    # Look for the merged profile file (contains ALL cycles)
    prof_file = f'{float_id}_prof.nc'
    
    if prof_file in items:
        target_file = prof_file
        print(f"Found merged profile file: {prof_file}")
    elif 'profiles' in items:
        # Fallback to profiles directory
        ftp.cwd('profiles')
        nc_files = [f for f in ftp.nlst() if f.endswith('.nc')]
        if nc_files:
            target_file = nc_files[0]
            print(f"Using profile from profiles/ directory: {target_file}")
        else:
            print("No NetCDF files found!")
            return False
    else:
        print("No merged profile file or profiles directory found!")
        return False
    local_path = os.path.join('netcdf_data', f'{float_id}_{target_file}')
    
    print(f"Downloading {target_file} to {local_path}...")
    
    with open(local_path, 'wb') as f:
        ftp.retrbinary(f'RETR {target_file}', f.write)
    
    file_size = os.path.getsize(local_path)
    print(f"✓ Downloaded successfully! Size: {file_size} bytes")
    print(f"File saved to: {local_path}")
    
    ftp.quit()
    return True

if __name__ == "__main__":
    try:
        download_argo_file()
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
