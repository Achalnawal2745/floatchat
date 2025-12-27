#!/usr/bin/env python3
"""Inspect what's actually in the metadata NetCDF files"""

import xarray as xr
import os

def inspect_meta_file(filename):
    """Show all variables in a metadata file"""
    
    if not os.path.exists(filename):
        print(f"File not found: {filename}")
        return
    
    print(f"\n{'='*60}")
    print(f"Inspecting: {filename}")
    print(f"{'='*60}")
    
    ds = xr.open_dataset(filename)
    
    print("\nALL VARIABLES:")
    for var in sorted(ds.data_vars):
        print(f"  - {var}")
    
    print("\nKEY METADATA VALUES:")
    
    # Check for common fields
    fields_to_check = [
        'PLATFORM_NUMBER',
        'FLOAT_SERIAL_NO',
        'LAUNCH_DATE',
        'START_DATE',
        'END_OF_MISSION_DATE',
        'LAUNCH_LATITUDE',
        'LAUNCH_LONGITUDE',
        'PI_NAME',
        'PROJECT_NAME',
        'DEPLOYMENT_PLATFORM',
        'FIRMWARE_VERSION',
        'FLOAT_OWNER',
        'OPERATING_INSTITUTION'
    ]
    
    for field in fields_to_check:
        if field in ds:
            val = ds[field].values
            if val.ndim == 0:
                val_str = str(val.item())[:50]
            else:
                val_str = str(val[0] if len(val) > 0 else "")[:50]
            print(f"  {field:30} = {val_str}")
        else:
            print(f"  {field:30} = [NOT FOUND]")
    
    ds.close()
    print()

if __name__ == "__main__":
    inspect_meta_file("netcdf_data/1902669_meta.nc")
    inspect_meta_file("netcdf_data/1902670_meta.nc")
