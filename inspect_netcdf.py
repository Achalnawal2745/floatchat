#!/usr/bin/env python3
"""Inspect NetCDF file structure to understand what we downloaded"""

import xarray as xr
import os

def inspect_netcdf():
    """Show what's actually in the NetCDF file"""
    
    nc_file = "netcdf_data/1900121_D1900121_001.nc"
    
    if not os.path.exists(nc_file):
        print(f"File not found: {nc_file}")
        return
    
    print("=" * 60)
    print(f"INSPECTING: {nc_file}")
    print("=" * 60)
    
    ds = xr.open_dataset(nc_file)
    
    print("\nFILE DIMENSIONS:")
    for dim, size in ds.dims.items():
        print(f"  {dim}: {size}")
    
    print("\nAVAILABLE VARIABLES:")
    for var in sorted(ds.data_vars):
        print(f"  {var}")
    
    print("\nGLOBAL ATTRIBUTES:")
    for attr in sorted(ds.attrs.keys())[:10]:  # First 10 attributes
        val = str(ds.attrs[attr])[:50]
        print(f"  {attr}: {val}")
    
    print("\nKEY METADATA:")
    try:
        print(f"  PLATFORM_NUMBER: {ds.PLATFORM_NUMBER.values}")
        print(f"  PI_NAME: {ds.PI_NAME.values}")
        print(f"  PROJECT_NAME: {ds.PROJECT_NAME.values}")
    except:
        print("  (Some metadata missing)")
    
    print("\nPROFILE INFO:")
    print(f"  Number of profiles (N_PROF): {ds.dims.get('N_PROF', 'N/A')}")
    print(f"  Number of levels (N_LEVELS): {ds.dims.get('N_LEVELS', 'N/A')}")
    
    ds.close()
    
    print("\n" + "=" * 60)
    print("FILE TYPE EXPLANATION:")
    print("=" * 60)
    print("""
ARGO has different file types:
1. D*.nc (Descending profiles) - Single profile, limited metadata
2. R*.nc (Real-time profiles) - Single profile
3. *_prof.nc (Merged profiles) - ALL profiles for a float
4. *_meta.nc (Metadata) - Complete float metadata
5. *_tech.nc (Technical data) - Sensor info

For complete data, we need the *_prof.nc file!
    """)

if __name__ == "__main__":
    inspect_netcdf()
