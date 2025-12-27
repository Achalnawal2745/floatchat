#!/usr/bin/env python3
"""
STEP 2: Ingest ARGO Float Data into Database

Run: python ingest_floats.py

This script:
- Reads all .nc files in netcdf_data/
- Updates existing floats (fills NULLs)
- Adds new floats
- No duplicates created
"""

import os
import glob
import asyncio
import asyncpg
import xarray as xr
import pandas as pd
import numpy as np
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

def safe_str(val):
    """Convert to string safely"""
    if isinstance(val, bytes):
        return val.decode('utf-8').strip()
    if isinstance(val, np.ndarray) and val.size == 1:
        return safe_str(val.item())
    return str(val).strip() if val is not None else None

def safe_float(val):
    """Convert to float safely"""
    try:
        return None if pd.isna(val) else float(val)
    except:
        return None

def safe_int(val):
    """Convert to int safely"""
    try:
        if val is None or pd.isna(val):
            return None
        return int(float(str(val)))
    except:
        return None

def safe_date(val):
    """Convert to datetime safely - handles ARGO date format"""
    try:
        if val is None or pd.isna(val):
            return None
        
        # Handle ARGO date format: YYYYMMDDHHMMSS (e.g., '20230926091300')
        if isinstance(val, (bytes, str)):
            date_str = val.decode('utf-8') if isinstance(val, bytes) else str(val)
            date_str = date_str.strip()
            
            # Empty or whitespace-only string
            if not date_str or date_str == '':
                return None
            
            # Try ARGO format first: YYYYMMDDHHMMSS
            if len(date_str) == 14 and date_str.isdigit():
                result = datetime.strptime(date_str, '%Y%m%d%H%M%S')
                return None if pd.isna(result) else result
            
            # Try ISO format
            result = pd.to_datetime(date_str).to_pydatetime()
            return None if pd.isna(result) else result
        
        # Try pandas conversion
        result = pd.to_datetime(val).to_pydatetime()
        return None if pd.isna(result) else result
    except:
        return None

async def ingest_metadata(meta_file, conn):
    """Ingest from *_meta.nc file"""
    print(f"  Reading {os.path.basename(meta_file)}...")
    ds = xr.open_dataset(meta_file)
    
    # Handle both scalar and array platform numbers
    pn_val = ds.PLATFORM_NUMBER.values
    if pn_val.ndim == 0:  # Scalar
        platform_number = int(safe_str(pn_val.item()))
    else:  # Array
        platform_number = int(safe_str(pn_val[0]))
    
    # Helper to extract metadata field safely
    def get_field(field_name):
        if field_name not in ds:
            return None
        val = ds[field_name].values
        if val.ndim == 0:
            return val.item()
        return val[0] if len(val) > 0 else None
    
    # Extract all metadata
    metadata = {
        'platform_number': platform_number,
        'float_serial_number': safe_int(get_field('FLOAT_SERIAL_NO')),
        'pi_name': safe_str(get_field('PI_NAME')),
        'project_name': safe_str(get_field('PROJECT_NAME')),
        'deployment_platform': safe_str(get_field('DEPLOYMENT_PLATFORM')),
        'firmware_version': safe_str(get_field('FIRMWARE_VERSION')),
        'float_owner': safe_str(get_field('FLOAT_OWNER')),
        'operating_institute': safe_str(get_field('OPERATING_INSTITUTION')),
        'launch_date': safe_date(get_field('LAUNCH_DATE')),
        'start_date': safe_date(get_field('START_DATE')),
        'end_of_life': safe_date(get_field('END_MISSION_DATE')),  # Fixed field name!
        'launch_latitude': safe_float(get_field('LAUNCH_LATITUDE')),
        'launch_longitude': safe_float(get_field('LAUNCH_LONGITUDE')),
    }
    
    ds.close()
    
    # Upsert (update if exists, insert if new)
    await conn.execute("""
        INSERT INTO float_metadata (
            platform_number, float_serial_number, pi_name, project_name,
            deployment_platform, firmware_version, float_owner, operating_institute,
            launch_date, start_date, end_of_life, launch_latitude, launch_longitude
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
        ON CONFLICT (platform_number) DO UPDATE SET
            float_serial_number = COALESCE(EXCLUDED.float_serial_number, float_metadata.float_serial_number),
            pi_name = COALESCE(EXCLUDED.pi_name, float_metadata.pi_name),
            project_name = COALESCE(EXCLUDED.project_name, float_metadata.project_name),
            deployment_platform = COALESCE(EXCLUDED.deployment_platform, float_metadata.deployment_platform),
            firmware_version = COALESCE(EXCLUDED.firmware_version, float_metadata.firmware_version),
            float_owner = COALESCE(EXCLUDED.float_owner, float_metadata.float_owner),
            operating_institute = COALESCE(EXCLUDED.operating_institute, float_metadata.operating_institute),
            launch_date = COALESCE(EXCLUDED.launch_date, float_metadata.launch_date),
            start_date = COALESCE(EXCLUDED.start_date, float_metadata.start_date),
            end_of_life = COALESCE(EXCLUDED.end_of_life, float_metadata.end_of_life),
            launch_latitude = COALESCE(EXCLUDED.launch_latitude, float_metadata.launch_latitude),
            launch_longitude = COALESCE(EXCLUDED.launch_longitude, float_metadata.launch_longitude)
    """, metadata['platform_number'], metadata['float_serial_number'], 
        metadata['pi_name'], metadata['project_name'], metadata['deployment_platform'],
        metadata['firmware_version'], metadata['float_owner'], metadata['operating_institute'],
        metadata['launch_date'], metadata['start_date'], metadata['end_of_life'],
        metadata['launch_latitude'], metadata['launch_longitude'])
    
    print(f"  [OK] Metadata updated")
    return platform_number

async def ingest_profiles(prof_file, conn):
    """Ingest from *_prof.nc file"""
    print(f"  Reading {os.path.basename(prof_file)}...")
    ds = xr.open_dataset(prof_file)
    
    platform_number = int(safe_str(ds.PLATFORM_NUMBER.values[0]))
    n_profs = ds.dims['N_PROF']
    
    total_measurements = 0
    
    for i in range(n_profs):
        cycle_number = int(ds.CYCLE_NUMBER.values[i])
        profile_date = safe_date(ds.JULD.values[i]) or datetime.now()
        lat = safe_float(ds.LATITUDE.values[i])
        lon = safe_float(ds.LONGITUDE.values[i])
        
        # Upsert profile
        await conn.execute("""
            INSERT INTO profiles (float_id, cycle_number, profile_date, latitude, longitude)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (float_id, cycle_number) DO NOTHING
        """, platform_number, cycle_number, profile_date, lat, lon)
        
        # Insert measurements
        pres = ds.PRES.values[i]
        temp = ds.TEMP.values[i]
        psal = ds.PSAL.values[i]
        
        records = []
        for lvl in range(len(pres)):
            p_val = float(pres[lvl])
            if np.isnan(p_val): continue
            
            t_val = safe_float(temp[lvl])
            s_val = safe_float(psal[lvl])
            
            if t_val is None and s_val is None: continue
            
            records.append((platform_number, cycle_number, int(lvl), p_val, p_val, t_val, s_val))
        
        if records:
            await conn.copy_records_to_table(
                'measurements', records=records,
                columns=['float_id', 'cycle_number', 'n_level', 'pressure', 'depth_m', 'temperature', 'salinity'],
                schema_name='public'
            )
            total_measurements += len(records)
    
    ds.close()
    print(f"  [OK] {n_profs} profiles, {total_measurements} measurements")
    return platform_number

async def ingest_float(float_id, data_dir="netcdf_data"):
    """Ingest both files for one float"""
    meta_file = os.path.join(data_dir, f"{float_id}_meta.nc")
    prof_file = os.path.join(data_dir, f"{float_id}_prof.nc")
    
    print(f"\n[Float {float_id}]")
    
    conn = await asyncpg.connect(DATABASE_URL, statement_cache_size=0)
    
    try:
        if os.path.exists(meta_file):
            await ingest_metadata(meta_file, conn)
        else:
            print(f"  ⚠ No metadata file")
        
        if os.path.exists(prof_file):
            await ingest_profiles(prof_file, conn)
        else:
            print(f"  ⚠ No profile file")
            
    finally:
        await conn.close()

async def main():
    # Find all unique float IDs
    prof_files = glob.glob("netcdf_data/*_prof.nc")
    float_ids = sorted(set(os.path.basename(f).replace("_prof.nc", "") for f in prof_files))
    
    print("="*60)
    print(f"Ingesting {len(float_ids)} floats...")
    print("="*60)
    
    for float_id in float_ids:
        await ingest_float(float_id)
    
    print(f"\n{'='*60}")
    print("Complete! Run: python verify_ingestion.py")
    print("="*60)

if __name__ == "__main__":
    asyncio.run(main())
