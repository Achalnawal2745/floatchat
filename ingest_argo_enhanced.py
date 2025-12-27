#!/usr/bin/env python3
"""
Enhanced ARGO Ingestion - Reads BOTH profile and metadata files

This script ingests:
1. *_meta.nc - Float metadata (launch date, serial number, etc.)
2. *_prof.nc - Profile data (measurements, cycles)
"""

import os
import glob
import asyncio
import asyncpg
import xarray as xr
import pandas as pd
import numpy as np
from datetime import datetime
import logging
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable is not set")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

class EnhancedArgoIngester:
    def __init__(self, db_url):
        self.db_url = db_url

    def safe_str(self, val):
        """Convert bytes or other types to string safely"""
        if isinstance(val, bytes):
            return val.decode('utf-8').strip()
        if isinstance(val, np.ndarray):
            if val.size == 1:
                return self.safe_str(val.item())
            return str(val[0]) if len(val) > 0 else ""
        return str(val).strip() if val is not None else None

    def safe_float(self, val):
        """Convert to float safely"""
        try:
            if pd.isna(val) or val is None:
                return None
            return float(val)
        except:
            return None

    def safe_date(self, val):
        """Convert to datetime safely"""
        try:
            if pd.isna(val) or val is None:
                return None
            return pd.to_datetime(val).to_pydatetime()
        except:
            return None

    async def ingest_metadata(self, meta_file: str, conn):
        """Ingest metadata from *_meta.nc file"""
        try:
            logger.info(f"Reading metadata from: {meta_file}")
            ds = xr.open_dataset(meta_file)
            
            # Extract metadata
            platform_number = int(self.safe_str(ds.PLATFORM_NUMBER.values[0]))
            
            # Get all available metadata fields
            metadata = {
                'platform_number': platform_number,
                'float_serial_number': self.safe_str(ds.FLOAT_SERIAL_NO.values[0]) if 'FLOAT_SERIAL_NO' in ds else None,
                'pi_name': self.safe_str(ds.PI_NAME.values[0]) if 'PI_NAME' in ds else None,
                'project_name': self.safe_str(ds.PROJECT_NAME.values[0]) if 'PROJECT_NAME' in ds else None,
                'deployment_platform': self.safe_str(ds.DEPLOYMENT_PLATFORM.values[0]) if 'DEPLOYMENT_PLATFORM' in ds else None,
                'firmware_version': self.safe_str(ds.FIRMWARE_VERSION.values[0]) if 'FIRMWARE_VERSION' in ds else None,
                'float_owner': self.safe_str(ds.FLOAT_OWNER.values[0]) if 'FLOAT_OWNER' in ds else None,
                'operating_institute': self.safe_str(ds.OPERATING_INSTITUTION.values[0]) if 'OPERATING_INSTITUTION' in ds else None,
                'launch_date': self.safe_date(ds.LAUNCH_DATE.values[0]) if 'LAUNCH_DATE' in ds else None,
                'start_date': self.safe_date(ds.START_DATE.values[0]) if 'START_DATE' in ds else None,
                'end_of_life': self.safe_date(ds.END_OF_MISSION_DATE.values[0]) if 'END_OF_MISSION_DATE' in ds else None,
                'launch_latitude': self.safe_float(ds.LAUNCH_LATITUDE.values[0]) if 'LAUNCH_LATITUDE' in ds else None,
                'launch_longitude': self.safe_float(ds.LAUNCH_LONGITUDE.values[0]) if 'LAUNCH_LONGITUDE' in ds else None,
            }
            
            ds.close()
            
            # Upsert metadata
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
            
            logger.info(f"✓ Metadata updated for float {platform_number}")
            return platform_number
            
        except Exception as e:
            logger.error(f"Failed to ingest metadata from {meta_file}: {e}")
            return None

    async def ingest_profiles(self, prof_file: str, conn):
        """Ingest profiles from *_prof.nc file (existing logic)"""
        try:
            logger.info(f"Reading profiles from: {prof_file}")
            ds = xr.open_dataset(prof_file)
            
            platform_number = int(self.safe_str(ds.PLATFORM_NUMBER.values[0]))
            n_profs = ds.dims['N_PROF']
            
            for i in range(n_profs):
                cycle_number = int(ds.CYCLE_NUMBER.values[i])
                date_val = ds.JULD.values[i]
                profile_date = pd.to_datetime(date_val).to_pydatetime() if pd.notnull(date_val) else datetime.now()
                lat = float(ds.LATITUDE.values[i]) if pd.notnull(ds.LATITUDE.values[i]) else None
                lon = float(ds.LONGITUDE.values[i]) if pd.notnull(ds.LONGITUDE.values[i]) else None
                
                # Insert Profile
                await conn.execute("""
                    INSERT INTO profiles (
                        float_id, cycle_number, profile_date, latitude, longitude
                    ) VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (float_id, cycle_number) DO NOTHING
                """, platform_number, cycle_number, profile_date, lat, lon)
                
                # Extract Measurements
                pres = ds.PRES.values[i]
                temp = ds.TEMP.values[i]
                psal = ds.PSAL.values[i]
                
                records = []
                for lvl in range(len(pres)):
                    p_val = float(pres[lvl])
                    if np.isnan(p_val): continue
                    
                    t_val = float(temp[lvl]) if not np.isnan(temp[lvl]) else None
                    s_val = float(psal[lvl]) if not np.isnan(psal[lvl]) else None
                    
                    if t_val is None and s_val is None: continue
                    
                    records.append((
                        platform_number, cycle_number, int(lvl),
                        p_val, p_val, t_val, s_val
                    ))
                
                if records:
                    await conn.copy_records_to_table(
                        'measurements',
                        records=records,
                        columns=['float_id', 'cycle_number', 'n_level', 'pressure', 'depth_m', 'temperature', 'salinity'],
                        schema_name='public'
                    )
                    logger.info(f"  Inserted {len(records)} measurements for cycle {cycle_number}")
            
            ds.close()
            logger.info(f"✓ Profiles ingested for float {platform_number}")
            return platform_number
            
        except Exception as e:
            logger.error(f"Failed to ingest profiles from {prof_file}: {e}")
            return None

    async def ingest_float(self, float_id: str, data_dir: str = "netcdf_data"):
        """Ingest both metadata and profiles for a float"""
        
        meta_file = os.path.join(data_dir, f"{float_id}_meta.nc")
        prof_file = os.path.join(data_dir, f"{float_id}_prof.nc")
        
        conn = await asyncpg.connect(self.db_url, statement_cache_size=0)
        
        try:
            # Ingest metadata if available
            if os.path.exists(meta_file):
                await self.ingest_metadata(meta_file, conn)
            else:
                logger.warning(f"Metadata file not found: {meta_file}")
            
            # Ingest profiles if available
            if os.path.exists(prof_file):
                await self.ingest_profiles(prof_file, conn)
            else:
                logger.warning(f"Profile file not found: {prof_file}")
                
        finally:
            await conn.close()

    async def ingest_directory(self, directory: str = "netcdf_data"):
        """Ingest all floats in directory"""
        # Find all unique float IDs
        prof_files = glob.glob(os.path.join(directory, "*_prof.nc"))
        float_ids = set()
        
        for f in prof_files:
            basename = os.path.basename(f)
            float_id = basename.replace("_prof.nc", "")
            float_ids.add(float_id)
        
        logger.info(f"Found {len(float_ids)} floats to ingest")
        
        for float_id in sorted(float_ids):
            logger.info(f"\n{'='*60}")
            logger.info(f"Processing Float: {float_id}")
            logger.info(f"{'='*60}")
            await self.ingest_float(float_id, directory)

if __name__ == "__main__":
    async def main():
        ingester = EnhancedArgoIngester(DATABASE_URL)
        await ingester.ingest_directory("netcdf_data")
    
    asyncio.run(main())
