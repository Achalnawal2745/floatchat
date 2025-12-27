#!/usr/bin/env python3
"""Verify NetCDF ingestion by querying the database directly"""

import asyncio
import asyncpg
import os
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

async def verify_ingestion():
    """Check if float 1900121 data was properly ingested"""
    
    conn = await asyncpg.connect(DATABASE_URL, statement_cache_size=0)
    
    try:
        print("=" * 60)
        print("VERIFICATION: Float 1900121 Ingestion")
        print("=" * 60)
        
        # 1. Check float metadata
        print("\n1. FLOAT METADATA:")
        metadata = await conn.fetchrow("""
            SELECT platform_number, pi_name, project_name, 
                   operating_institute, created_at
            FROM float_metadata 
            WHERE platform_number = 1900121
        """)
        
        if metadata:
            print(f"   Platform Number: {metadata['platform_number']}")
            print(f"   PI Name: {metadata['pi_name']}")
            print(f"   Project: {metadata['project_name']}")
            print(f"   Institute: {metadata['operating_institute']}")
            print(f"   Created: {metadata['created_at']}")
        else:
            print("   ❌ NO METADATA FOUND!")
            return
        
        # 2. Check profiles
        print("\n2. PROFILES:")
        profiles = await conn.fetch("""
            SELECT cycle_number, profile_date, latitude, longitude
            FROM profiles 
            WHERE float_id = 1900121
            ORDER BY cycle_number
        """)
        
        print(f"   Total profiles: {len(profiles)}")
        for p in profiles:
            print(f"   - Cycle {p['cycle_number']}: {p['profile_date']} at ({p['latitude']}, {p['longitude']})")
        
        # 3. Check measurements
        print("\n3. MEASUREMENTS:")
        measurement_count = await conn.fetchval("""
            SELECT COUNT(*) 
            FROM measurements 
            WHERE float_id = 1900121
        """)
        print(f"   Total measurements: {measurement_count}")
        
        # Sample measurements
        samples = await conn.fetch("""
            SELECT cycle_number, n_level, pressure, depth_m, 
                   temperature, salinity
            FROM measurements 
            WHERE float_id = 1900121
            ORDER BY cycle_number, n_level
            LIMIT 5
        """)
        
        print("\n   Sample measurements (first 5):")
        print("   Cycle | Level | Pressure | Depth | Temp | Salinity")
        print("   " + "-" * 55)
        for m in samples:
            temp = f"{m['temperature']:.2f}" if m['temperature'] else "NULL"
            sal = f"{m['salinity']:.2f}" if m['salinity'] else "NULL"
            print(f"   {m['cycle_number']:5} | {m['n_level']:5} | {m['pressure']:8.2f} | {m['depth_m']:5.2f} | {temp:6} | {sal:6}")
        
        # 4. Check data completeness
        print("\n4. DATA COMPLETENESS:")
        stats = await conn.fetchrow("""
            SELECT 
                COUNT(*) as total,
                COUNT(temperature) as has_temp,
                COUNT(salinity) as has_sal,
                COUNT(pressure) as has_pres,
                MIN(pressure) as min_pres,
                MAX(pressure) as max_pres
            FROM measurements 
            WHERE float_id = 1900121
        """)
        
        print(f"   Total records: {stats['total']}")
        print(f"   With temperature: {stats['has_temp']} ({stats['has_temp']/stats['total']*100:.1f}%)")
        print(f"   With salinity: {stats['has_sal']} ({stats['has_sal']/stats['total']*100:.1f}%)")
        print(f"   With pressure: {stats['has_pres']} ({stats['has_pres']/stats['total']*100:.1f}%)")
        print(f"   Pressure range: {stats['min_pres']:.2f} - {stats['max_pres']:.2f} dbar")
        
        print("\n" + "=" * 60)
        print("✓ VERIFICATION COMPLETE")
        print("=" * 60)
        
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(verify_ingestion())
