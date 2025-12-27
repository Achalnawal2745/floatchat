#!/usr/bin/env python3
"""Check what metadata was actually ingested"""

import asyncio
import asyncpg
import os
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

async def check_metadata():
    conn = await asyncpg.connect(DATABASE_URL, statement_cache_size=0)
    
    try:
        print("="*60)
        print("CHECKING METADATA FOR FLOATS 1902669 & 1902670")
        print("="*60)
        
        for float_id in [1902669, 1902670]:
            print(f"\nFloat {float_id}:")
            
            row = await conn.fetchrow("""
                SELECT 
                    platform_number,
                    float_serial_number,
                    launch_date,
                    start_date,
                    end_of_life,
                    launch_latitude,
                    launch_longitude,
                    pi_name,
                    project_name,
                    firmware_version,
                    deployment_platform,
                    float_owner,
                    operating_institute
                FROM float_metadata
                WHERE platform_number = $1
            """, float_id)
            
            if row:
                for key, val in dict(row).items():
                    status = "✓" if val is not None else "✗ NULL"
                    print(f"  {key:25} {status:10} {val}")
            else:
                print(f"  NOT FOUND IN DATABASE!")
        
        print("\n" + "="*60)
        
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(check_metadata())
