#!/usr/bin/env python3
"""Quick check of metadata for floats 1902669 & 1902670"""

import asyncio
import asyncpg
import os
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

async def check():
    conn = await asyncpg.connect(DATABASE_URL, statement_cache_size=0)
    
    try:
        for float_id in [1902669, 1902670]:
            row = await conn.fetchrow("""
                SELECT platform_number, float_serial_number, launch_date, 
                       start_date, end_of_life, launch_latitude, launch_longitude
                FROM float_metadata WHERE platform_number = $1
            """, float_id)
            
            print(f"\nFloat {float_id}:")
            if row:
                for k, v in dict(row).items():
                    print(f"  {k:25} = {v}")
            else:
                print("  NOT FOUND!")
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(check())
