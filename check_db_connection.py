#!/usr/bin/env python3
"""Check which database we're actually connected to"""

import asyncio
import asyncpg
import os
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

async def check_connection():
    """Verify database connection details"""
    
    print("=" * 60)
    print("DATABASE CONNECTION CHECK")
    print("=" * 60)
    
    # Parse connection string (hide password)
    if DATABASE_URL:
        parts = DATABASE_URL.replace("postgresql://", "").split("@")
        if len(parts) == 2:
            user_part = parts[0].split(":")[0]
            host_part = parts[1].split("/")[0]
            db_part = parts[1].split("/")[1].split("?")[0] if "/" in parts[1] else "unknown"
            
            print(f"\nConnection String Info:")
            print(f"  User: {user_part}")
            print(f"  Host: {host_part}")
            print(f"  Database: {db_part}")
    
    conn = await asyncpg.connect(DATABASE_URL, statement_cache_size=0)
    
    try:
        # Get database info
        db_info = await conn.fetchrow("""
            SELECT 
                current_database() as database,
                current_user as user,
                version() as version
        """)
        
        print(f"\nActual Connection:")
        print(f"  Database: {db_info['database']}")
        print(f"  User: {db_info['user']}")
        print(f"  Version: {db_info['version'][:50]}...")
        
        # Check table counts
        print(f"\nTable Row Counts:")
        
        tables = ['float_metadata', 'profiles', 'measurements']
        for table in tables:
            count = await conn.fetchval(f"SELECT COUNT(*) FROM {table}")
            print(f"  {table}: {count} rows")
        
        # Check if float 1900121 exists
        print(f"\nFloat 1900121 Check:")
        exists = await conn.fetchval("""
            SELECT EXISTS(
                SELECT 1 FROM float_metadata WHERE platform_number = 1900121
            )
        """)
        print(f"  Exists in float_metadata: {exists}")
        
        if exists:
            measurements = await conn.fetchval("""
                SELECT COUNT(*) FROM measurements WHERE float_id = 1900121
            """)
            print(f"  Measurements count: {measurements}")
        
        print("\n" + "=" * 60)
        
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(check_connection())
