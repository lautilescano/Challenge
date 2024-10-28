import asyncio
import asyncpg
from loguru import logger

async def check_db_connection(connection_params: dict) -> bool:
    """Test database connection"""
    try:
        conn = await asyncpg.connect(**connection_params)
        await conn.close()
        logger.success("Successfully connected to database")
        return True
    except Exception as e:
        logger.error(f"Failed to connect to database: {str(e)}")
        return False

if __name__ == "__main__":
    # Use the same connection params as main.py
    params = {
        'host': 'localhost',
        'port': 5432,
        'database': 'sf_fire_db',
        'user': 'sf_fire_user',
        'password': 'sf_fire_pass',
        'ssl': 'prefer'
    }
    asyncio.run(check_db_connection(params))

