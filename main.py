import asyncio
import os
from datetime import datetime, timedelta
from src.extractors.sf_data_api import SFFireDataExtractor
from src.loaders.postgres_loader import PostgresLoader
from loguru import logger

async def main():
    # Initialize extractor
    extractor = SFFireDataExtractor()
    
    # Initialize loader with connection parameters
    loader = PostgresLoader({
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', '5432')),
        'database': os.getenv('DB_NAME', 'sf_fire_db'),
        'user': os.getenv('DB_USER', 'sf_fire_user'),
        'password': os.getenv('DB_PASS', 'sf_fire_pass'),
        'ssl': 'prefer',
        'command_timeout': 180  # Only keep command_timeout
    }, schema='raw')
    
    try:
        # Extract data from last 30 days
        last_update = datetime.now() - timedelta(days=30)
        logger.info(f"Extracting data since {last_update}")
        
        records = await extractor.extract_incremental(last_update)
        logger.info(f"Extracted {len(records)} records")
        
        # Load data
        await loader.bulk_load(records)
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise
    finally:
        await extractor.close()

if __name__ == "__main__":
    asyncio.run(main())
