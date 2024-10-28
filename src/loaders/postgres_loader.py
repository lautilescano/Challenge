from typing import List, Dict
import asyncpg
from loguru import logger
from datetime import datetime
from pydantic import BaseModel
from tenacity import retry, stop_after_attempt, wait_exponential  # Changed from hasty to tenacity
from .models import FireIncident  # We'll create this in a separate file

class PostgresLoader:
    def __init__(
        self,
        connection_params: Dict[str, str],
        schema: str = "raw"
    ):
        # Remove schema from connection params but keep it in the class
        self.schema = schema
        # Create a copy of connection params without schema
        self.connection_params = {k: v for k, v in connection_params.items() 
                                if k not in ['schema']}
        
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    async def _init_pool(self) -> asyncpg.Pool:
        """Initialize connection pool with retry logic"""
        try:
            # First create the pool
            pool = await asyncpg.create_pool(**self.connection_params)
            
            # Then configure the connection
            async with pool.acquire() as conn:
                # First ensure schema exists
                await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema}")
                logger.info(f"Ensured schema {self.schema} exists")
                
                # Then set the search path
                await conn.execute(f"SET search_path TO {self.schema}, public")
                logger.info("Set search path")
                
                # Finally set the statement timeout
                await conn.execute('SET statement_timeout = 180000')
                logger.info("Set statement timeout")
            
            return pool
            
        except Exception as e:
            logger.error(f"Failed to connect to database: {str(e)}")
            logger.info(f"Connection params (without password): {dict(filter(lambda x: x[0] != 'password', self.connection_params.items()))}")
            raise
            
    async def _create_schema_if_not_exists(self, pool: asyncpg.Pool):
        """Ensure schema exists"""
        async with pool.acquire() as conn:
            await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema}")
            
    async def _create_target_table(self, pool: asyncpg.Pool, table: str):
        """Create target table without indexes"""
        async with pool.acquire() as conn:
            try:
                logger.info("Starting table creation process")
                await conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {self.schema}.{table} (
                        incident_number TEXT NOT NULL,
                        id TEXT PRIMARY KEY,
                        incident_date TIMESTAMP,
                        alarm_dttm TIMESTAMP,
                        arrival_dttm TIMESTAMP,
                        close_dttm TIMESTAMP,
                        address TEXT,
                        city TEXT,
                        zipcode TEXT,
                        battalion TEXT,
                        station_area TEXT,
                        supervisor_district TEXT,
                        neighborhood_district TEXT,
                        point TEXT,
                        data_loaded_at TIMESTAMP NOT NULL,
                        _loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                logger.info("Base table created")
            except Exception as e:
                logger.error(f"Error in table creation step: {str(e)}")
                raise
                
    async def _create_staging_table(self, pool: asyncpg.Pool):
        """Create staging table for bulk loads"""
        async with pool.acquire() as conn:
            await conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.schema}.fire_incidents_staging (
                    LIKE {self.schema}.fire_incidents INCLUDING ALL
                )
            """)
            
    async def bulk_load(self, records: List[Dict], table: str = "fire_incidents") -> None:
        """Bulk load data into PostgreSQL"""
        if not records:
            logger.warning("No records to load")
            return
        
        total_records = len(records)
        logger.info(f"Starting bulk load of {total_records} records")
        
        pool = await self._init_pool()
        batch_size = 10000  # Process in smaller batches
        
        try:
            # Create table first
            await self._create_target_table(pool, table)
            logger.info("Table structure verified")
            
            # Load data in batches
            for i in range(0, total_records, batch_size):
                batch = records[i:i + batch_size]
                batch_number = (i // batch_size) + 1
                total_batches = (total_records + batch_size - 1) // batch_size
                
                logger.info(f"Processing batch {batch_number}/{total_batches} ({len(batch)} records)")
                
                # Validate records
                validated_records = [FireIncident(**record).dict() for record in batch]
                
                async with pool.acquire() as conn:
                    records_tuple = [
                        (r['incident_number'], r['id'], r['incident_date'], r['alarm_dttm'],
                         r['arrival_dttm'], r['close_dttm'], r['address'], r['city'],
                         r['zipcode'], r['battalion'], r['station_area'],
                         r['supervisor_district'], r['neighborhood_district'],
                         r['point'], r['data_loaded_at'])
                        for r in validated_records
                    ]
                    
                    logger.info(f"Inserting batch {batch_number}")
                    await conn.executemany(f"""
                        INSERT INTO {self.schema}.{table} (
                            incident_number, id, incident_date, alarm_dttm,
                            arrival_dttm, close_dttm, address, city, zipcode,
                            battalion, station_area, supervisor_district,
                            neighborhood_district, point, data_loaded_at
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
                        ON CONFLICT (id) DO UPDATE SET
                            data_loaded_at = EXCLUDED.data_loaded_at,
                            _loaded_at = CURRENT_TIMESTAMP
                    """, records_tuple)
                    
                logger.success(f"Completed batch {batch_number}/{total_batches}")
                
            logger.success(f"Successfully loaded all {total_records} records into {self.schema}.{table}")
            
            # Create indexes after data is loaded
            logger.info("Creating indexes...")
            await self._create_indexes(pool, table)
            logger.info("Indexes created successfully")
            
        except Exception as e:
            logger.error(f"Error loading data: {str(e)}")
            raise
        finally:
            await pool.close()

    async def _create_indexes(self, pool: asyncpg.Pool, table: str):
        """Create indexes in background"""
        async with pool.acquire() as conn:
            # Create indexes concurrently to avoid blocking
            await conn.execute(f"""
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_{table}_battalion 
                ON {self.schema}.{table}(battalion)
            """)
            await conn.execute(f"""
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_{table}_incident_date 
                ON {self.schema}.{table}(incident_date)
            """)
