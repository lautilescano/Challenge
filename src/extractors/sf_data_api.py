from datetime import datetime
from typing import List, Dict, Optional, Any
import asyncio
import aiohttp
from tenacity import retry, stop_after_attempt, wait_exponential
from loguru import logger
from pydantic import BaseModel, Field

class IncidentRecord(BaseModel):
    """Pydantic model for validating incident records"""
    incident_number: str
    id: str
    incident_date: datetime = Field(alias='incident_date')
    alarm_dttm: Optional[datetime] = None
    arrival_dttm: Optional[datetime] = None
    close_dttm: Optional[datetime] = None
    address: Optional[str] = None
    city: Optional[str] = None
    zipcode: Optional[str] = None
    battalion: Optional[str] = None
    station_area: Optional[str] = None
    supervisor_district: Optional[str] = None
    neighborhood_district: Optional[str] = None
    point: Optional[Dict[str, Any]] = None  
    data_loaded_at: datetime
    
    def dict(self, *args, **kwargs) -> Dict:
        """Convert model to dictionary with string point representation"""
        d = super().dict(*args, **kwargs)
        if self.point:
            # Convert GeoJSON point to string representation
            coords = self.point.get('coordinates', [])
            d['point'] = f"POINT({coords[0]} {coords[1]})" if coords else None
        return d
    
    class Config:
        arbitrary_types_allowed = True

class SFFireDataExtractor:
    def __init__(
        self,
        base_url: str = "https://data.sfgov.org/resource/wr8u-xric.json",
        batch_size: int = 50000
    ):
        self.base_url = base_url
        self.batch_size = batch_size
        self._session = None

    async def _get_session(self):
        if self._session is None:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self):
        if self._session:
            await self._session.close()
            self._session = None

    async def extract_incremental(self, last_update: datetime) -> List[Dict]:
        """Extract records since last update"""
        try:
            session = await self._get_session()
            # Format date for Socrata API
            date_filter = last_update.strftime('%Y-%m-%dT%H:%M:%S')
            
            # Use API's date filter
            params = {
                '$where': f"incident_date >= '{date_filter}'",
                '$limit': self.batch_size,
                '$offset': 0
            }
            
            all_records = []
            
            while True:
                async with session.get(self.base_url, params=params) as response:
                    if response.status != 200:
                        raise Exception(f"API request failed: {response.status}")
                    
                    new_records = await response.json()
                    
                    if not new_records:  # No more records
                        break
                        
                    all_records.extend(new_records)
                    logger.info(f"Fetched {len(new_records)} records. Total: {len(all_records)}")
                    
                    params['$offset'] += self.batch_size
                    
            return all_records
            
        except Exception as e:
            logger.error(f"Error extracting data: {str(e)}")
            raise
