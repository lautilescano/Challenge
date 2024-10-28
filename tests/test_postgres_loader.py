import pytest
import asyncio
from datetime import datetime
from src.loaders.postgres_loader import PostgresLoader

@pytest.mark.asyncio
async def test_bulk_load():
    connection_params = {
        "host": "localhost",
        "database": "test_db",
        "user": "test_user",
        "password": "test_pass"
    }
    
    loader = PostgresLoader(connection_params)
    
    test_records = [{
        "incident_number": "test123",
        "id": "unique123",
        "incident_date": datetime.now(),
        "alarm_dttm": datetime.now(),
        "arrival_dttm": datetime.now(),
        "close_dttm": datetime.now(),
        "district": "D1",
        "battalion": "B1",
        "data_loaded_at": datetime.now()
    }]
    
    await loader.bulk_load(test_records)


@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for each test case."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()
