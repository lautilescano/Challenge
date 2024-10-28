from pydantic import BaseModel
from typing import List, Dict
from datetime import datetime

class Point(BaseModel):
    type: str
    coordinates: List[float]

class FireIncident(BaseModel):
    incident_number: str
    id: str
    incident_date: datetime | None = None
    alarm_dttm: datetime | None = None
    arrival_dttm: datetime | None = None
    close_dttm: datetime | None = None
    address: str | None = None
    city: str | None = None
    zipcode: str | None = None
    battalion: str | None = None
    station_area: str | None = None
    supervisor_district: str | None = None
    neighborhood_district: str | None = None
    point: Point | Dict | None = None  
    data_loaded_at: datetime

    def dict(self, *args, **kwargs):
        # Convert to dict and handle point serialization
        d = super().dict(*args, **kwargs)
        if d.get('point'):
            d['point'] = str(d['point'])  # Convert point to string representation
        return d
