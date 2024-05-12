from pydantic import BaseModel
from typing import Optional
class Sensor(BaseModel):
    id: int
    name: str
    latitude: float
    longitude: float
    joined_at: str
    last_seen: str
    type: str
    mac_address: str
    battery_level: float
    temperature: Optional[float]
    humidity: Optional[float]
    velocity: Optional[float]
    description: str
    
    
    class Config:
        orm_mode = True
        
class SensorCreate(BaseModel):
    name: str
    longitude: float
    latitude: float
    type: str
    mac_address: str
    manufacturer: str
    model: str
    serie_number: str
    firmware_version: str
    description: str

class SensorData(BaseModel):
    temperature: Optional[float]
    humidity: Optional[float]
    velocity: Optional[float]
    battery_level: float
    last_seen: str