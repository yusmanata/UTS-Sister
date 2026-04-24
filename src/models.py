from pydantic import BaseModel, ConfigDict
from typing import Dict, Any, List
from datetime import datetime

class EventModel(BaseModel):
    model_config = ConfigDict(extra='ignore')
    
    topic: str
    event_id: str
    timestamp: datetime
    source: str
    payload: Dict[str, Any]

class StatsResponse(BaseModel):
    received: int
    unique_processed: int
    duplicate_dropped: int
    topics: List[str]
    uptime_seconds: float
