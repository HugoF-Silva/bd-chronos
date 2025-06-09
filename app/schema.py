from pydantic import BaseModel
from typing import Optional, Literal, List
from datetime import datetime
from decimal import Decimal
from typing import Union
class AnnotateEventRequest(BaseModel):
    pseudonym: str
    unit: str
    event_type: Literal["cinza", "rc"]
    risk_color: Optional[str] = None  # Only required for RC event
    timestamp: datetime

class EstimateRequest(BaseModel):
    unit: str
    risk_color: str
    query_time: datetime

class EstimateResponse(BaseModel):
    estimated_wait: float | str
    
class HealthCheckResponse(BaseModel):
    status: str

class UnitEstimates(BaseModel):
    unit: str
    blue: float
    green: float
    yellow: float
    orange: float
    red: float

class AllEstimatesResponse(BaseModel):
    estimates: List[UnitEstimates]
    query_time: datetime

class RegisterUnitRequest(BaseModel):
    unit: str
    address: Optional[str] = None
    postal_code: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None

class RegisterUnitResponse(BaseModel):
    success: bool
    unit: str
    lat: Optional[float]
    lng: Optional[float]
    message: Optional[str] = None

class RouteTimeRequest(BaseModel):
    user_phone: str
    latitude: float
    longitude: float

class RouteTimeResult(BaseModel):
    unit: str
    travel_time_min: Decimal
    timestamp: Optional[str]

class RouteTimeResponse(BaseModel):
    user_phone: str
    results: List[RouteTimeResult]