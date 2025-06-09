from fastapi import FastAPI, Query, Depends, HTTPException, status, Request
from schema import (
    AnnotateEventRequest, EstimateRequest, EstimateResponse, HealthCheckResponse, 
    AllEstimatesResponse, UnitEstimates, RegisterUnitRequest, RegisterUnitResponse,
    RouteTimeRequest, RouteTimeResponse, RouteTimeResult
)
from data_store import DataStore
from models import WaitTimeEstimator, AdminConfig
from datetime import datetime, timezone
from utils import get_route_time
import requests
import httpx
from config import CEP_ABERTO_TOKEN

app = FastAPI()
datastore = DataStore()
estimator = WaitTimeEstimator(datastore)

from fastapi.middleware.cors import CORSMiddleware
import logging
from decimal import Decimal

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://ALERT",
        "https://ALERT",
    ],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health", response_model=HealthCheckResponse)
def health():
    return HealthCheckResponse(status="ok")

@app.post("/register_unit", response_model=RegisterUnitResponse)
def register_unit(req: RegisterUnitRequest):
    # For simplicity, skip geocoding if no lat/lng given
    item = datastore.register_unit(
        unit=req.unit,
        address=req.address,
        postal_code=req.postal_code,
        latitude=Decimal(str(req.latitude)),
        longitude=Decimal(str(req.longitude))
    )
    return RegisterUnitResponse(
        success=True,
        unit=req.unit,
        lat=item.get("lat"),
        lng=item.get("lng"),
        message="Unit registered"
    )

@app.post("/annotate")
def annotate_event(event: AnnotateEventRequest):
    dt = datastore.ingest_event(
        pseudonym=event.pseudonym,
        unit=event.unit,
        event_type=event.event_type,
        risk_color=event.risk_color,
        timestamp=event.timestamp
    )
    return {"message": "Event processed.", "delta_t": dt}

@app.post("/estimate", response_model=EstimateResponse)
def estimate_wait_time(req: EstimateRequest):
    est = estimator.estimate_wait_time(
        unit=req.unit,
        risk_color=req.risk_color,
        query_time=req.query_time
    )
    return EstimateResponse(
        estimated_wait=est
    )

@app.get("/all_estimates", response_model=AllEstimatesResponse)
def all_estimates(query_time: datetime = Query(...)):
    units = datastore.list_units()
    estimates = []
    for unit in units:
        # logger.info(f"\nunit {unit}, blue")
        # blue_est = estimator.estimate_wait_time(unit, 'b', query_time)
        blue_est = 0
        # logger.info(f"\nunit {unit}, green")
        green_est = estimator.estimate_wait_time(unit, 'g', query_time)
        # logger.info(f"\nunit {unit}, yellow")
        # yellow_est = estimator.estimate_wait_time(unit, 'y', query_time)
        yellow_est = 0
        # logger.info(f"\nunit {unit}, orange")
        # orange_est = estimator.estimate_wait_time(unit, 'o', query_time)
        orange_est = 0
        # logger.info(f"\nunit {unit}, red")
        # red_est = estimator.estimate_wait_time(unit, 'r', query_time)
        red_est = 0
        estimates.append(
            UnitEstimates(
                unit=unit,
                blue=blue_est,
                green=green_est,
                yellow=yellow_est,
                orange=orange_est,
                red=red_est
            )
        )
    estimates.sort(key=lambda x: x.green)
    return AllEstimatesResponse(estimates=estimates, query_time=query_time)

@app.post("/route_times")
def route_times(req: RouteTimeRequest):
    units = datastore.get_all_units_with_locations()
    results = []
    for unit_info in units:
        print(f"NO DUPLICATE -- UNIT NAME: {unit_info.get('unit')}")
        lat, lng = unit_info.get("lat"), unit_info.get("lng")
        if lat is None or lng is None:
            continue
        travel_time = get_route_time(
            req.latitude,
            req.longitude,
            lat,
            lng
        )
        print(f"travel_time: {travel_time}")
        results.append(
            {
                "unit": unit_info["unit"],
                "travel_time_min": travel_time
            }
        )
    # Store or overwrite for user
    datastore.store_user_route_times(req.user_phone, results)
    return {"message": "Route times stored."}

@app.get("/route_times/{user_phone}", response_model=RouteTimeResponse)
def get_user_route_times(user_phone: str):
    items = datastore.get_user_route_times(user_phone)
    results = [
        RouteTimeResult(
            unit=item["unit"],
            travel_time_min=item["travel_time_min"],
            timestamp=item["timestamp"]
        )
        for item in items
    ]
    # You may also want to include last used location/timestamp, etc.
    return RouteTimeResponse(
        user_phone=user_phone,
        results=results
    )

@app.get("/units")
def list_units():
    items = datastore.get_all_units_with_locations()
    return {"units": [{"unit": i["unit"]} for i in items if "unit" in i]}

@app.get("/cep_lookup")
def cep_lookup(cep: str):
    url = f"https://www.cepaberto.com/api/v3/cep?cep={cep}"
    headers = {"Authorization": f"Token token={CEP_ABERTO_TOKEN}"}
    resp = requests.get(url, headers=headers)
    return resp.json() 