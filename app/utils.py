from datetime import datetime, time, timedelta, date
from fastapi import HTTPException, Request, status
import numpy as np
from typing import List, Tuple, Optional
from WazeRouteCalculator import WazeRouteCalculator
import logging
from zoneinfo import ZoneInfo
from botocore.exceptions import ClientError
import boto3
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
    
def get_secret(secret_name: str):
    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    secret = get_secret_value_response['SecretString']
    return json.loads(secret)

def assign_time_slot(ts: datetime, slots: list[tuple[str,str]]) -> str:
    """
    Given a timezone-aware or naive UTC datetime `ts`, first convert it
    to local Brazil time (America/Sao_Paulo), then pick the correct slot.
    """
    # 1) Ensure ts is timezone-aware in UTC, then convert to local
    if ts.tzinfo is None:
        # assume naive == UTC
        ts = ts.replace(tzinfo=ZoneInfo("UTC"))
    local_ts = ts.astimezone(ZoneInfo("America/Sao_Paulo"))
    # 2) Extract local time and match against your slots
    t = local_ts.time()
    for start_str, end_str in slots:
        start = datetime.strptime(start_str, "%H:%M").time()
        end   = datetime.strptime(end_str,   "%H:%M").time()
        # normal same-day slot
        if start <= t < end:
            return f"{start_str}-{end_str}", local_ts
        # if you ever have overnight slots (end < start), handle here—
        # for now your slots are all same-day so we skip that.
    return "off-hours", local_ts

def assign_rc_wait(local_ts: datetime, slots: list[tuple[str,str,int]]) -> str:
    """
    Given a ts from local Brazil time (America/Sao_Paulo), pick the correct slot.
    """
    # 2) Extract local time and match against your slots
    t = local_ts.time()
    for start_str, end_str, def_wait in slots:
        start = datetime.strptime(start_str, "%H:%M").time()
        end   = datetime.strptime(end_str,   "%H:%M").time()
        # normal same-day slot
        if start <= t < end:
            return def_wait
        # if you ever have overnight slots (end < start), handle here—
        # for now your slots are all same-day so we skip that.
    return 0

def compute_iqr(values: np.ndarray) -> float:
    if len(values) == 0:
        return 0.0
    q75, q25 = np.percentile(values, [75, 25])
    return q75 - q25

def apply_iqr_filter(values: np.ndarray, factor: float = 1.5):
    if len(values) == 0:
        return np.array([])   # not 0.0!
    iqr = compute_iqr(values)
    q1, q3 = np.percentile(values, [25, 75])
    lower = q1 - factor * iqr
    upper = q3 + factor * iqr
    return values[(values >= lower) & (values <= upper)]

def compute_temporal_weights(dates: List[date], reference: date, decay_rate: float) -> np.ndarray:
    """
    For each sample date d in `dates`, compute decay_rate ** business_days_between(d, reference).
    Returns an array of weights aligned with dates.
    """
    w = []
    for d in dates:
        # count Mon–Fri days between d and reference
        # logger.info(f"business days between: {d} and {reference}")
        days = business_days_between(d, reference)
        # logger.info(f"days: {days}")
        if days <= 0:
            w.append(0)
        else:
            w.append(decay_rate ** days)
        # logger.info(f"weights loading: {w}")
    return np.array(w)

def get_adjacent_slots(slots: List[Tuple[str, str]], slot_label: str) -> Tuple[Optional[str], Optional[str]]:
    """Given a slot label, returns (previous_slot, next_slot) labels if exist."""
    slot_labels = [f"{start}-{end}" for start, end in slots]
    idx = slot_labels.index(slot_label) if slot_label in slot_labels else -1
    prev_slot = slot_labels[idx - 1] if idx > 0 else None
    next_slot = slot_labels[idx + 1] if idx >= 0 and idx + 1 < len(slot_labels) else None
    return prev_slot, next_slot

def slot_boundaries(slots: List[Tuple[str, str]], slot_label: str) -> Tuple[time, time]:
    """Returns (start_time, end_time) for the given slot label."""
    for start_str, end_str in slots:
        if slot_label == f"{start_str}-{end_str}":
            start = datetime.strptime(start_str, "%H:%M").time()
            end = datetime.strptime(end_str, "%H:%M").time()
            return start, end
    raise ValueError("Slot label not found")

from dateutil import parser 

def to_date(d):
    if isinstance(d, date) and not isinstance(d, datetime):
        return d
    if isinstance(d, datetime):
        return d.date()
    # handles ISO strings with “Z” or offsets:
    return parser.isoparse(d).date()

def business_days_between(start_date, end_date) -> int:
    """Count Mon–Fri days from start_date to end_date inclusive."""
    start = to_date(start_date)
    end   = to_date(end_date)
    if start > end:
        return 0

    total_days = (end - start).days + 1
    business_days = 0
    for i in range(total_days):
        if (start + timedelta(days=i)).weekday() < 5:
            business_days += 1
    return business_days


def get_route_time(start_lat, start_lng, end_lat, end_lng):
    try:
        start = f"{start_lat},{start_lng}"
        end = f"{end_lat},{end_lng}"
        region = 'EU'  # Use 'EU' for Brazil
        calculator = WazeRouteCalculator(start, end, region)
        route_time, route_distance = calculator.calc_route_info()
        return route_time  # minutes
    except Exception as e:
        # Log error or return a high fallback value
        return None

def weighted_median(data: np.ndarray, weights: np.ndarray) -> float:
    sorter = np.argsort(data)
    data, weights = data[sorter], weights[sorter]
    cum_weights = np.cumsum(weights)
    cutoff = weights.sum() / 2.0
    return data[cum_weights >= cutoff][0]
