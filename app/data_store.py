git clone https://github.com/yael1992/WazeRouteCalculator.git
cd WazeRouteCalculator
pip install .


import pandas as pd
from datetime import datetime, timezone
from typing import Optional, List, Dict
from config import (
    RISK_COLORS, MAX_WAIT_MINUTES, MIN_WAIT_MINUTES, TIME_SLOTS, 
    DYNAMODB_TABLE, AWS_REGION, DEFAULT_WAIT_BY_COLOR
)
from utils import assign_time_slot, compute_iqr, get_secret
import boto3
from boto3.dynamodb.conditions import Key
from decimal import Decimal
import hashlib
from zoneinfo import ZoneInfo
import time
from cachetools import TTLCache
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def hash_pseudonym(pseudonym: str, salt: str) -> str:
    to_hash = f"{salt}{pseudonym}".encode("utf-8")
    return hashlib.sha256(to_hash).hexdigest()


class DataStore:
    def __init__(self):
        self.dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
        self.units_table = self.dynamodb.Table("units")
        self.table = self.dynamodb.Table(DYNAMODB_TABLE)
        self.user_route_table = self.dynamodb.Table("user_route_times")
        self.secret = get_secret("pseudonym/bd")["key_salt"]
        self.est_cache = TTLCache(maxsize=320000, ttl=720)  # 12 minutos de cache

    def ingest_event(self, pseudonym: str, unit: str, event_type: str,
                     risk_color: Optional[str], timestamp: datetime):
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        timestamp_str = timestamp.astimezone(timezone.utc).isoformat().replace('+00:00', 'Z')
        hashed_pseudonym = hash_pseudonym(pseudonym, self.secret)

        response = self.table.query(
            KeyConditionExpression=Key("pseudonym").eq(hashed_pseudonym) & Key("event_id").begins_with(f"{unit}#")
        )
        items = response.get("Items", [])

        local_ts = timestamp.astimezone(ZoneInfo("America/Sao_Paulo"))
        day_str = local_ts.date().isoformat()
        weekday = local_ts.weekday()
        slot, _ = assign_time_slot(timestamp, TIME_SLOTS)

        day_slot_color = f"{day_str}#{slot}#{risk_color}" if risk_color else None
        slot_color = f"{slot}#{risk_color}" if risk_color else None
        color_slot_weekday = f"{risk_color}#{slot}#{weekday}" if risk_color else None

        if event_type == "cinza":
            if items:
                for item in items:
                    self.table.delete_item(
                        Key={"pseudonym": hashed_pseudonym, "event_id": item["event_id"]}
                    )
            item = {
                "pseudonym": hashed_pseudonym,
                "event_id": f"{unit}#{event_type}",
                "unit": unit,
                "cinza_time": timestamp_str,
                "event_time": timestamp_str,
                "event_type": "cinza"
            }
            self.table.put_item(Item=item)
            return None

        elif event_type == "rc":
            cinza_time = None
            for item in items:
                if item["event_type"] == "rc":
                    self.table.delete_item(
                        Key={"pseudonym": hashed_pseudonym, "event_id": item["event_id"]}
                    )
                if item["event_type"] == "cinza":
                    cinza_entry = item
                    cinza_time = datetime.fromisoformat(cinza_entry["cinza_time"])

            if not cinza_time:
                return None

            delta_t = (timestamp - cinza_time).total_seconds() / 60.0

            item = {
                "pseudonym": hashed_pseudonym,
                "event_id": f"{unit}#{event_type}",
                "unit": unit,
                "event_time": timestamp_str,
                "cinza_time": cinza_entry["cinza_time"],
                "rc_time": timestamp_str,
                "risk_color": risk_color,
                "delta_t": Decimal(str(delta_t)),
                "slot": slot,
                "day": day_str,
                "weekday": weekday,
                "day_slot_color": day_slot_color,
                "slot_color": slot_color,
                "color_slot_weekday": color_slot_weekday,
                "event_type": "rc"
            }
            self.table.put_item(Item=item)
            return delta_t

        else:
            return None

    def list_units(self):
        response = self.units_table.scan(ProjectionExpression="unit")
        items = response.get('Items', [])
        units = [item['unit'] for item in items if 'unit' in item]
        return units

    def store_user_route_times(self, user_phone, results):
        ttl_value = int(time.time()) + 48 * 60 * 60
        with self.user_route_table.batch_writer() as batch:
            for r in results:
                batch.put_item(Item={
                    "user_phone": user_phone,
                    "unit": r["unit"],
                    "travel_time_min": Decimal(str(r["travel_time_min"])) if r["travel_time_min"] is not None else None,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "ttl": ttl_value
                })

    def register_unit(self, unit: str, address: Optional[str] = None,
                      postal_code: Optional[str] = None,
                      latitude: Optional[float] = None,
                      longitude: Optional[float] = None) -> Dict:
        item = {"unit": unit}
        if latitude is not None and longitude is not None:
            item.update({"lat": latitude, "lng": longitude})
        if address:
            item["address"] = address
        if postal_code:
            item["postal_code"] = postal_code
        self.units_table.put_item(Item=item)
        return item

    def get_all_units_with_locations(self) -> List[Dict]:
        response = self.units_table.scan()
        return response.get("Items", [])

    def get_user_route_times(self, user_phone: str) -> List[Dict]:
        resp = self.user_route_table.query(
            KeyConditionExpression=Key("user_phone").eq(user_phone)
        )
        return resp.get("Items", [])

    # ========== Consultas Otimizadas com Índices ==========

    def fetch_samples_unit_day_slot_color_df(self, unit: str, color: str,
                                             slot: str, day_str: str) -> pd.DataFrame:
        key = ("unit_day_slot_color", unit, color, slot, day_str)
        if key in self.est_cache:
            return self.est_cache[key]

        resp = self.table.query(
            IndexName="UnitDaySlotColorIndex",
            KeyConditionExpression=Key('unit').eq(unit) & Key('day_slot_color').eq(f"{day_str}#{slot}#{color}")
        )
        items = resp.get('Items', [])
        df = pd.DataFrame(items) if items else pd.DataFrame(columns=['delta_t', 'day'])
        if 'delta_t' in df:
            df['delta_t'] = df['delta_t'].astype(float)

        self.est_cache[key] = df
        return df[['delta_t', 'day']]

    def fetch_samples_unit_slot_color_all_days_df(self, unit: str, color: str, slot: str) -> pd.DataFrame:
        key = ("unit_slot_color_all_days", unit, color, slot)
        if key in self.est_cache:
            return self.est_cache[key]

        resp = self.table.query(
            IndexName="UnitSlotColorIndex",
            KeyConditionExpression=Key('unit').eq(unit) & Key('slot_color').eq(f"{slot}#{color}")
        )
        items = resp.get('Items', [])
        df = pd.DataFrame(items) if items else pd.DataFrame(columns=['delta_t', 'day'])
        if 'delta_t' in df:
            df['delta_t'] = df['delta_t'].astype(float)

        self.est_cache[key] = df
        return df[['delta_t', 'day']]

    def fetch_samples_unit_color_slot_weekday_df(self, unit: str, color: str, slot: str, weekday: int) -> pd.DataFrame:
        key = ("unit_color_slot_weekday", unit, color, slot, weekday)
        if key in self.est_cache:
            return self.est_cache[key]

        resp = self.table.query(
            IndexName="UnitSlotColorWeekdayIndex",
            KeyConditionExpression=Key('unit').eq(unit) & Key('color_slot_weekday').eq(f"{color}#{slot}#{weekday}")
        )
        items = resp.get('Items', [])
        df = pd.DataFrame(items) if items else pd.DataFrame(columns=['delta_t', 'day'])
        if 'delta_t' in df:
            df['delta_t'] = df['delta_t'].astype(float)

        self.est_cache[key] = df
        return df[['delta_t', 'day']]

    def fetch_samples_color_slot_all_units_df(self, color: str, slot: str) -> pd.DataFrame:
        key = ("color_slot_all_units", color, slot)
        if key in self.est_cache:
            return self.est_cache[key]

        resp = self.table.query(
            IndexName="ColorSlotIndex",
            KeyConditionExpression=Key('risk_color').eq(color) & Key('slot').eq(slot)
        )
        items = resp.get('Items', [])
        df = pd.DataFrame(items) if items else pd.DataFrame(columns=['delta_t'])
        if 'delta_t' in df:
            df['delta_t'] = df['delta_t'].astype(float)

        self.est_cache[key] = df
        return df[['delta_t']]
