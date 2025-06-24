import uuid
import random
from datetime import datetime, timedelta
from decimal import Decimal
import boto3

# -------- CONFIGURATION --------
TABLE_NAME = "wait_time_events"
REGION_NAME = "us-east-1"

NUM_PATIENTS = 1000               # how many cinza events (patients) to generate
RC_PROBABILITY = 0.7              # fraction of patients who also get an RC event
START_DATE = datetime(2025, 6, 16) # earliest cinza_time
END_DATE   = datetime(2025, 6, 20) # latest cinza_time

UNITS = [
    "CIAMS Urias Magalhães",
    "UPA Campinas",
    "Cais Finsocial",
    "UPA Região Noroeste",
    "CAIS Cândida de Morais"
]

SLOTS = ["05:00-08:00", "08:00-11:00", "11:00-14:00", "14:00-17:00", "17:00-20:00"]
RISK_COLORS = ["b", "g", "y", "o", "r"]  # blue, green, yellow, orange, red
# --------------------------------

def random_datetime(start, end):
    """Return a random datetime between two datetimes."""
    delta = end - start
    seconds = random.uniform(0, delta.total_seconds())
    return start + timedelta(seconds=seconds)

def make_cinza_item(pseudonym, unit, cinza_ts):
    day_str = cinza_ts.date().isoformat()
    return {
        "pseudonym": pseudonym,
        "event_id": f"{unit}#cinza",
        "cinza_time": cinza_ts.isoformat() + "Z",
        "day": day_str,
        "delta_t": Decimal("0.0"),
        "event_time": cinza_ts.isoformat() + "Z",
        "event_type": "cinza",
        "unit": unit,
    }

def make_rc_item(pseudonym, unit, cinza_ts, rc_ts):
    day_str = cinza_ts.date().isoformat()
    # compute total seconds as an integer, then convert to Decimal minutes
    total_secs = int((rc_ts - cinza_ts).total_seconds())
    delta_minutes = Decimal(total_secs) / Decimal(60)
    slot = SLOTS[(rc_ts.hour // 3) % len(SLOTS)]
    risk_color = random.choice(RISK_COLORS)
    return {
        "pseudonym": pseudonym,
        "event_id": f"{unit}#rc",
        "cinza_time": cinza_ts.isoformat() + "Z",
        "day": day_str,
        "delta_t": delta_minutes,
        "rc_time": rc_ts.isoformat() + "Z",
        "event_time": rc_ts.isoformat() + "Z",
        "event_type": "rc",
        "risk_color": risk_color,
        "slot": slot,
        "unit": unit,
        "unit_slot_color": f"{unit}#{slot}#{risk_color}",
        "color_slot": f"{risk_color}#{slot}"
    }

def main():
    # initialize DynamoDB table resource
    dynamodb = boto3.resource("dynamodb", region_name=REGION_NAME)
    table = dynamodb.Table(TABLE_NAME)

    with table.batch_writer() as batch:
        for _ in range(NUM_PATIENTS):
            # generate a single pseudonym for this patient
            pseudonym = uuid.uuid4().hex

            # 1) cinza event
            unit = random.choice(UNITS)
            cinza_ts = random_datetime(START_DATE, END_DATE)
            cinza_item = make_cinza_item(pseudonym, unit, cinza_ts)
            batch.put_item(Item=cinza_item)

            # 2) optionally rc event
            if random.random() < RC_PROBABILITY:
                rc_delay = timedelta(minutes=random.uniform(5, 120))
                rc_ts = cinza_ts + rc_delay
                rc_item = make_rc_item(pseudonym, unit, cinza_ts, rc_ts)
                batch.put_item(Item=rc_item)

    print(f"Inserted ~{NUM_PATIENTS} cinza events and "
        f"~{int(NUM_PATIENTS * RC_PROBABILITY)} rc events into “{TABLE_NAME}”.")

if __name__ == "__main__":
    main()