import os

RISK_COLORS = ['b', 'g', 'y', 'o', 'r']

# Default time slot definitions
TIME_SLOTS = [
    ("05:00", "08:00"),
    ("08:00", "11:30"),
    ("11:30", "15:00"),
    ("15:00", "18:00"),
    ("18:00", "21:00")
]

CONCEPT1_MIN_SAMPLES = 1
CONCEPT3_MIN_SAMPLES = 5

RC_TIME_SLOTS = [
    ("05:00", "07:00", 15),
    ("07:00", "11:00", 10),
    ("11:00", "14:00", 30),
    ("14:00", "16:00", 40),
    ("16:00", "18:00", 45),
    ("18:00", "21:00", 60)
]

DEFAULT_RC_ROOM_WAIT = {
    "05:00-07:00": 15,
    "07:00-11:00": 10,
    "11:00-14:00": 30,
    "14:00-16:00": 40,
    "16:00-18:00": 45,
    "18:00-21:00": 60
}

DEFAULT_WAIT_BY_SLOT_COLOR = {
    "05:00-08:00": {'b': 80, 'g': 65, 'y': 50, 'o': 35, 'r': 2},
    "08:00-11:30": {'b':100, 'g': 85, 'y': 70, 'o': 55, 'r': 2},
    "11:30-15:00": {'b': 90, 'g': 75, 'y': 60, 'o': 45, 'r': 2},
    "15:00-18:00": {'b': 95, 'g': 80, 'y': 65, 'o': 50, 'r': 2},
    "18:00-21:00": {'b': 85, 'g': 70, 'y': 55, 'o': 40, 'r': 2}
}

# Minutes for boundary blending
SLOT_BOUNDARY_SMOOTHING_WINDOW_MIN = 75

# Outlier thresholds (IQR method)
IQR_OUTLIER_FACTOR = 2.0

# Default wait times by color (minutes)
DEFAULT_WAIT_BY_COLOR = {
    'b': 60,  # blue
    'g': 40,  # green
    'y': 30,  # yellow
    'o': 15,  # orange
    'r': 5    # red
}

# Maximum valid delta_t (minutes)
MAX_WAIT_MINUTES = 360
MIN_WAIT_MINUTES = 5

DYNAMODB_TABLE = os.getenv("DYNAMODB_TABLE", "wait_time_events")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
CEP_ABERTO_TOKEN = os.getenv("CEP_ABERTO_TOKEN")
TEMPORAL_DECAY_RATE = 0.8