# models.py

from datetime import datetime, timedelta
import numpy as np
from typing import Union
import json
from config import (
    TIME_SLOTS,
    MIN_WAIT_MINUTES,
    MAX_WAIT_MINUTES,
    DEFAULT_WAIT_BY_SLOT_COLOR,
    SLOT_BOUNDARY_SMOOTHING_WINDOW_MIN,
    CONCEPT1_MIN_SAMPLES,
    CONCEPT3_MIN_SAMPLES,
    TEMPORAL_DECAY_RATE,
    IQR_OUTLIER_FACTOR,
    RC_TIME_SLOTS
)
from utils import (
    assign_time_slot,
    apply_iqr_filter,
    get_adjacent_slots,
    slot_boundaries,
    compute_temporal_weights, 
    weighted_median,
    assign_rc_wait,
    get_secret
)
from data_store import DataStore
import logging
from zoneinfo import ZoneInfo
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WaitTimeEstimator:
    def __init__(self, datastore: DataStore):
        self.ds = datastore

    def estimate_wait_time(self, unit: str, color: str, query_time: datetime) -> Union[float, str]:
        # 1) Figure out which slot we’re in
        slot, query_time_sp = assign_time_slot(query_time, TIME_SLOTS)

        # 2) Fetch that slot’s start/end as datetimes
        try:
            slot_start_t, slot_end_t = slot_boundaries(TIME_SLOTS, slot)
        except ValueError:
            return "off-hours"

        slot_start_dt = query_time_sp.replace(
            hour=slot_start_t.hour, minute=slot_start_t.minute,
            second=0, microsecond=0
        )

        slot_end_dt = query_time_sp.replace(
            hour=slot_end_t.hour, minute=slot_end_t.minute,
            second=0, microsecond=0
        )

        # handle overnight
        if slot_end_t < slot_start_t:
            slot_end_dt += timedelta(days=1)

        # 3) How close are we to the start or end boundary?
        delta_to_start = (query_time_sp - slot_start_dt).total_seconds() / 60.0
        delta_to_end   = (slot_end_dt - query_time_sp).total_seconds()  / 60.0

        # 4) If we’re within the smoothing window at the **start** of the slot, blend
        if 0 <= delta_to_start < SLOT_BOUNDARY_SMOOTHING_WINDOW_MIN:
            prev_slot, _ = get_adjacent_slots(TIME_SLOTS, slot)
            if prev_slot:
                est_here = self._estimate_for_slot(unit, color, query_time_sp, slot)
                est_prev = self._estimate_for_slot(unit, color, query_time_sp, prev_slot)
                w = delta_to_start / SLOT_BOUNDARY_SMOOTHING_WINDOW_MIN
                blended = (1 - w) * est_here + w * est_prev
                return self._clip(blended)

        # 5) Likewise at the **end** boundary
        if 0 <= delta_to_end < SLOT_BOUNDARY_SMOOTHING_WINDOW_MIN:
            _, next_slot = get_adjacent_slots(TIME_SLOTS, slot)
            if next_slot:
                est_here = self._estimate_for_slot(unit, color, query_time_sp, slot)
                est_next = self._estimate_for_slot(unit, color, query_time_sp, next_slot)
                w = delta_to_end / SLOT_BOUNDARY_SMOOTHING_WINDOW_MIN
                blended = (1 - w) * est_here + w * est_next
                return self._clip(blended)

        # 6) Otherwise, just use the slot‐based estimate
        return self._estimate_for_slot(unit, color, query_time_sp, slot)

    def _estimate_for_slot(self, unit: str, color: str, query_time_sp: datetime, slot: str) -> float:
        """
        Core Concept 1-4 logic for a specific (unit, color, slot).
        """
        day_str = query_time_sp.date().isoformat()
        # logger.info(f"day_str: {day_str}")
        weekday = query_time_sp.weekday()
        # logger.info(f"weekday: {weekday}")
        ref_date = query_time_sp.date()
        # logger.info(f"ref_date: {ref_date}")
        # Concept 1: same day & same slot
        # logger.info(f"debug unit: {unit}")
        # logger.info(f"debug color: {color}")
        # logger.info(f"debug slot: {slot}")
        # logger.info(f"debug day_str: {day_str}")
        df1 = self.ds.fetch_samples_unit_day_slot_color_df(unit, color, slot, day_str)
        # logger.info("df1 as json: %s", df1.to_json(orient="records"))
        # df1 has columns ["delta_t","day"]; all days == ref_date
        s1 = apply_iqr_filter(df1["delta_t"].to_numpy(), IQR_OUTLIER_FACTOR)
        n1 = len(s1)
        m1 = float(np.median(s1)) if n1 else None

        # Concept 3: all days, same slot
        df3 = self.ds.fetch_samples_unit_slot_color_all_days_df(unit, color, slot)
        # logger.info("df3 as json: %s", df3.to_json(orient="records"))
        # raw3 = apply_iqr_filter(df3["delta_t"].to_numpy(), IQR_OUTLIER_FACTOR)
        # temporal weights by day
        weights3 = compute_temporal_weights(
            [d for d in df3["day"]], ref_date, TEMPORAL_DECAY_RATE
        )
        # logger.info(f"weights3: {weights3}")
        # align weights to raw3 after filter (simplest: assume df3 already IQR-filtered)
        raw3 = df3["delta_t"].to_numpy()
        n3 = len(raw3)
        m3 = float(weighted_median(raw3, weights3)) if n3 else None

        # Concept 2: same weekday, same slot
        df2 = self.ds.fetch_samples_unit_color_slot_weekday_df(unit, color, slot, weekday)
        # logger.info("df2 as json: %s", df2.to_json(orient="records"))
        # raw2 = apply_iqr_filter(df2["delta_t"].to_numpy(), IQR_OUTLIER_FACTOR)
        raw2 = df2["delta_t"].to_numpy()
        weights2 = compute_temporal_weights(
            [d for d in df2["day"]], ref_date, TEMPORAL_DECAY_RATE
        )
        # logger.info(f"weights2: {weights2}")
        n2 = len(raw2)
        m2 = float(weighted_median(raw2, weights2)) if n2 else None

        # Concept 4: cross‐unit, same slot
        df4 = self.ds.fetch_samples_color_slot_all_units_df(color, slot)
        # logger.info("df4 as json: %s", df4.to_json(orient="records"))
        s4 = apply_iqr_filter(df4["delta_t"].to_numpy(), IQR_OUTLIER_FACTOR)
        n4 = len(s4)
        m4 = float(np.median(s4)) if n4 else DEFAULT_WAIT_BY_SLOT_COLOR[slot][color]

        # ——————————————————————————————
        # 1) Base: Prefers C1, else C3, else C4
        fallback_to_c3 = False
        if n1 >= CONCEPT1_MIN_SAMPLES:
            # logger.info("using: same day & same slot")
            est, total_n = m1, n1
            fallback_to_c3 = (n1 == CONCEPT1_MIN_SAMPLES)
        elif n3 > 0:
            # logger.info("using: all days, same slot")
            est, total_n = m3, n3
            fallback_to_c3 = True
        else:
            # logger.info("using: cross-unit, same slot")
            est, total_n = m4, n4
            fallback_to_c3 = False

        # 2) Tilt toward C2 if available
        if m2 is not None:
            # logger.info("using: same weekday, same slot")
            w2 = n2 / (total_n + n2)
            est = (1 - w2) * est + w2 * m2
            total_n += n2

        # 3) Dynamic C3 threshold based on how long we've been collecting
        threshold3 = max(CONCEPT3_MIN_SAMPLES, n2)

        # 4) If we fell back to C3 but have too few C3 samples, tilt toward C4
        if fallback_to_c3 and n3 < threshold3:
            w4 = n4 / (total_n + n4)
            est = (1 - w4) * est + w4 * m4
            total_n += n4

        # 5) Clip to plausible range
        plausible_delta = self._clip(est)
        rc_room_wait_slot = assign_rc_wait(query_time_sp, RC_TIME_SLOTS)
        return plausible_delta + rc_room_wait_slot

    def _clip(self, value: float) -> float:
        """Ensure we never predict outside [MIN_WAIT, MAX_WAIT]."""
        return max(min(value, MAX_WAIT_MINUTES), MIN_WAIT_MINUTES)


if __name__ == "__main__":
    datastore = DataStore()
    w = WaitTimeEstimator(datastore)
    dt_object = datetime.strptime("2025-06-04T12:05:15.140Z", "%Y-%m-%dT%H:%M:%S.%fZ")
    v = w.estimate_wait_time("UPA Urias Magalhães", "g", dt_object)
    print(v)