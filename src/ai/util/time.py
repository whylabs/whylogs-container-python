import time
from datetime import datetime
from dateutil import tz
from enum import Enum


def current_time_ms() -> int:
    return time.time_ns() // 1_000_000


class TimeGranularity(Enum):
    H = "Hour"
    D = "Day"


def truncate_time_ms(t: int, granularity: TimeGranularity) -> int:
    dt = datetime.fromtimestamp(t / 1000, tz=tz.tzutc()).replace(second=0, microsecond=0, minute=0)

    if granularity == TimeGranularity.H:
        trunc = dt
    elif granularity == TimeGranularity.D:
        trunc = dt.replace(hour=0)

    return int(trunc.timestamp() * 1000)
