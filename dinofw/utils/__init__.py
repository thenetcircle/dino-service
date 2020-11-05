import arrow
from datetime import datetime


def split_into_chunks(objects, n):
    for i in range(0, len(objects), n):
        # yields successive n-sized chunks of data
        yield objects[i:i + n]


def utcnow_ts():
    # force the use of milliseconds instead microseconds
    now = arrow.utcnow()
    seconds = now.int_timestamp
    ms = now.format("SSS")

    return round(float(f"{seconds}.{ms}"), 3)


def utcnow_dt(ts: float = None):
    if ts is None:
        return arrow.get(utcnow_ts()).datetime
    return arrow.get(ts).datetime


def trim_micros(dt: datetime):
    ts_millis = round(dt.timestamp(), 3)
    return datetime.fromtimestamp(ts_millis, tz=dt.tzinfo)
