import json
from datetime import datetime
from datetime import timedelta
from typing import Optional

import arrow


def split_into_chunks(objects, n):
    for i in range(0, len(objects), n):
        # yields successive n-sized chunks of data
        yield objects[i:i + n]


def unicode_len(string):
    return int(len(string.encode(encoding="utf_16_le")) / 2)


def truncate_json_message(msg, limit=100, only_content: bool = False):
    if msg is None:
        return None

    try:
        msg_json = json.loads(msg)
    except Exception:
        return msg

    # not a text message
    if "content" not in msg_json:
        return msg

    if only_content:
        # ignore other keys
        msg_json = {
            "content": msg_json["content"]
        }
        msg = json.dumps(msg_json, ensure_ascii=False)

    n_chars_content = len(json.dumps(msg_json["content"], ensure_ascii=False))
    if n_chars_content <= limit:
        return msg

    # in case of emojis, strip end of potential backslashes
    msg_json["content"] = msg_json["content"][:limit]
    msg_json["content"] = msg_json["content"].rstrip("\\")

    return json.dumps(msg_json, ensure_ascii=False)


def is_none_or_zero(number):
    if number is None:
        return True

    try:
        int_value = int(number)
    except ValueError:
        return True

    return int_value == 0


def is_non_zero(number):
    if number is None:
        return False

    try:
        int_value = int(number)
    except ValueError:
        return False

    return int_value > 0


def max_one_year_ago(delete_before, since):
    a_year_ago = delete_before - timedelta(days=365)
    print(f"delete_before: {delete_before}, since: {since}, a_year_ago: {a_year_ago}")
    print(f'max: {max(delete_before - timedelta(days=365), since)}')

    if a_year_ago > since:
        return a_year_ago
    return since


def utcnow_ts():
    # force the use of milliseconds instead microseconds
    now = arrow.utcnow()
    seconds = now.int_timestamp
    ms = now.format("SSS")

    return round(float(f"{seconds}.{ms}"), 3)


def utcnow_dt(ts: float = None, ms_to_add: int = 0):
    if ts is None:
        dt = arrow.get(utcnow_ts()).datetime
    else:
        dt = arrow.get(ts).datetime

    if ms_to_add > 0:
        # if user is sending multiple images at the same time, there's a change different servers will
        # create them, causing potential primary key collision if the generated time has the exact same
        # milliseconds, so add some ms to each creation time
        dt += timedelta(milliseconds=ms_to_add)

    return dt


def trim_micros(dt: datetime, allow_none: bool = False) -> Optional[datetime]:
    if allow_none and dt is None:
        return

    ts_millis = round(dt.timestamp(), 3)
    return datetime.fromtimestamp(ts_millis, tz=dt.tzinfo)


def users_to_group_id(user_a: int, user_b: int) -> str:
    # convert integer ids to hex; need to be sorted
    users = map(hex, sorted([user_a, user_b]))

    # drop the initial '0x' and left-pad with zeros (a uuid is two
    # 16 character parts, so pad to length 16)
    u = "".join([user[2:].zfill(16) for user in users])

    # insert dashes at the correct places
    return f"{u[:8]}-{u[8:12]}-{u[12:16]}-{u[16:20]}-{u[20:]}"


def group_id_to_users(group_id: str) -> (int, int):
    group_id = group_id.replace("-", "")
    user_a = int(group_id[:16].lstrip("0"), 16)
    user_b = int(group_id[16:].lstrip("0"), 16)
    return sorted([user_a, user_b])


def to_dt(s, allow_none: bool = False, default: datetime = None) -> Optional[datetime]:
    if s is None and default is not None:
        return default

    if s is None and allow_none:
        return None

    if s is None:
        s = utcnow_dt()
    else:
        # millis not micros
        s = arrow.get(round(float(s), 3)).datetime

    return s


def to_ts(ds, allow_none: bool = False) -> Optional[float]:
    if ds is None and allow_none:
        return None

    if ds is None:
        return utcnow_ts()

    # millis not micros
    return round(arrow.get(ds).float_timestamp, 3)


def need_to_update_stats_in_group(user_stats, last_message_time: datetime):
    if user_stats.bookmark:
        return True

    if user_stats.mentions > 0:
        return True

    if user_stats.highlight_time > last_message_time:
        return True

    return last_message_time > user_stats.last_read
