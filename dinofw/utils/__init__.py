import json
import math
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


def utcnow_ts(trim_micros: bool = False):
    if not trim_micros:
        return arrow.utcnow().float_timestamp

    # otherwise trim it, only used for tests to compare with api responses
    now = arrow.utcnow()
    now_float = now.float_timestamp

    # use modf and round to avoid floating point issues, e.g. 5.55 => 0.5499999999999998
    micros_only = round(math.modf(now_float)[0], 6)

    # to check if there's non-zero micros set in the db, e.g. '0.543000'
    micros_as_millis = round(math.modf(micros_only * 1000)[0], 3)

    # no needs to floor value if all 6 digits are 0 (e.g. the default long_ago timestamp),
    # also no need if it's a timestamp set by community that does not have micros, e.g. '0.444000'
    if micros_only == 0 or micros_as_millis == 0:
        return round(math.modf(now_float)[0], 3)

    # round again to avoid floating point issues, e.g. 789000000.001 => 789000000.0020001
    return round(now_float + 0.001, 3)


def utcnow_dt(ts: float = None):
    if ts is None:
        return arrow.get(utcnow_ts()).datetime
    return arrow.get(ts).datetime


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


def to_dt(s, allow_none: bool = False, default: datetime = None, floor_millis: bool = False) -> Optional[datetime]:
    if s is None and default is not None:
        return default
    if s is None and allow_none:
        return None
    if s is None:
        s = utcnow_dt()
    else:
        if floor_millis:
            # when api using 'until' to get groups for user, it's compared to last_message_time; when paginating,
            # the client will call the api with the message time that has been increased by 1ms, so to avoid returning
            # duplicate groups, we have to subtract this 1ms when querying for groups
            s = arrow.get(round(float(s), 3)).datetime - timedelta(seconds=0.001)
        else:
            s = arrow.get(float(s)).datetime
    return s


def to_ts(ds, allow_none: bool = False) -> Optional[float]:
    if ds is None and allow_none:
        return None

    if ds is None:
        return utcnow_ts()

    arrow_ds = arrow.get(ds)

    # cassandra stores microseconds, but the api works with milliseconds (js requirement), so we
    # have to ceil the millis to interval queries still work as expected
    millis = round(arrow_ds.float_timestamp, 3)  # millis not micros

    # if rounding adds 1ms, don't add a 1ms again
    millis_str_before_trim = str(arrow_ds.float_timestamp).split(".")[1][:3]
    millis_str_after_trim = str(millis).split(".")[1]
    rounding_added_1ms = int(millis_str_after_trim) != int(millis_str_before_trim)

    # to check if there's non-zero micros set in the db, e.g. '0.543000'
    micros_as_millis = round(math.modf(arrow_ds.float_timestamp * 1000)[0], 3)

    # no needs to ceil value if all 6 digits are 0 (e.g. the default long_ago timestamp),
    # also no need if it's a timestamp set by community that does not have micros, e.g. '0.444000'
    if micros_as_millis == 0 or rounding_added_1ms:
        return millis

    # round again to avoid floating point issues, e.g. 789000000.001 => 789000000.0020001
    return round(millis + 0.001, 3)


def need_to_update_stats_in_group(user_stats, last_message_time: datetime):
    if user_stats.bookmark:
        return True

    if user_stats.highlight_time > last_message_time:
        return True

    return last_message_time > user_stats.last_read
