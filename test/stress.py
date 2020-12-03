import random
import time
import sys
import socket
import os
import numpy as np
import traceback
from functools import wraps

import requests


N_RUNS = 1000
BASE_URL = sys.argv[1]
USERS = list()
HEADERS = {
    "Content-Type": "application/json"
}

with open("users.txt", "r") as f:
    for line in f:
        USERS.append(int(float(line.replace("\n", ""))))


class ApiKeys:
    GROUPS = "groups"
    HISTORIES = "histories"
    SEND = "send"
    STATS = "stats"


ALL_API_KEYS = [ApiKeys.__dict__[key] for key in ApiKeys.__dict__ if key.isupper()]


t_calls = {
    ApiKeys.GROUPS: list(),
    ApiKeys.HISTORIES: list(),
    ApiKeys.SEND: list(),
    ApiKeys.STATS: list(),
}
n_groups = 0
n_messages = 0
session = requests.Session()


class Endpoints:
    BASE = "http://{host}/v1"

    GROUPS = BASE + "/users/{user_id}/groups"
    HISTORIES = BASE + "/groups/{group_id}/user/{user_id}/histories"
    SEND = BASE + "/users/{user_id}/send"
    STATS = BASE + "/userstats/{user_id}"


def timeit(key: str):
    def factory(view_func):
        @wraps(view_func)
        def decorator(*args, **kwargs):
            before = time.time()

            try:
                return view_func(*args, **kwargs)
            except Exception as e:
                print(f"could not call api: {str(e)}")
                print(traceback.format_exc(e))
                return None
            finally:
                the_time = (time.time() - before) * 1000
                t_calls[key].append(the_time)
        return decorator
    return factory


@timeit(ApiKeys.GROUPS)
def call_groups(_user_id):
    r = session.post(
        url=Endpoints.GROUPS.format(
            host=BASE_URL,
            user_id=_user_id
        ),
        json={
            "only_unread": False,
            "per_page": 50,
        },
        headers=HEADERS
    )

    json = r.json()
    r.close()

    global n_groups
    n_groups += len(json)

    return json


@timeit(ApiKeys.HISTORIES)
def call_histories(_group_id, _user_id):
    r = session.post(
        url=Endpoints.HISTORIES.format(
            host=BASE_URL,
            group_id=_group_id,
            user_id=_user_id
        ),
        json={
            "only_unread": False,
            "per_page": 50,
        },
        headers=HEADERS
    )

    json = r.json()
    r.close()

    global n_messages
    n_messages += len(json["messages"])


@timeit(ApiKeys.SEND)
def call_send(_user_id, _receiver_id):
    session.post(
        url=Endpoints.SEND.format(
            host=BASE_URL,
            user_id=_user_id
        ),
        json={
            "receiver_id": _receiver_id,
            "message_type": 0,
            "message_payload": "{\"content\":\"stress test message\"}"
        },
        headers=HEADERS
    ).close()


@timeit(ApiKeys.STATS)
def call_user_stats(_user_id):
    session.post(
        url=Endpoints.STATS.format(
            host=BASE_URL,
            user_id=_user_id
        ),
        json={
            "count_unread": True,
            "only_unread": True,
            "hidden": False
        },
        headers=HEADERS
    ).close()


def format_times(elapsed):
    with open(f"{socket.gethostname().split('.')[0]}-p{os.getpid()}.txt", "w") as file:
        file.write(f"{elapsed}\n")
        file.write(f"{n_groups}\n")
        file.write(f"{n_messages}\n")

        for key, values in t_calls.items():
            for value in values:
                file.write(f"{key},{value}\n")

    print(f"time elapsed: {elapsed:.2f}s")
    print()

    for key in ALL_API_KEYS:
        if not len(t_calls[key]):
            continue

        calls = t_calls[key]

        mean = np.mean(calls)
        median = np.median(calls)
        p75 = np.percentile(calls, 75)
        p95 = np.percentile(calls, 95)
        p99 = np.percentile(calls, 99)

        p_api = f"{key}: {len(calls)}\t"
        p_mean = f"mean {mean:.2f}ms\t"
        p_median = f"median {median:.2f}ms\t"
        p_p75 = f"75%: {p75:.2f}ms\t"
        p_p95 = f"95%: {p95:.2f}ms\t"
        p_p99 = f"99%: {p99:.2f}ms"

        print(f"{p_api} {p_mean} {p_median} {p_p75} {p_p95} {p_p99}".expandtabs(20))

    print()
    print(f"number of groups: {n_groups}")
    print(f"number of messages: {n_messages}")
    print()

    for prefix, key in [
        ("groups/sec", ApiKeys.GROUPS),
        ("histories/sec", ApiKeys.HISTORIES),
        ("send/sec", ApiKeys.SEND),
        ("stats/sec", ApiKeys.STATS),
    ]:
        calls_per_second = 1000 / np.median(t_calls[key])
        print(f"{prefix} \t {calls_per_second:.2f}".expandtabs(20))


test_start = time.time()
groups_per_user = list()

for i in range(N_RUNS):
    try:
        user = USERS[i]
        groups = call_groups(user)
        if groups is None or not len(groups):
            print(f"no groups for user {user}")
            continue

        if "detail" not in groups:
            groups = random.choices(groups, k=5)

            for group in groups:
                call_histories(group["group"]["group_id"], user)

                users = [user["user_id"] for user in group["group"]["users"]]
                receiver_ids = [int(a_user) for a_user in users if int(a_user) != user]
                if not len(receiver_ids):
                    print(f"no receivers in group {group['group']['group_id']} for user {user}, users was {groups['group']['users']}")
                    continue

                receiver_id = receiver_ids[0]
                call_send(user, receiver_id)
                call_user_stats(user)
        else:
            print(f"problem in response: {groups}")

    except Exception as e:
        print(f"ERROR: {str(e)}")
        print(traceback.format_exc())
        time.sleep(2)
    except KeyboardInterrupt:
        break

test_elapsed = time.time() - test_start
format_times(test_elapsed)
