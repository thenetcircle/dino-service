import sys

import requests

BASE_URL = sys.argv[1]
USERS = list()
HEADERS = {
    "Content-Type": "application/json"
}

with open("users.txt", "r") as f:
    for line in f:
        USERS.append(int(float(line.replace("\n", ""))))


class Endpoints:
    BASE = "http://{host}/v1"
    GROUPS = BASE + "/users/{user_id}/groups"


def call_groups(_user_id):
    r = requests.post(
        url=Endpoints.GROUPS.format(
            host=BASE_URL,
            user_id=_user_id
        ),
        json={
            "only_unread": False,
            "per_page": 50,
        },
        headers=HEADERS
    ).json()

    global n_groups
    n_groups += len(r)

    return r


for user in USERS:
    try:
        groups = call_groups(user)
        if groups is None or not len(groups):
            continue

        print(user)
    except Exception as e:
        print(f"ERROR: {str(e)}")
    except KeyboardInterrupt:
        break
