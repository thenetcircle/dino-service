import sys
import datetime
import numpy as np


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
elapsed_time = 0
n_scripts = len(sys.argv[1:])


def format_times(elapsed):
    elapsed = str(datetime.timedelta(seconds=elapsed))
    print(f"time elapsed: {elapsed}")
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

    total_calls_per_second = 0
    for prefix, key in [
        ("groups/sec", ApiKeys.GROUPS),
        ("histories/sec", ApiKeys.HISTORIES),
        ("send/sec", ApiKeys.SEND),
        ("stats/sec", ApiKeys.STATS),
    ]:
        calls_per_second = int((1000 / np.median(t_calls[key])) * n_scripts)
        total_calls_per_second += calls_per_second
        print(f"{prefix} \t {calls_per_second}".expandtabs(20))

    print()
    print(f"total API calls per second: {total_calls_per_second:.2f}")


for filename in sys.argv[1:]:
    with open(filename, "r") as f:
        new_elapsed = float(f.readline().replace("\n", ""))
        n_groups += int(float(f.readline().replace("\n", "")))
        n_messages += int(float(f.readline().replace("\n", "")))

        if new_elapsed > elapsed_time:
            elapsed_time = new_elapsed

        for line in f:
            key, timing = line.split(",")
            float_timing = float(timing.replace("\n", ""))

            t_calls[key].append(float_timing)


format_times(elapsed_time)
