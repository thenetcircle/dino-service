#!/bin/python

import sys
import os
import re
import subprocess

from datetime import datetime
from datetime import timedelta

unit = sys.argv[1]
pattern = sys.argv[2]
last_check_file = "~/.dino-journalctl-last-check"

if os.path.exists(last_check_file):
    with open(last_check_file, "r") as f:
        last_check = f.readline().replace("\n", "")
else:
    now = datetime.now()
    an_hour_ago = now - timedelta(hours=1)
    last_check = an_hour_ago.strftime("%Y-%m-%d %H:%M:%S")

process = subprocess.Popen([
    "journalctl", "-u", unit, "-o", "cat", "--since", last_check, "--no-pager"
], stdout=subprocess.PIPE)

out, _ = process.communicate()
out = str(out, "utf-8")
logs = out.strip().split("\n")

most_recent_timestamp_line = None

# reverse check last timestamp
for log in logs[::-1]:
    if log.startswith("202"):
        most_recent_timestamp_line = log
        break

if most_recent_timestamp_line is None:
    this_check_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
else:
    this_check_timestamp = most_recent_timestamp_line.split(" ", maxsplit=1)[0]

with open(last_check_file, "w") as f:
    f.write(this_check_timestamp)

regex = re.compile(pattern)

for log in logs:
    if regex.search(log) is not None:
        print(f"found log line matching pattern '{pattern}': {log}")
        sys.exit(1)

sys.exit(0)
