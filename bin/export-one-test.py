import subprocess
import sys
import arrow

from_time = '2023-12-07 02:10:00.000+0000'
to_time = '2023-12-07 03:25:00.000+0000'
group_id = '00000000-00a8-449a-0000-000000a91f15'

args = [
    "/usr/local/tncdata/apps/apache-cassandra-3.11.8/bin/cqlsh",
    "--request-timeout=3600",
    "casd1",
    "-k",
    "dinoms_popp_prod",
    "-e"
    "paging off; select created_at, user_id from messages where group_id = {} and created_at > '{}' and created_at < '{}'".format(
        group_id,
        from_time,
        to_time
    )
]

cmd_output = str(subprocess.check_output(args), 'utf-8')


