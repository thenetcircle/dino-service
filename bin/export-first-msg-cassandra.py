import subprocess
import sys
import arrow
from tqdm import tqdm


def group_id_to_users(group_id_: str) -> (int, int):
    group_id_ = group_id_.replace("-", "")
    user_a_ = int(group_id_[:16].lstrip("0"), 16)
    user_b_ = int(group_id_[16:].lstrip("0"), 16)
    return sorted([user_a_, user_b_])


def receiver_for(group_id_, sender_id_):
    user_a, user_b = map(str, group_id_to_users(group_id_))
    if user_a == sender_id_:
        return user_b
    else:
        return user_a


def split_into_chunks(objects, n):
    for i in range(0, len(objects), n):
        # yields successive n-sized chunks of data
        yield objects[i:i + n]


"""
get group ids using this query:

dinoms_popp_prod=# copy (select group_id from groups where updated_at >= '2023-12-07 02:10:00+00' and first_message_at < '2023-12-07 03:25:00+00') to '/tmp/popp_msgs_psql-231207.csv' (format csv);
COPY 104871
"""


with open(sys.argv[1]) as f:
    group_ids = [g.replace('\n', '') for g in f.readlines()]


from_time = '2023-12-07 03:25:00.000+0000'
to_time = '2023-12-07 07:25:00.000+0000'

with open('popp_msgs_cassandra-231212.csv', 'w') as f:
    f.write('sender_id,receiver_id,created_at\n')

    for group_ids in tqdm(split_into_chunks(group_ids, 100), total=len(group_ids) / 100):
        args = [
            "/usr/local/tncdata/apps/apache-cassandra-3.11.8/bin/cqlsh",
            "--request-timeout=3600",
            "casd1",
            "-k",
            "dinoms_popp_prod",
            "-e"
            "paging off; select group_id, created_at, user_id from messages where group_id in ({}) and created_at > '{}' and created_at < '{}'".format(
                ','.join(group_ids),
                from_time,
                to_time
            )
        ]

        cmd_output = str(subprocess.check_output(args), 'utf-8')

        try:
            for line in cmd_output.split('\n')[3:-3]:
                if line.startswith('----') or not len(line.strip()):
                    continue

                try:
                    group_id, created_at, sender_id = map(str.strip, line.split('|'))
                except Exception as e2:
                    print(e2)
                    print(line)
                    print(cmd_output)
                    continue

                receiver_id = receiver_for(group_id, sender_id)

                line_to_write = ",".join([sender_id, receiver_id, str(arrow.get(created_at).float_timestamp)])
                f.write(line_to_write + '\n')

        except Exception as e:
            print(e)
            print(cmd_output)
            raise e
