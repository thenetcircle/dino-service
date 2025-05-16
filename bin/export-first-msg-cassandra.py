import os
import sys

import arrow
import dotenv
from cassandra.cluster import Cluster
from cassandra.cluster import PlainTextAuthProvider
from cassandra.cqlengine import connection
from tqdm import tqdm

dotenv.load_dotenv()


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

dinoms_feti_prod=# copy (select group_id from groups where updated_at >= '2025-05-06 20:43:13+00' and first_message_time <= '2025-05-06 20:45:38+00') to '/tmp/feti_msgs_psql-250516-2.csv' (format csv);
COPY 108091
"""

with open(sys.argv[1]) as f:
    group_ids = [g.replace('\n', '') for g in f.readlines()]

from_time = '2025-05-06 16:19:00.000+0000'
to_time = '2025-05-06 16:21:03.000+0000'
outname = 'feti_msgs_cassandra-250516.csv'
existing = set()
batch_size = 75

if os.path.exists(outname):
    with open(outname, 'r') as f:
        for line in f.readlines():
            existing.add(','.join(sorted(line.split(',')[0:2])))

print(f'found {len(existing)} existing exports already')

kwargs = {
    "default_keyspace": os.environ['CASD_KEY_SPACE'],
    "protocol_version": 3,
    "retry_connect": True,
    "auth_provider": PlainTextAuthProvider(
        username=os.environ['CASD_USER'],
        password=os.environ['CASD_PASS']
    )
}
connection.setup(os.environ['CASD_HOSTS'].split(','), **kwargs)

cluster = Cluster(
    contact_points=os.environ['CASD_HOSTS'],
    protocol_version=3,
    auth_provider=kwargs["auth_provider"]
)

session = cluster.connect(os.environ['CASD_KEY_SPACE'])


with open(outname, 'wa') as f:
    if not len(existing):
        f.write('sender_id,receiver_id,created_at\n')

    # don't increase above 100 in batch size, query usually will timeout then and the whole export needs to be restarted
    for group_ids_to_try in tqdm(split_into_chunks(group_ids, batch_size), total=len(group_ids) / batch_size):
        group_ids = [
            group_id for group_id in group_ids_to_try
                if ','.join([str(j) for j in group_id_to_users(group_id)]) not in existing
        ]

        if not len(group_ids):
            continue

        try:
            rows = session.execute("select group_id, created_at, user_id, message_type from messages where group_id in ({}) and created_at > '{}' and created_at < '{}'".format(
                ','.join(group_ids),
                from_time,
                to_time
            ))

            for row in rows:
                receiver_id = receiver_for(row.group_id, row.user_id)

                line_to_write = ",".join([row.user_id, receiver_id, row.message_type, str(arrow.get(row.created_at).float_timestamp)])
                f.write(line_to_write + '\n')

        except Exception as e:
            print(e)
            raise e
