import os
import sys

import arrow
import dotenv
from cassandra.cluster import Cluster
from cassandra.cluster import PlainTextAuthProvider
from cassandra.cqlengine import connection
from tqdm import tqdm
import psycopg2
from urllib.parse import urlparse
from loguru import logger
import yaml

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


def get_group_ids(_db_uri, _from_time, _to_time):
    try:
        result = urlparse(_db_uri)
        username = result.username
        password = result.password
        database = result.path[1:]
        hostname = result.hostname
        port = result.port

        conn = psycopg2.connect(
            database=database,
            user=username,
            password=password,
            host=hostname,
            port=port
        )
    except Exception as e:
        logger.error(f"unable to connect to the database: {str(e)}")
        logger.exception(e)
        sys.exit(1)

    with conn.cursor() as curs:
        try:
            curs.execute(f"select group_id from groups where updated_at >= '{_from_time}' and first_message_time < '{_to_time}'")
            rows = curs.fetchall()
            return [row[0] for row in rows]
        except (Exception, psycopg2.DatabaseError) as e:
            logger.error(f"could not query db: {str(e)}")
            logger.exception(e)
            sys.exit(1)


env_name = sys.argv[1]
from_time = arrow.get(int(sys.argv[2])).strftime('%Y-%m-%d %H:%M:%S+00')
to_time = arrow.get(int(sys.argv[3])).strftime('%Y-%m-%d %H:%M:%S+00')
outname = sys.argv[4]

secrets_path = os.path.join("..", "secrets", f"{env_name}.yaml")
with open(secrets_path) as f:
    secrets = yaml.safe_load(f.read())
    db_uri = secrets["DINO_DB_URI"]
    key_space = secrets["DINO_STORAGE_KEY_SPACE"]

logger.info(f"exporting messages from '{from_time}' to '{to_time}' for environment '{env_name}'")

group_ids = get_group_ids(db_uri, from_time, to_time)

existing = set()
batch_size = 75
mode = 'w'

if os.path.exists(outname):
    mode = 'a'
    with open(outname, 'r') as f:
        for line in f.readlines():
            existing.add(','.join(sorted(line.split(',')[0:2])))

print(f'found {len(existing)} existing exports already')

kwargs = {
    "default_keyspace": key_space,
    "protocol_version": 3,
    "retry_connect": True,
    "auth_provider": PlainTextAuthProvider(
        username=os.environ['CASD_USER'],
        password=os.environ['CASD_PASS']
    )
}
connection.setup(os.environ['CASD_HOSTS'].split(','), **kwargs)

cluster = Cluster(
    contact_points=os.environ['CASD_HOSTS'].split(','),
    protocol_version=3,
    auth_provider=kwargs["auth_provider"]
)

session = cluster.connect(key_space)


with open(outname, mode) as f:
    if not len(existing):
        f.write('sender_id,receiver_id,message_type,created_at\n')

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
                receiver_id = receiver_for(str(row.group_id), row.user_id)
                line_to_write = ",".join(map(str, [row.user_id, receiver_id, row.message_type, arrow.get(row.created_at).float_timestamp]))
                f.write(line_to_write + '\n')

        except Exception as e:
            print(e)
            raise e
