import os
import sys

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


def get_group_ids(_db_uri, _user_id):
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
            curs.execute(f"select distinct group_id from user_group_stats where user_id = '{_user_id}'")
            rows = curs.fetchall()
            return [row[0] for row in rows]
        except (Exception, psycopg2.DatabaseError) as e:
            logger.error(f"could not query db: {str(e)}")
            logger.exception(e)
            sys.exit(1)


env_name = sys.argv[1]
user_id = sys.argv[2]
outname = sys.argv[3]

secrets_path = os.path.join("..", "secrets", f"{env_name}.yaml")
with open(secrets_path) as f:
    secrets = yaml.safe_load(f.read())
    db_uri = secrets["DINO_DB_URI"]
    key_space = secrets["DINO_STORAGE_KEY_SPACE"]

logger.info(f"exporting messages for user '{user_id}' for environment '{env_name}'")

group_ids = get_group_ids(db_uri, user_id)

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
        f.write('group_id,created_at,sender_id,message_type,file_id,message_payload\n')

    # don't increase above 100 in batch size, query usually will timeout then and the whole export needs to be restarted
    for group_ids_to_try in tqdm(split_into_chunks(group_ids, batch_size), total=len(group_ids) / batch_size):
        group_ids = [
            group_id for group_id in group_ids_to_try
                if ','.join([str(j) for j in group_id_to_users(group_id)]) not in existing
        ]

        if not len(group_ids):
            continue

        try:
            rows = session.execute("select group_id, created_at, user_id, message_type, file_id, message_payload from messages where group_id in ({})".format(
                ','.join(group_ids)
            ))

            for row in rows:
                line_to_write = ",".join(map(str, [row.group_id, row.created_at, row.user_id, row.message_type, row.file_id, row.message_payload]))
                f.write(line_to_write + '\n')

        except Exception as e:
            print(e)
            raise e
