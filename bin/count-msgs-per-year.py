import os.path
import sys
import traceback
from collections import defaultdict

from pprint import pprint
from tqdm import tqdm, trange

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.cqlengine import connection
import yaml
import psycopg2


def count_messages(config_file, max_year):
    start_year = 2007
    max_year = int(max_year)
    year_counts = defaultdict(lambda: 0)

    with open(config_file, 'r') as f:
        config = yaml.safe_load(f.read())

    key_space = config['cassandra']['keyspace']
    c_pass = config['cassandra']['username']
    c_user = config['cassandra']['password']
    c_host = config['cassandra']['host'].split(',')

    p_host = config['postgres']['host']
    p_user = config['postgres']['user']
    p_pass = config['postgres']['pass']
    p_name = config['postgres']['name']

    community = config['community']

    print(f"{max_year=}, {community=}")

    try:
        conn = psycopg2.connect(f"dbname='{p_name}' user='{p_user}' host='{p_host}' password='{p_pass}'")
    except Exception as e:
        print(f"could not connect to the database: {str(e)}")
        print(traceback.format_exception(e))
        sys.exit(1)


    with conn.cursor() as curs:
        try:
            for year in range(start_year, int(max_year)):
                after = f"{year}-01-01"
                before = f"{year+1}-01-01"

                year_path = f"groups-{community}-{year}.csv"
                if os.path.exists(year_path):
                    continue

                curs.execute(f"SELECT group_id FROM groups where first_message_time < '{before}' and first_message_time >= '{after}' limit 10;")
                rows = curs.fetchall()

                print(f'groups started in {year}: {len(rows)}')

                with open(year_path, 'w') as f:
                    for row in rows:
                        f.write(f"{row[0]}\n")

        except (Exception, psycopg2.DatabaseError) as error:
            print(f'error querying: {error}')

    kwargs = {
        "default_keyspace": key_space,
        "protocol_version": 3,
        "retry_connect": True,
        "auth_provider": PlainTextAuthProvider(
            username=c_user,
            password=c_pass,
        )
    }

    connection.setup(c_host, **kwargs)

    cluster = Cluster(
        contact_points=c_host,
        protocol_version=3,
        auth_provider=kwargs["auth_provider"]
    )

    session = cluster.connect(key_space)

    for year in trange(start_year, int(max_year), desc="parsing years"):
        msg_file = f"msgs-{community}-{year}.csv"
        group_file = f"groups-{community}-{year}.csv"

        if os.path.exists(msg_file):
            continue

        with open(group_file, 'r') as f:
            group_ids = f.read().splitlines()

        for group_id in tqdm(group_ids, leave=False):
            try:
                rows = session.execute(f"SELECT message_id, created_at FROM messages where group_id = {group_id}")
                rows = list(rows)

                for row in list(rows):
                    if row.created_at.year > max_year:
                        continue
                    year_counts[row.created_at.year] += 1
            except Exception as e:
                print(f'could not query msgs for group {group_id}: {str(e)}')

    pprint(year_counts)


if __name__ == "__main__":
    count_messages(sys.argv[1], sys.argv[2])
    # count_messages('count-labpopp.yml', '2013')
