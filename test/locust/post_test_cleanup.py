import os
import sys
import logging

from pathlib import Path
from typing import List
from tqdm import tqdm
from urllib.parse import urlparse

import arrow
import psycopg2
from gnenv import create_env
from gnenv.environ import GNEnvironment
from loguru import logger

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, Session
from cassandra.cqlengine import connection

from dinofw.utils import split_into_chunks
from dinofw.utils.config import ConfigKeys

DRYRUN = True

logging.getLogger("cassandra.io.asyncorereactor").setLevel(logging.WARNING)
logging.getLogger("cassandra.cluster").setLevel(logging.WARNING)
logging.getLogger("cassandra.connection").setLevel(logging.WARNING)
logging.getLogger("cassandra.pool").setLevel(logging.WARNING)
logging.getLogger("cassandra.cqlengine.connection").setLevel(logging.WARNING)
logging.getLogger("cassandra.policies").setLevel(logging.INFO)


def get_env(env_name: str) -> GNEnvironment:
    CONFIG_DIR = str(Path(__file__).parent.parent.parent)
    SECRETS_DIR = os.path.join(CONFIG_DIR, "secrets")

    if not os.path.exists(CONFIG_DIR):
        print(f"error: config dir {CONFIG_DIR} does not exist")
        sys.exit(1)

    if not os.path.exists(SECRETS_DIR):
        print(f"error: secrets dir {SECRETS_DIR} does not exist")
        sys.exit(1)

    if not os.path.exists(os.path.join(SECRETS_DIR, f"{env_name}.yaml")):
        print(f"error: secrets file {env_name}.yaml does not exist in {SECRETS_DIR}")
        sys.exit(1)

    return create_env(
        config_path=CONFIG_DIR,
        gn_environment=env_name,
        secrets_path=SECRETS_DIR,
        quiet=False
    )


def get_group_ids(conn, min_user_id: int, the_test_date: str) -> List[str]:
    the_day_after = arrow.get(the_test_date).shift(days=1).format("YYYY-MM-DD")

    with conn.cursor() as curs:
        try:
            curs.execute(f"""
                SELECT 
                    distinct group_id 
                FROM 
                    user_group_stats 
                WHERE 
                    user_id >= {min_user_id} AND
                    join_time >= '{the_test_date}' AND 
                    join_time < '{the_day_after}';
            """)

            rows = curs.fetchall()
            if rows is None or not len(rows):
                logger.warning(f"could not find any users within {the_day_after}")
                sys.exit(1)

            rows = [row[0] for row in rows]
            logger.info(f"groups matching test date of {the_test_date}: {len(rows)}")
        except (Exception, psycopg2.DatabaseError) as e:
            logger.error(f'error querying: {e}')
            logger.exception(e)
            sys.exit(1)

    return rows


def delete_cassandra_test_messages(session: Session, group_ids: List[str], the_test_date: str):
    the_day_after = arrow.get(the_test_date).shift(days=1).format("YYYY-MM-DD")
    test_datetime = f"{the_test_date} 00:00:00.000+0000"
    day_after_datetime = f"{the_day_after} 00:00:00.000+0000"

    for group_id_chunk in tqdm(split_into_chunks(group_ids, 100), desc="deleting test messages"):
        group_id_chunk_str = ",".join(group_id_chunk)

        try:
            query_string = f"""
                DELETE FROM 
                    messages 
                WHERE 
                    group_id IN ({group_id_chunk_str}) AND
                    created_at >= '{test_datetime}' AND 
                    created_at < '{day_after_datetime}';
            """

            if DRYRUN:
                print(f"dryrun: session.execute('{query_string}')")
            else:
                session.execute(query_string)
        except Exception as e:
            logger.error(f'could not delete cassandra messages:: {str(e)}')
            logger.exception(e)
            sys.exit(1)


def _delete_postgres_data(
        conn: psycopg2.extensions.connection,
        group_ids: List[str],
        min_user_id: int,
        the_test_date: str,
        table_name: str,
        date_filter_column: str
):
    the_day_after = arrow.get(the_test_date).shift(days=1).format("YYYY-MM-DD")

    with conn.cursor() as curs:
        for group_id_chunk in tqdm(split_into_chunks(group_ids, 100), desc=f"deleting test {table_name} data"):
            try:
                gid_string = ",".join([f"'{gid}'" for gid in group_id_chunk])

                query_string = f"""
                    DELETE FROM 
                        {table_name}
                    WHERE
                        group_id IN ({gid_string}) AND
                        user_id >= {min_user_id} AND
                        {date_filter_column} >= '{the_test_date}' AND
                        {date_filter_column} < '{the_day_after}';
                """

                if DRYRUN:
                    print(f"dryrun: curs.execute('{query_string}')")
                else:
                    curs.execute(query_string)

            except (Exception, psycopg2.DatabaseError) as e:
                logger.error(f'error deleting test {table_name}: {e}')
                logger.exception(e)
                sys.exit(1)


def delete_postgres_test_user_data(
        conn: psycopg2.extensions.connection,
        group_ids: List[str],
        min_user_id: int,
        the_test_date: str
):
    _delete_postgres_data(
        conn=conn,
        group_ids=group_ids,
        min_user_id=min_user_id,
        the_test_date=the_test_date,
        table_name="user_group_stats",
        date_filter_column="join_time"
    )


def delete_postgres_test_group_data(
        conn: psycopg2.extensions.connection,
        group_ids: List[str],
        min_user_id: int,
        the_test_date: str
):
    _delete_postgres_data(
        conn=conn,
        group_ids=group_ids,
        min_user_id=min_user_id,
        the_test_date=the_test_date,
        table_name="groups",
        date_filter_column="created_at"
    )


def get_cassandra_session(env: GNEnvironment) -> Session:
    key_space = env.config.get(ConfigKeys.KEY_SPACE, domain=ConfigKeys.STORAGE)
    c_pass = env.config.get(ConfigKeys.PASSWORD, domain=ConfigKeys.STORAGE)
    c_user = env.config.get(ConfigKeys.USER, domain=ConfigKeys.STORAGE)
    c_host = env.config.get(ConfigKeys.HOST, domain=ConfigKeys.STORAGE).split(',')

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

    return cluster.connect(key_space)


def get_postgres_connection(env: GNEnvironment) -> psycopg2.extensions.connection:
    result = urlparse(env.config.get(ConfigKeys.URI, domain=ConfigKeys.DB))

    try:
        return psycopg2.connect(
            database=result.path[1:],
            user=result.username,
            password=result.password,
            host=result.hostname,
            port=result.port
        )
    except Exception as e:
        logger.error(f"could not connect to the database: {str(e)}")
        logger.exception(e)
        sys.exit(1)


def validate_args(args):
    if len(args) < 3:
        print("This script will delete all test data from the databases that was created by the load-test tool.")
        print("Use with caution.")
        print()
        print("Usage: python3 post_test_cleanup.py <ENV_NAME> <DAY TEST WAS RUN ON: 2025-03-18>")
        sys.exit(1)

    if "test" not in args[1] and "stag" not in args[1]:
        print("error: ENV_NAME must be a test or staging environment")
        sys.exit(1)

    global DRYRUN
    if "--no-dryrun" in args:
        DRYRUN = False
        print("NOT RUNNING IN DRY-RUN MODE")
    else:
        DRYRUN = True
        print("running in dry-run mode")


def get_group_ids_from_disk_or_db(
        conn: psycopg2.extensions.connection,
        min_user_id: int,
        the_test_date: str
) -> List[str]:
    """
    If the group ids file exists, read the group ids from it, otherwise query the database and write the group ids to
    the file. This is so that if the deletion stops or fails, we can resume from where we left off.
    """
    group_ids_file = f"group-ids-{arrow.utcnow().format('YYYY-MM-DD')}"

    if os.path.exists(group_ids_file):
        with open(group_ids_file, 'r') as f:
            group_ids = [line.replace('\n', '') for line in f.readlines()]
            logger.info(f"found {len(group_ids)} groups from disk cache for file {group_ids_file}")
    else:
        group_ids = get_group_ids(conn, min_user_id=min_user_id, the_test_date=the_test_date)
        with open(group_ids_file, 'w') as f:
            for group_id in group_ids:
                f.write(f"{group_id}\n")

        logger.info(f"found {len(group_ids)} groups from database and wrote to disk cache for file {group_ids_file}")

    return group_ids

def run():
    validate_args(sys.argv)

    # no user in prod has this high user id
    min_user_id = 1_000_000_000

    env_name = sys.argv[1]
    the_test_date = sys.argv[2]

    env = get_env(env_name=env_name)
    session = get_cassandra_session(env)
    conn = get_postgres_connection(env)

    group_ids = get_group_ids_from_disk_or_db(conn, min_user_id=min_user_id, the_test_date=the_test_date)

    delete_cassandra_test_messages(session, group_ids, the_test_date=the_test_date)
    delete_postgres_test_user_data(conn, group_ids, min_user_id=min_user_id, the_test_date=the_test_date)
    delete_postgres_test_group_data(conn, group_ids, min_user_id=min_user_id, the_test_date=the_test_date)


if __name__ == '__main__':
    try:
        run()
    except Exception as ee:
        logger.exception(ee)
        sys.exit(1)
