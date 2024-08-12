import sys

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.cqlengine import connection


hosts = sys.argv[1].split(',')
username = sys.argv[2]
password = sys.argv[3]
key_space = sys.argv[4]
year = int(float(sys.argv[5]))

print(f'{hosts=}, {username=}, {password=}, {key_space=}, {year=}')
sys.exit(1)

kwargs = {
    "default_keyspace": key_space,
    "protocol_version": 3,
    "retry_connect": True,
    "auth_provider": PlainTextAuthProvider(
        username=username,
        password=password,
    )
}

connection.setup(hosts, **kwargs)

cluster = Cluster(
    contact_points=hosts,
    protocol_version=3,
    auth_provider=kwargs["auth_provider"]
)

session = cluster.connect(key_space)

before = f'{year}-01-01 00:00:00'
after = f'{year+1}-01-01 00:00:00'

rows = session.execute(f"SELECT count(*) FROM messages where created_at < '{after}' and created_at >= '{before}' allow filtering")
print(f'Number of messages in {year}: {len(rows)}')


