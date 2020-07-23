from unittest import TestCase

from cassandra.cluster import Cluster
from gnenv.environ import find_config
from gnenv.environ import load_secrets_file
from gnenv.environ import ConfigDict

from dinofw.config import ConfigKeys
from dinofw.rest.server.models import MessageQuery
from dinofw.db.cassandra.handler import CassandraHandler


class FakeEnv:
    class Config:
        def __init__(self):
            self.config = {
                "storage": {
                    "key_space": "dinofw",
                    "host": "maggie-cassandra-1,maggie-cassandra-2"
                }
            }

        def get(self, key, domain):
            return self.config[domain][key]

    def __init__(self):
        self.config = FakeEnv.Config()


class TestCassandraHandler(TestCase):
    USER_ID = 1234

    # TODO: handle this somehow, too slow to drop/create kespace between each test

    def setUp(self) -> None:
        config_dict, config_path = find_config("../..")
        config_dict = load_secrets_file(
            config_dict,
            secrets_path="../../secrets",
            env_name="test"
        )

        env = FakeEnv()
        env.config = ConfigDict(config_dict)

        """
        key_space = env.config.get(ConfigKeys.KEY_SPACE, domain=ConfigKeys.STORAGE)
        hosts = env.config.get(ConfigKeys.HOST, domain=ConfigKeys.STORAGE)
        hosts = hosts.split(",")

        if "test" in key_space:
            cluster = Cluster(hosts)

            session = cluster.connect()
            session.execute(f"DROP KEYSPACE {key_space};")
            session.execute(f"CREATE KEYSPACE {key_space};")

            cluster.shutdown()
        """

        self.handler = CassandraHandler(env)
        self.handler.setup_tables()

    def _test_get_messages_for_group(self):
        query = MessageQuery(per_page=10, since=1594064834)
        messages = self.handler.get_messages_in_group(
            'df82fea8-bffe-11ea-8bf5-f72fbcad0196',
            query
        )
        self.assertEqual(0, len(messages))
