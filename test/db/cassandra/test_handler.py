from unittest import TestCase

from dinofw.rest.models import HistoryQuery, MessageQuery
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

    def setUp(self) -> None:
        self.handler = CassandraHandler(FakeEnv())
        self.handler.setup_tables()

    def test_get_messages_for_group(self):
        query = MessageQuery(per_page=10, since=1594064834)
        messages = self.handler.get_messages_in_group(
            'df82fea8-bffe-11ea-8bf5-f72fbcad0196',
            query
        )
        self.assertIsNotNone(messages)
