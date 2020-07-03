from unittest import TestCase

from dinofw.rest.models import GroupQuery
from dinofw.storage.cassandra import CassandraHandler


class TestCassandraHandler(TestCase):
    USER_ID = 1234

    def setUp(self) -> None:
        self.handler = CassandraHandler()

    def test_query_groups(self):
        query = GroupQuery(per_page=10)
        self.handler.get_groups_for_user(TestCassandraHandler.USER_ID, query)
