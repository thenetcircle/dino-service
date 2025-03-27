import os
os.environ["CQLENG_ALLOW_SCHEMA_MANAGEMENT"] = "1"

from typing import Optional
from pathlib import Path

from cassandra.cluster import Cluster
from cassandra.cqlengine.connection import execute
from cassandra.cqlengine.management import sync_table
from gnenv.environ import find_config
from gnenv.environ import load_secrets_file
from gnenv.environ import ConfigDict

from dinofw.db.storage.handler import CassandraHandler
from dinofw.db.storage.models import AttachmentModel
from dinofw.db.storage.models import MessageModel
from dinofw.rest.queries import MessageQuery
from dinofw.utils.config import ConfigKeys
from dinofw.utils import utcnow_dt
from test.base import BaseTest


def get_project_root() -> Path:
    return Path(__file__).parent.parent.parent.parent


class FakeEnv:
    class Config:
        def __init__(self):
            self.config = {
                "storage": {
                    "key_space": "defaulttest",
                    "host": "maggie-cassandra-1,maggie-cassandra-2",
                }
            }

        def get(self, key, domain):
            return self.config[domain][key]

    def __init__(self):
        self.config = FakeEnv.Config()


class BaseCassandraHandlerTest(BaseTest):
    # invalid GROUP_ID in BaseTest
    ADMIN_ID = "5678"
    GROUP_ID = "df82fea8-bffe-11ea-8bf5-f72fbcad0196"

    # TODO: handle this somehow, too slow to drop/create kespace between each test

    def setUp(self) -> None:
        config_dict, config_path = find_config(str(get_project_root()))
        config_dict = load_secrets_file(
            config_dict,
            secrets_path=os.path.join(str(get_project_root()), "secrets"),
            env_name="test",
        )

        env = FakeEnv()
        env.config = ConfigDict(config_dict)
        key_space = env.config.get(ConfigKeys.KEY_SPACE, domain=ConfigKeys.STORAGE)
        hosts = env.config.get(ConfigKeys.HOST, domain=ConfigKeys.STORAGE)
        hosts = hosts.split(",")

        if "test" in key_space:
            cluster = Cluster(hosts)
            session = cluster.connect()
            session.execute(
                f"CREATE KEYSPACE IF NOT EXISTS {key_space} WITH REPLICATION={{'class':'SimpleStrategy', 'replication_factor':1}}"
            )
            cluster.shutdown()

        self.handler = CassandraHandler(env)
        self.handler.setup_tables()
        # sync table after setting up
        sync_table(MessageModel)
        sync_table(AttachmentModel)
        execute(f"TRUNCATE TABLE {key_space}.{MessageModel.__table_name__};")
        execute(f"TRUNCATE TABLE {key_space}.{AttachmentModel.__table_name__};")

    @classmethod
    def _generate_message_query(
        cls,
        page: int = 10,
        since: Optional[float] = None,
        until: Optional[float] = None,
    ) -> MessageQuery:
        return MessageQuery(per_page=page, since=since, until=until)

    async def clear_messages(self) -> None:
        await self.handler.delete_messages_in_group_before(
            BaseCassandraHandlerTest.GROUP_ID, utcnow_dt()
        )
        await self.assert_get_messages_in_group_empty()

    async def clear_attachments(self) -> None:
        await self.handler.delete_attachments_in_group_before(
            BaseCassandraHandlerTest.GROUP_ID, utcnow_dt()
        )
        await self.assert_get_attachments_in_group_for_user_empty()

    async def assert_get_messages_in_group_empty(self) -> None:
        messages = await self.handler.get_messages_in_group(
            BaseCassandraHandlerTest.GROUP_ID,
            BaseCassandraHandlerTest._generate_message_query(),
        )
        self.assertEqual(0, len(messages))

    async def assert_get_attachments_in_group_for_user_empty(self) -> None:
        messages = await self.handler.get_attachments_in_group_for_user(
            BaseCassandraHandlerTest.GROUP_ID,
            BaseCassandraHandlerTest._generate_user_group_stats(),
            BaseCassandraHandlerTest._generate_message_query(),
        )
        self.assertEqual(0, len(messages))
