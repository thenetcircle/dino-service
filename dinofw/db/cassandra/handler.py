from datetime import datetime as dt
from uuid import uuid4 as uuid

import pytz
from cassandra.cqlengine import connection
from cassandra.cqlengine.management import sync_table
from gnenv.environ import GNEnvironment

from dinofw.config import ConfigKeys
from dinofw.db.cassandra.models import ActionLogModel
from dinofw.db.cassandra.models import JoinerModel
from dinofw.db.cassandra.models import MessageModel
from dinofw.rest.models import GroupJoinerQuery
from dinofw.rest.models import HistoryQuery
from dinofw.rest.models import MessageQuery
from dinofw.rest.models import SendMessageQuery


class CassandraHandler:
    def __init__(self, env: GNEnvironment):
        self.env = env

    def setup_tables(self):
        key_space = self.env.config.get(ConfigKeys.KEY_SPACE, domain=ConfigKeys.STORAGE)
        hosts = self.env.config.get(ConfigKeys.HOST, domain=ConfigKeys.STORAGE)
        hosts = hosts.split(",")

        connection.setup(
            hosts,
            default_keyspace=key_space,
            protocol_version=3,
            retry_connect=True
        )

        sync_table(MessageModel)
        sync_table(ActionLogModel)
        sync_table(JoinerModel)

    def get_messages_in_group(self, group_id: str, query: MessageQuery):
        return (
            MessageModel.objects(
                MessageModel.group_id == group_id,
                MessageModel.created_at <= MessageQuery.to_dt(query.since),
            )
            .limit(query.per_page or 100)
            .all()
        )

    def get_action_log_in_group(self, group_id: str, query: HistoryQuery):
        return (
            ActionLogModel.objects(
                ActionLogModel.group_id == group_id,
                ActionLogModel.created_at <= HistoryQuery.to_dt(query.since),
            )
            .limit(query.per_page or 100)
            .all()
        )

    def get_joiners_in_group(self, group_id: str, query: GroupJoinerQuery):
        return (
            ActionLogModel.objects(
                JoinerModel.group_id == group_id,
                JoinerModel.created_at <= GroupJoinerQuery.to_dt(query.since),
            )
            .filter(JoinerModel.status == query.status)
            .limit(query.per_page or 100)
            .all()
        )

    def store_message(self, group_id: str, user_id: int, query: SendMessageQuery):
        created_at = dt.utcnow()
        created_at = created_at.replace(tzinfo=pytz.UTC)
        message_id = uuid()

        MessageModel.create(
            group_id=group_id,
            user_id=user_id,
            created_at=created_at,
            message_id=message_id,
            message_payload=query.message_payload,
            message_type=query.message_type,
        )

        return str(message_id)
