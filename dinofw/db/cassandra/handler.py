from datetime import datetime as dt
from typing import List
from uuid import uuid4 as uuid

import pytz
from cassandra.cqlengine import connection
from cassandra.cqlengine.management import sync_table
from gnenv.environ import GNEnvironment

from dinofw.config import ConfigKeys
from dinofw.db.cassandra.models import ActionLogModel
from dinofw.db.cassandra.models import JoinerModel
from dinofw.db.cassandra.models import MessageModel
from dinofw.db.cassandra.schemas import MessageBase, JoinerBase
from dinofw.rest.models import GroupJoinerQuery, GroupJoinQuery
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

    def get_messages_in_group(self, group_id: str, query: MessageQuery) -> List[MessageBase]:
        until = MessageQuery.to_dt(query.until)
        hide_before = MessageQuery.to_dt(query.hide_before)  # TODO: default hide_before should be loooong ago

        # TODO: add message_type and status filter from MessageQuery
        raw_messages = (
            MessageModel.objects(
                MessageModel.group_id == group_id,
                MessageModel.created_at <= until,
                MessageModel.created_at > hide_before,
            )
            .limit(query.per_page or 100)
            .all()
        )

        messages = list()

        for message in raw_messages:
            messages.append(CassandraHandler.message_base_from_entity(message))

        return messages

    def get_group_joins_for_status(self, group_id: str, query: GroupJoinerQuery) -> List[JoinerBase]:
        until = MessageQuery.to_dt(query.until)
        hide_before = MessageQuery.to_dt(query.hide_before)  # TODO: default hide_before should be loooong ago

        raw_joins = (
            JoinerModel.objects(
                JoinerModel.group_id == group_id,
                JoinerModel.status == query.status,
                JoinerModel.created_at <= until.until,
                MessageModel.created_at > hide_before,
            )
            .limit(query.per_page or 100)
            .all()
        )

        joins = list()

        for join in raw_joins:
            joins.append(CassandraHandler.joiner_base_from_entity(join))

        return joins

    def get_group_join_for_user(self, group_id: str, joiner_id: int) -> JoinerBase:
        raw_join = (
            JoinerModel.objects(
                JoinerModel.group_id == group_id,
                JoinerModel.joined_id == joiner_id,
            )
            .first()
        )

        return CassandraHandler.joiner_base_from_entity(raw_join)

    def get_messages_in_group_for_user(self, group_id: str, user_id: int, query: MessageQuery) -> List[MessageBase]:
        until = MessageQuery.to_dt(query.until)

        # TODO: add message_type and status filter from MessageQuery
        raw_messages = (
            MessageModel.objects(
                MessageModel.group_id == group_id,
                MessageModel.user_id == user_id,
                MessageModel.created_at <= until,
            )
            .limit(query.per_page or 100)
            .all()
        )

        messages = list()

        for message in raw_messages:
            messages.append(CassandraHandler.message_base_from_entity(message))

        return messages

    def save_group_join_request(self, group_id: str, query: GroupJoinQuery) -> JoinerBase:
        created_at = dt.utcnow()
        created_at = created_at.replace(tzinfo=pytz.UTC)

        joiner = JoinerModel.create(
            group_id=group_id,
            inviter_id=query.inviter_id,
            joiner_id=query.joiner_id,
            created_at=created_at,
            invitation_context=query.invitation_context,
            status=0,  # TODO: need to specify in query? or not required?
        )

        return joiner

    def store_message(self, group_id: str, user_id: int, query: SendMessageQuery):
        created_at = dt.utcnow()
        created_at = created_at.replace(tzinfo=pytz.UTC)
        message_id = uuid()

        message = MessageModel.create(
            group_id=group_id,
            user_id=user_id,
            created_at=created_at,
            message_id=message_id,
            message_payload=query.message_payload,
            message_type=query.message_type,
        )

        return CassandraHandler.message_base_from_entity(message)

    def get_action_log_in_group(self, group_id: str, query: MessageQuery):
        until = MessageQuery.to_dt(query.until)

        return (
            ActionLogModel.objects(
                ActionLogModel.group_id == group_id,
                ActionLogModel.created_at <= until,
            )
            .limit(query.per_page or 100)
            .all()
        )

    def get_joiners_in_group(self, group_id: str, query: GroupJoinerQuery):
        until = GroupJoinerQuery.to_dt(query.until)

        return (
            ActionLogModel.objects(
                JoinerModel.group_id == group_id,
                JoinerModel.created_at <= until,
            )
            .filter(JoinerModel.status == query.status)
            .limit(query.per_page or 100)
            .all()
        )


    @staticmethod
    def message_base_from_entity(message: MessageModel) -> MessageBase:
        return MessageBase(
            group_id=str(message.group_id),
            created_at=message.created_at,
            user_id=message.user_id,
            message_id=str(message.message_id),
            message_payload=message.message_payload,
            status=message.status,
            message_type=message.message_type,
            updated_at=message.updated_at,
            removed_at=message.removed_at,
            removed_by_user=message.removed_by_user,
            last_action_log_id=message.last_action_log_id,
        )

    @staticmethod
    def joiner_base_from_entity(join: JoinerModel) -> JoinerBase:
        return JoinerBase(
            group_id=str(join.group_id),
            created_at=join.created_at,
            inviter_id=join.inviter_id,
            joined_id=join.joined_id,
            status=join.status,
            invitation_context=join.invitation_context,
        )