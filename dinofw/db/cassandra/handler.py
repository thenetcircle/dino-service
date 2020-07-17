from datetime import datetime as dt
from time import time
from typing import List, Optional
from uuid import uuid4 as uuid

import pytz
from cassandra.cqlengine import connection
from cassandra.cqlengine.query import BatchQuery
from cassandra.cqlengine.management import sync_table
from gnenv.environ import GNEnvironment

from dinofw.config import ConfigKeys
from dinofw.db.cassandra.models import ActionLogModel
from dinofw.db.cassandra.models import JoinerModel
from dinofw.db.cassandra.models import MessageModel
from dinofw.db.cassandra.schemas import JoinerBase
from dinofw.db.cassandra.schemas import MessageBase
from dinofw.rest.models import GroupJoinQuery, EditMessageQuery, AdminQuery, JoinerUpdateQuery
from dinofw.rest.models import GroupJoinerQuery
from dinofw.rest.models import MessageQuery
from dinofw.rest.models import SendMessageQuery

import logging


class CassandraHandler:
    def __init__(self, env: GNEnvironment):
        self.env = env
        self.logger = logging.getLogger(__name__)

        # used when no `hide_before` is specified in a query
        beginning_of_1995 = 789_000_000
        self.long_ago = dt.utcfromtimestamp(beginning_of_1995)

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
        hide_before = MessageQuery.to_dt(query.hide_before, default=self.long_ago)

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
        hide_before = MessageQuery.to_dt(query.hide_before, default=self.long_ago)

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
                JoinerModel.joiner_id == joiner_id,
            )
            .first()
        )

        return CassandraHandler.joiner_base_from_entity(raw_join)

    def delete_join_request(self, group_id: str, joiner_id: int) -> None:
        # TODO: test if we need to fetch first and delete later or if we can do it in the same query
        _ = (
            JoinerModel.objects(
                JoinerModel.group_id == group_id,
                JoinerModel.joiner_id == joiner_id,
            )
            .first()
            .delete()
        )

    def update_join_request(self, group_id: str, joiner_id: int, query: JoinerUpdateQuery) -> Optional[JoinerBase]:
        raw_join = (
            JoinerModel.objects(
                JoinerModel.group_id == group_id,
                JoinerModel.joiner_id == joiner_id,
            )
            .first()
        )

        if raw_join is None:
            return None

        raw_join.update(
            status=query.status or raw_join.status
        )

        return CassandraHandler.joiner_base_from_entity(raw_join)

    def get_messages_in_group_for_user(self, group_id: str, user_id: int, query: MessageQuery) -> List[MessageBase]:
        until = MessageQuery.to_dt(query.until)
        hide_before = MessageQuery.to_dt(query.hide_before, default=self.long_ago)

        # TODO: add message_type and status filter from MessageQuery
        raw_messages = (
            MessageModel.objects(
                MessageModel.group_id == group_id,
                MessageModel.user_id == user_id,
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

    def count_messages_in_group(self, group_id: str) -> int:
        # TODO: cache for a while if more than X messages? maybe TTL proportional to the amount
        # TODO: count all or only after `hide_before`?

        return (
            MessageModel.objects(
                MessageModel.group_id == group_id
            )
            .limit(None)
            .count()
        )

    def count_messages_in_group_since(self, group_id: str, since: dt) -> int:
        # TODO: count all or only after `hide_before`?
        return (
            MessageModel.objects(
                MessageModel.group_id == group_id,
                MessageModel.created_at > since,
            )
            .limit(None)
            .count()
        )

    def get_message(self, group_id: str, user_id: int, message_id: str) -> MessageBase:
        # TODO: use `hide_before` here?

        message = (
            MessageModel.objects(
                MessageModel.group_id == group_id,
                MessageModel.user_id == user_id,
                MessageModel.message_id == message_id,
            )
            .allow_filtering()
            .first()
        )

        return CassandraHandler.message_base_from_entity(message)

    def delete_message(self, group_id: str, user_id: int, message_id: str, query: MessageQuery) -> None:
        message = (
            MessageModel.objects(
                MessageModel.group_id == group_id,
                MessageModel.user_id == user_id,
                MessageModel.message_id == message_id,
            )
            .allow_filtering()
            .first()
        )

        if message is None:
            self.logger.warning(f"no message found for user {user_id}, group {group_id}, message {message_id}")
            return

        removed_at = dt.utcnow()
        removed_at = removed_at.replace(tzinfo=pytz.UTC)

        message.update(
            message_payload=None,
            status=query.status or message.status,
            removed_by_user=query.admin_id,
            removed_at=removed_at,
        )

    def edit_message(self, group_id: str, user_id: int, message_id: str, query: EditMessageQuery) -> None:
        message = (
            MessageModel.objects(
                MessageModel.group_id == group_id,
                MessageModel.user_id == user_id,
                MessageModel.message_id == message_id,
            )
            .allow_filtering()
            .first()
        )

        if message is None:
            self.logger.warning(f"no message found for user {user_id}, group {group_id}, message {message_id}")
            return

        message.update(
            message_payload=query.message_payload or message.message_payload,
            status=query.status or message.status,
        )

    def update_messages_in_group(self, group_id: str, query: MessageQuery) -> None:
        def callback(message: MessageModel) -> None:
            message.status = query.status

        self._update_all_messages_in_group(
            group_id=group_id,
            callback=callback
        )

    def update_messages_in_group_for_user(self, group_id: str, user_id: int, query: MessageQuery) -> None:
        def callback(message: MessageModel) -> None:
            message.status = query.status

        self._update_all_messages_in_group(
            group_id=group_id,
            callback=callback,
            user_id=user_id,
        )

    def delete_messages_in_group(self, group_id: str, query: MessageQuery) -> None:
        def callback(message: MessageModel):
            # TODO: is there a status they should use in the query for deletions?
            message.status = query.status

            # TODO: needed?
            message.message_type = query.message_type

            message.removed_at = removed_at
            message.removed_by_user = query.admin_id

        removed_at = dt.utcnow()
        removed_at = removed_at.replace(tzinfo=pytz.UTC)

        self._update_all_messages_in_group(
            group_id=group_id,
            callback=callback
        )

    def delete_messages_in_group_for_user(self, group_id: str, user_id: int, query: MessageQuery) -> None:
        def callback(message: MessageModel):
            # TODO: is there a status they should use in the query for deletions?
            message.status = query.status

            # TODO: needed?
            message.message_type = query.message_type

            message.removed_at = removed_at
            message.removed_by_user = query.admin_id

        removed_at = dt.utcnow()
        removed_at = removed_at.replace(tzinfo=pytz.UTC)

        self._update_all_messages_in_group(
            group_id=group_id,
            callback=callback,
            user_id=user_id,
        )

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
        hide_before = MessageQuery.to_dt(query.hide_before, default=self.long_ago)

        return (
            ActionLogModel.objects(
                ActionLogModel.group_id == group_id,
                ActionLogModel.created_at <= until,
                ActionLogModel.created_at > hide_before,
            )
            .limit(query.per_page or 100)
            .all()
        )

    def get_joiners_in_group(self, group_id: str, query: GroupJoinerQuery):
        until = GroupJoinerQuery.to_dt(query.until)
        hide_before = MessageQuery.to_dt(query.hide_before, default=self.long_ago)

        return (
            ActionLogModel.objects(
                JoinerModel.group_id == group_id,
                JoinerModel.created_at <= until,
                JoinerModel.created_at > hide_before,
            )
            .filter(JoinerModel.status == query.status)
            .limit(query.per_page or 100)
            .all()
        )

    def _update_all_messages_in_group(self, group_id: str, callback: callable, user_id: int = None):
        until = dt.utcnow()
        until = until.replace(tzinfo=pytz.UTC)
        start = time()
        amount = 0

        while True:
            messages = self._get_batch_of_messages_in_group(
                group_id=group_id,
                until=until,
                user_id=user_id,
            )

            if not len(messages):
                end = time()
                elapsed = (end - start) / 1000
                self.logger.info(f"finished batch updating {amount} messages in group {group_id} after {elapsed:.2f}s")
                break

            amount += len(messages)
            until = self._update_messages(messages, callback)

    def _update_messages(self, messages: List[MessageModel], callback: callable) -> Optional[dt]:
        until = None

        with BatchQuery() as b:
            for message in messages:
                callback(message)
                message.batch(b).save()

                until = message.created_at

        return until

    def _get_batch_of_messages_in_group(self, group_id: str, until: dt, user_id: int = None) -> List[MessageModel]:
        if user_id is None:
            return (
                MessageModel.objects(
                    MessageModel.group_id == group_id,
                    MessageModel.created_at < until,
                )
                .limit(500)
                .all()
            )
        else:
            return (
                MessageModel.objects(
                    MessageModel.group_id == group_id,
                    MessageModel.user_id == user_id,
                    MessageModel.created_at < until,
                )
                .limit(500)
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
            joiner_id=join.joiner_id,
            status=join.status,
            invitation_context=join.invitation_context,
        )
