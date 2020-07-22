from datetime import datetime as dt
from time import time
from typing import List, Optional, Dict
from uuid import uuid4 as uuid

import pytz
from cassandra.cqlengine import connection
from cassandra.cqlengine.query import BatchQuery
from cassandra.cqlengine.management import sync_table
from gnenv.environ import GNEnvironment

from dinofw.config import ConfigKeys
from dinofw.db.cassandra.models import ActionLogModel
from dinofw.db.cassandra.models import MessageModel
from dinofw.db.cassandra.schemas import ActionLogBase
from dinofw.db.cassandra.schemas import MessageBase
from dinofw.rest.server.models import EditMessageQuery
from dinofw.rest.server.models import AdminQuery
from dinofw.rest.server.models import MessageQuery
from dinofw.rest.server.models import SendMessageQuery

import logging


class CassandraHandler:
    ACTION_TYPE_JOIN = 0
    ACTION_TYPE_LEAVE = 1

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
            hosts, default_keyspace=key_space, protocol_version=3, retry_connect=True
        )

        sync_table(MessageModel)
        sync_table(ActionLogModel)

    def get_messages_in_group(
        self, group_id: str, query: MessageQuery
    ) -> List[MessageBase]:
        until = MessageQuery.to_dt(query.until)

        # TODO: get default hide_before from user stats in db/cache if not in query
        # TODO: we don't know which user it is for this api
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

    def get_messages_in_group_for_user(
        self, group_id: str, user_id: int, query: MessageQuery
    ) -> List[MessageBase]:
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
            MessageModel.objects(MessageModel.group_id == group_id).limit(None).count()
        )

    def count_messages_in_group_since(self, group_id: str, since: dt) -> int:
        # TODO: count all or only after `hide_before`?
        return (
            MessageModel.objects(
                MessageModel.group_id == group_id, MessageModel.created_at > since,
            )
            .limit(None)
            .count()
        )

    def get_message(
        self, group_id: str, user_id: int, message_id: str
    ) -> Optional[MessageBase]:
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

        if message is None:
            return None

        return CassandraHandler.message_base_from_entity(message)

    def delete_message(
        self, group_id: str, user_id: int, message_id: str, query: AdminQuery
    ) -> None:
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
            self.logger.warning(
                f"no message found for user {user_id}, group {group_id}, message {message_id}"
            )
            return

        removed_at = dt.utcnow()
        removed_at = removed_at.replace(tzinfo=pytz.UTC)

        message.update(
            message_payload="-",  # TODO: allow None values in cassanrda tbale
            removed_by_user=query.admin_id,
            removed_at=removed_at,
        )

    def edit_message(
        self, group_id: str, user_id: int, message_id: str, query: EditMessageQuery
    ) -> Optional[MessageBase]:
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
            self.logger.warning(
                f"no message found for user {user_id}, group {group_id}, message {message_id}"
            )
            return None

        now = dt.utcnow()
        now = now.replace(tzinfo=pytz.UTC)

        message.update(
            message_payload=query.message_payload or message.message_payload,
            status=query.status or message.status,
            updated_at=now
        )

        return CassandraHandler.message_base_from_entity(message)

    def update_messages_in_group(self, group_id: str, query: MessageQuery) -> None:
        def callback(message: MessageModel) -> None:
            message.status = query.status

        self._update_all_messages_in_group(group_id=group_id, callback=callback)

    def update_messages_in_group_for_user(
        self, group_id: str, user_id: int, query: MessageQuery
    ) -> None:
        def callback(message: MessageModel) -> None:
            message.status = query.status

        self._update_all_messages_in_group(
            group_id=group_id, callback=callback, user_id=user_id,
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

        self._update_all_messages_in_group(group_id=group_id, callback=callback)

    def create_join_action_log(
        self, group_id: str, users: Dict[int, float], action_time: dt
    ) -> List[ActionLogBase]:
        user_ids = [user_id for user_id, _ in users.items()]
        return self._create_action_log(
            group_id, user_ids, action_time, CassandraHandler.ACTION_TYPE_JOIN
        )

    def create_leave_action_log(
        self, group_id: str, user_ids: [int], action_time: dt
    ) -> List[ActionLogBase]:
        return self._create_action_log(
            group_id, user_ids, action_time, CassandraHandler.ACTION_TYPE_LEAVE
        )

    def _create_action_log(
        self, group_id: str, user_ids: List[int], action_time: dt, action_type: int
    ) -> List[ActionLogBase]:
        logs = list()

        for user_id in user_ids:
            log = ActionLogModel.create(
                group_id=group_id,
                user_id=user_id,
                created_at=action_time,
                action_type=action_type,
                action_id=uuid(),
            )

            logs.append(CassandraHandler.action_log_base_from_entity(log))

        return logs

    def delete_messages_in_group_for_user(
        self, group_id: str, user_id: int, query: MessageQuery
    ) -> None:
        # TODO: copy messages to another table `messages_deleted` and then remove the rows for `messages`

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
            group_id=group_id, callback=callback, user_id=user_id,
        )

    def store_message(self, group_id: str, user_id: int, query: SendMessageQuery) -> MessageBase:
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

    def get_action_log_in_group(self, group_id: str, query: MessageQuery) -> List[ActionLogBase]:
        until = MessageQuery.to_dt(query.until)
        hide_before = MessageQuery.to_dt(query.hide_before, default=self.long_ago)

        action_logs = (
            ActionLogModel.objects(
                ActionLogModel.group_id == group_id,
                ActionLogModel.created_at <= until,
                ActionLogModel.created_at > hide_before,
            )
            .limit(query.per_page or 100)
            .all()
        )

        return [
            CassandraHandler.action_log_base_from_entity(log) for log in action_logs
        ]

    def _update_all_messages_in_group(
        self, group_id: str, callback: callable, user_id: int = None
    ):
        until = dt.utcnow()
        until = until.replace(tzinfo=pytz.UTC)
        start = time()
        amount = 0

        while True:
            messages = self._get_batch_of_messages_in_group(
                group_id=group_id, until=until, user_id=user_id,
            )

            if not len(messages):
                end = time()
                elapsed = (end - start) / 1000
                self.logger.info(
                    f"finished batch updating {amount} messages in group {group_id} after {elapsed:.2f}s"
                )
                break

            amount += len(messages)
            until = self._update_messages(messages, callback)

    def _update_messages(
        self, messages: List[MessageModel], callback: callable
    ) -> Optional[dt]:
        until = None

        with BatchQuery() as b:
            for message in messages:
                callback(message)
                message.batch(b).save()

                until = message.created_at

        return until

    def _get_batch_of_messages_in_group(
        self, group_id: str, until: dt, user_id: int = None
    ) -> List[MessageModel]:
        if user_id is None:
            return (
                MessageModel.objects(
                    MessageModel.group_id == group_id, MessageModel.created_at < until,
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
    def action_log_base_from_entity(log: ActionLogModel) -> ActionLogBase:
        return ActionLogBase(
            group_id=str(log.group_id),
            created_at=log.created_at,
            user_id=log.user_id,
            action_id=str(log.action_id),
            action_type=log.action_type,
            admin_id=log.admin_id,
            message_id=str(log.message_id) if log.message_id is not None else None,
        )
