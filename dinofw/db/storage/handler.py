import logging
from datetime import datetime as dt
from time import time
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from uuid import uuid4 as uuid

import arrow
from cassandra.cluster import EXEC_PROFILE_DEFAULT
from cassandra.cluster import ExecutionProfile
from cassandra.cluster import PlainTextAuthProvider
from cassandra.cluster import Session
from cassandra.connection import ConsistencyLevel
from cassandra.cqlengine import connection
from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine.query import BatchQuery
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.policies import RetryPolicy
from cassandra.policies import TokenAwarePolicy

from dinofw.db.rdbms.schemas import UserGroupStatsBase
from dinofw.db.storage.models import AttachmentModel
from dinofw.db.storage.models import MessageModel
from dinofw.db.storage.schemas import MessageBase
from dinofw.rest.models import AttachmentQuery, ActionLogQuery
from dinofw.rest.models import CreateActionLogQuery
from dinofw.rest.models import CreateAttachmentQuery
from dinofw.rest.models import MessageQuery
from dinofw.rest.models import SendMessageQuery
from dinofw.utils import utcnow_dt
from dinofw.utils.config import ConfigKeys
from dinofw.utils.config import DefaultValues
from dinofw.utils.config import MessageTypes
from dinofw.utils.exceptions import NoSuchAttachmentException
from dinofw.utils.exceptions import NoSuchMessageException


class CassandraHandler:
    def __init__(self, env):
        self.env = env
        self.logger = logging.getLogger(__name__)

        # used when no `hide_before` is specified in a query
        beginning_of_1995 = 789_000_000
        self.long_ago = dt.utcfromtimestamp(beginning_of_1995)

    def setup_tables(self):
        key_space = self.env.config.get(ConfigKeys.KEY_SPACE, domain=ConfigKeys.STORAGE)
        hosts = self.env.config.get(ConfigKeys.HOST, domain=ConfigKeys.STORAGE)
        hosts = hosts.split(",")

        # required to specify execution profiles in future versions
        profiles = {
            # override the default so we can set consistency level later
            EXEC_PROFILE_DEFAULT: ExecutionProfile(
                load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy()),
                retry_policy=RetryPolicy(),
                request_timeout=10.0,
                row_factory=Session._row_factory,  # noqa
                # should probably be changed to QUORUM when having more than 3 nodes in the cluster
                consistency_level=ConsistencyLevel.LOCAL_ONE,
            ),
            # TODO: there doesn't seem to be a way to specify execution profile when
            #  using the library's object mapping approach, only when writing pure
            #  cql queries:
            #  https://docs.datastax.com/en/developer/python-driver/3.24/execution_profiles/
            # batch profile has longer timeout since they are run async anyway
            "batch": ExecutionProfile(
                load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy()),
                request_timeout=120.0,
                consistency_level=ConsistencyLevel.LOCAL_ONE,
            )
        }

        kwargs = {
            "default_keyspace": key_space,
            "protocol_version": 3,
            "retry_connect": True,
            "execution_profiles": profiles,
        }

        username = self._get_from_conf(ConfigKeys.USER, ConfigKeys.STORAGE)
        password = self._get_from_conf(ConfigKeys.PASSWORD, ConfigKeys.STORAGE)

        if password is not None:
            auth_provider = PlainTextAuthProvider(
                username=username,
                password=password,
            )
            kwargs["auth_provider"] = auth_provider

        connection.setup(hosts, **kwargs)

        sync_table(MessageModel)
        sync_table(AttachmentModel)

    def _get_from_conf(self, key, domain):
        if key not in self.env.config.get(domain):
            return None

        value = self.env.config.get(key, domain=domain)
        if value is None or not len(value.strip()):
            return None

        return value

    # noinspection PyMethodMayBeStatic
    def get_messages_in_group(
        self,
        group_id: str,
        query: MessageQuery
    ) -> List[MessageBase]:
        until = MessageQuery.to_dt(query.until)

        raw_messages = (
            MessageModel.objects(
                MessageModel.group_id == group_id,
                MessageModel.created_at < until,
            )
            .limit(query.per_page or DefaultValues.PER_PAGE)
            .all()
        )

        messages = list()

        for message in raw_messages:
            messages.append(CassandraHandler.message_base_from_entity(message))

        return messages

    # noinspection PyMethodMayBeStatic
    def get_attachments_in_group_for_user(
            self,
            group_id: str,
            user_stats: UserGroupStatsBase,
            query: MessageQuery
    ) -> List[MessageBase]:
        until = MessageQuery.to_dt(query.until)

        raw_attachments = (
            AttachmentModel.objects(
                AttachmentModel.group_id == group_id,
                AttachmentModel.created_at <= until,
                AttachmentModel.created_at > user_stats.delete_before,
            )
            .limit(query.per_page or DefaultValues.PER_PAGE)
            .all()
        )

        attachments = list()

        for attachment in raw_attachments:
            attachments.append(CassandraHandler.message_base_from_entity(attachment))

        return attachments

    # noinspection PyMethodMayBeStatic
    def get_messages_in_group_for_user(
            self,
            group_id: str,
            user_stats: UserGroupStatsBase,
            query: MessageQuery
    ) -> List[MessageBase]:
        until = MessageQuery.to_dt(query.until)

        raw_messages = (
            MessageModel.objects(
                MessageModel.group_id == group_id,
                MessageModel.created_at < until,
                MessageModel.created_at > user_stats.delete_before,
            )
            .limit(query.per_page or DefaultValues.PER_PAGE)
            .all()
        )

        messages = list()

        for message in raw_messages:
            messages.append(CassandraHandler.message_base_from_entity(message))

        return messages

    # noinspection PyMethodMayBeStatic
    def count_messages_in_group_since(self, group_id: str, since: dt) -> int:
        return (
            MessageModel.objects(
                MessageModel.group_id == group_id,
            )
            .filter(
                MessageModel.created_at > since,
            )
            .limit(None)
            .count()
        )

    def get_unread_in_group(self, group_id: str, user_id: int, last_read: dt) -> int:
        unread = self.env.cache.get_unread_in_group(group_id, user_id)
        if unread is not None:
            return unread

        unread = self.count_messages_in_group_since(group_id, last_read)

        self.env.cache.set_unread_in_group(group_id, user_id, unread)
        return unread

    def delete_attachments_in_all_groups(
        self,
        group_created_at: List[Tuple[str, dt]],
        user_id: int
    ) -> Dict[str, List[MessageBase]]:
        group_to_atts = dict()
        start = time()

        for group_id, created_at in group_created_at:
            attachments = self.delete_attachments(group_id, created_at, user_id)

            if len(attachments):
                group_to_atts[group_id] = attachments

        elapsed = time() - start
        if elapsed > 5:
            n = len(group_to_atts)
            self.logger.info(
                f"batch delete attachments in {n} groups for user {user_id} took {elapsed:.2f}s"
            )

        return group_to_atts

    def delete_messages_in_group_before(self, group_id: str, before: dt):
        messages = (
            MessageModel.objects(
                MessageModel.group_id == group_id,
                MessageModel.created_at <= before,
            )
            .all()
        )

        self.logger.info(f"deleting {len(messages)} messages in group {group_id}...")
        self._delete_messages(messages, "messages")

    def delete_attachments_in_group_before(self, group_id: str, before: dt):
        attachments = (
            AttachmentModel.objects(
                AttachmentModel.group_id == group_id,
                AttachmentModel.created_at <= before,
            )
            .all()
        )

        self.logger.info(f"deleting {len(attachments)} attachments in group {group_id}...")
        self._delete_messages(attachments, "attachments")

    def delete_attachments(
        self,
        group_id: str,
        group_created_at: dt,
        user_id: int
    ) -> List[MessageBase]:
        attachments = (
            AttachmentModel.objects(
                AttachmentModel.group_id == group_id,
                AttachmentModel.created_at > group_created_at,
                AttachmentModel.user_id == user_id,
            )
            .allow_filtering()
            .all()
        )

        file_ids = {attachment.file_id for attachment in attachments}
        attachment_msg_ids = {attachment.message_id for attachment in attachments}
        attachment_bases = [
            CassandraHandler.message_base_from_entity(attachment) for attachment in attachments
        ]

        # no un-deleted attachments in this group
        if not len(file_ids):
            return list()

        messages_all = (
            MessageModel.objects(
                MessageModel.group_id == group_id,
                MessageModel.created_at > group_created_at,
                MessageModel.user_id == user_id,
                MessageModel.message_type == MessageTypes.IMAGE,  # TODO: need to have type in query? image/video/etc.
            )
            .allow_filtering()
            .all()
        )
        messages = [
            message for message in messages_all
            if message.message_id in attachment_msg_ids
        ]

        self._delete_messages(messages, "messages")
        self._delete_messages(attachments, "attachments")

        return attachment_bases

    def delete_attachment(
        self,
        group_id: str,
        group_created_at: dt,
        query: AttachmentQuery
    ) -> MessageBase:
        attachment = (
            AttachmentModel.objects(
                AttachmentModel.group_id == group_id,
                AttachmentModel.created_at > group_created_at,
                AttachmentModel.file_id == query.file_id,
            )
            .allow_filtering()
            .first()
        )

        if attachment is None:
            raise NoSuchAttachmentException(query.file_id)

        # to be returned
        attachment_base = CassandraHandler.message_base_from_entity(attachment)

        # convert uuid to str
        message_id = str(attachment.message_id)

        self.delete_message(
            group_id,
            attachment.user_id,
            message_id,
            attachment.created_at
        )

        # delete attachment after message; delete_message() throws NoSuchMessage if not found
        attachment.delete()

        return attachment_base

    # noinspection PyMethodMayBeStatic
    def delete_message(
        self, group_id: str, user_id: int, message_id: str, created_at: dt
    ) -> None:
        approx_date = arrow.get(created_at).shift(minutes=-1).datetime

        message = (
            MessageModel.objects(
                MessageModel.group_id == group_id,
                MessageModel.created_at > approx_date,
                MessageModel.user_id == user_id,
                MessageModel.message_id == message_id,
            )
            .allow_filtering()
            .first()
        )

        if message is None:
            raise NoSuchMessageException(message_id)

        removed_at = utcnow_dt()

        message.update(
            message_payload="",
            removed_at=removed_at,
            updated_at=removed_at,
        )

    def get_message_with_id(self, group_id: str, user_id: int, message_id: str, created_at: float):
        approx_after = arrow.get(created_at).shift(minutes=-1).datetime
        approx_before = arrow.get(created_at).shift(minutes=1).datetime

        message = (
            MessageModel.objects(
                MessageModel.group_id == group_id,
                MessageModel.user_id == user_id,
                MessageModel.created_at > approx_after,
                MessageModel.created_at > approx_before,
                MessageModel.message_id == message_id,
            )
            .allow_filtering()
            .first()
        )

        if message is None:
            raise NoSuchAttachmentException(message_id)

        return CassandraHandler.message_base_from_entity(message)

    # noinspection PyMethodMayBeStatic
    def get_attachment_from_file_id(self, group_id: str, created_at: dt, query: AttachmentQuery) -> MessageBase:
        approx_date = arrow.get(created_at).shift(minutes=-1).datetime

        attachment = (
            AttachmentModel.objects(
                AttachmentModel.group_id == group_id,
                AttachmentModel.created_at > approx_date,
                AttachmentModel.file_id == query.file_id,
            )
            .allow_filtering()
            .first()
        )

        if attachment is None:
            raise NoSuchAttachmentException(query.file_id)

        return CassandraHandler.message_base_from_entity(attachment)

    # noinspection PyMethodMayBeStatic
    def store_attachment(
            self, group_id: str, user_id: int, message_id: str, query: CreateAttachmentQuery
    ) -> MessageBase:
        # we should filter on the 'created_at' field, since it's a clustering key
        # and 'message_id' is not; if we don't filter by 'created_at' each edit
        # will require a full table scan, and editing a recent message happens
        # quite often, which will become very slow after a group has a long
        # message history...
        created_at = query.created_at
        now = utcnow_dt()

        # querying by exact datetime seems to be shifty in cassandra, so just
        # filter by a minute before and after
        approx_date_after = arrow.get(created_at).shift(minutes=-1).datetime
        approx_date_before = arrow.get(created_at).shift(minutes=1).datetime

        message = (
            MessageModel.objects(
                MessageModel.group_id == group_id,
                MessageModel.user_id == user_id,
                MessageModel.created_at > approx_date_after,
                MessageModel.created_at < approx_date_before,
                MessageModel.message_id == message_id,
            )
            .allow_filtering()
            .first()
        )

        if message is None:
            raise NoSuchMessageException(message_id)

        message.update(
            message_payload=query.message_payload,
            file_id=query.file_id,
            updated_at=now,
        )

        AttachmentModel.create(
            group_id=group_id,
            user_id=user_id,
            created_at=message.created_at,
            message_id=message_id,
            message_payload=query.message_payload,
            message_type=message.message_type,
            updated_at=now,
            file_id=query.file_id,
        )

        return CassandraHandler.message_base_from_entity(message)

    def delete_messages_in_group(self, group_id: str, query: MessageQuery) -> None:
        def callback(message: MessageModel):
            message.removed_at = removed_at
            message.removed_by_user = query.admin_id

        removed_at = utcnow_dt()

        self._update_all_messages_in_group(group_id=group_id, callback=callback)

    # noinspection PyMethodMayBeStatic
    def create_action_log(
            self,
            user_id: int,
            group_id: str,
            query: ActionLogQuery
    ) -> MessageBase:
        action_time = utcnow_dt()

        log = MessageModel.create(
            group_id=group_id,
            user_id=user_id,
            created_at=action_time,
            message_type=MessageTypes.ACTION,
            message_payload=query.payload,
            message_id=uuid(),
        )

        return CassandraHandler.message_base_from_entity(log)

    def delete_messages_in_group_for_user(
        self, group_id: str, user_id: int, query: MessageQuery
    ) -> None:
        # TODO: copy messages to another table `messages_deleted` and then remove the rows for `messages`

        def callback(message: MessageModel):
            message.removed_at = removed_at
            message.removed_by_user = query.admin_id

        removed_at = utcnow_dt()

        self._update_all_messages_in_group(
            group_id=group_id, callback=callback, user_id=user_id,
        )

    # noinspection PyMethodMayBeStatic
    def store_message(self, group_id: str, user_id: int, query: SendMessageQuery) -> MessageBase:
        created_at = utcnow_dt()
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

    def _update_all_messages_in_group(
        self, group_id: str, callback: callable, user_id: int = None
    ):
        until = utcnow_dt()
        start = time()
        amount = 0

        while True:
            messages = self._get_batch_of_messages_in_group(
                group_id=group_id, until=until, user_id=user_id,
            )

            if not len(messages):
                elapsed = time() - start

                if elapsed > 5 or amount > 500:
                    self.logger.info(
                        f"finished batch updating {amount} messages in group {group_id} after {elapsed:.2f}s"
                    )
                break

            amount += len(messages)
            until = self._update_messages(messages, callback)

    def _delete_messages(self, messages, types: str) -> None:
        start = time()
        with BatchQuery() as b:
            for message in messages:
                message.batch(b).delete()

        elapsed = time() - start
        if elapsed > 1:
            self.logger.info(f"batch deleted {len(message)} {types} in {elapsed:.2f}s")

    # noinspection PyMethodMayBeStatic
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

    # noinspection PyMethodMayBeStatic
    def _get_batch_of_messages_in_group(
        self, group_id: str, until: dt, user_id: int = None
    ) -> List[MessageModel]:
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
            message_type=message.message_type,
            updated_at=message.updated_at,
            file_id=message.file_id,
        )
