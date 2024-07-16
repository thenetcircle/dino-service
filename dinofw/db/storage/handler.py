import json
import sys
from datetime import datetime as dt
from datetime import timedelta
from time import time
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from uuid import UUID
from uuid import uuid4 as uuid

import arrow
from cassandra.cluster import Cluster
from cassandra.cluster import EXEC_PROFILE_DEFAULT
from cassandra.cluster import ExecutionProfile
from cassandra.cluster import PlainTextAuthProvider
from cassandra.cluster import Session
from cassandra.connection import ConsistencyLevel
from cassandra.cqlengine import connection
from cassandra.cqlengine.query import BatchQuery
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.policies import RetryPolicy
from cassandra.policies import TokenAwarePolicy
from loguru import logger

from dinofw.db.rdbms.schemas import UserGroupStatsBase
from dinofw.db.storage.models import AttachmentModel
from dinofw.db.storage.models import MessageModel
from dinofw.db.storage.schemas import MessageBase
from dinofw.rest.queries import ActionLogQuery, AdminQuery
from dinofw.rest.queries import AttachmentQuery
from dinofw.rest.queries import CreateAttachmentQuery
from dinofw.rest.queries import DeleteAttachmentQuery
from dinofw.rest.queries import EditMessageQuery
from dinofw.rest.queries import MessageQuery
from dinofw.rest.queries import SendMessageQuery
from dinofw.utils import to_dt, is_non_zero, is_none_or_zero, max_one_year_ago, one_year_ago
from dinofw.utils import utcnow_dt
from dinofw.utils.config import ConfigKeys
from dinofw.utils.config import DefaultValues
from dinofw.utils.config import MessageTypes
from dinofw.utils.config import PayloadStatus
from dinofw.utils.exceptions import NoSuchAttachmentException
from dinofw.utils.exceptions import NoSuchMessageException


class CassandraHandler:
    def __init__(self, env):
        self.env = env

        # only used for transactions (LWT) when creating image messages
        self.session = None

        # used when no `hide_before` is specified in a query
        beginning_of_1995 = 789_000_000
        self.long_ago = arrow.get(beginning_of_1995).datetime

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
            # batch profile has longer timeout since they are run async anyway
            "batch": ExecutionProfile(
                load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy()),
                request_timeout=120.0,
                consistency_level=ConsistencyLevel.LOCAL_ONE,
            ),
            "transaction": ExecutionProfile(
                load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy()),
                request_timeout=10.0,
                consistency_level=ConsistencyLevel.QUORUM,
                # https://docs.datastax.com/en/developer/python-driver/3.25/api/cassandra/query/#cassandra.query.Statement.serial_consistency_level
                serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL
            )
        }

        kwargs = {
            "default_keyspace": key_space,
            "protocol_version": 3,
            "retry_connect": True,
            "execution_profiles": profiles,
            "auth_provider": None
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

        cluster = Cluster(
            contact_points=hosts,
            protocol_version=3,
            execution_profiles=profiles,
            auth_provider=kwargs["auth_provider"]
        )

        # used for serial consistency level when inserting images with "if not exists"
        self.session = cluster.connect(key_space)

        # from cassandra.cqlengine.management import sync_table
        # sync_table(MessageModel)
        # sync_table(AttachmentModel)

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
        until = to_dt(query.until)

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
        until = to_dt(query.until, allow_none=True)
        since = to_dt(query.since, allow_none=True)

        statement = AttachmentModel.objects.filter(
            AttachmentModel.group_id == group_id
        )

        if until is not None:
            statement = statement.filter(
                AttachmentModel.created_at < until
            )
            statement = statement.filter(
                AttachmentModel.created_at > user_stats.delete_before
            )

        elif since is not None:
            if since < user_stats.delete_before:
                since = user_stats.delete_before

            statement = statement.filter(AttachmentModel.created_at > since)

            # default ordering is descending, so change to ascending when using 'since'
            statement = statement.order_by('created_at')

        messages = statement.limit(query.per_page or DefaultValues.PER_PAGE).all()
        messages = [
            CassandraHandler.message_base_from_entity(message)
            for message in messages
            # can't do "!=" in cassandra, so filter out audio messages here
            if message.message_type != MessageTypes.AUDIO
        ]

        if since is None:
            return messages

        # since we need ascending order on cassandra query if we use 'since', reverse the results here
        return list(reversed(messages))

    def _try_parse_messages(self, raw_messages: List[MessageModel]) -> List[MessageBase]:
        messages = list()

        for message in raw_messages:
            try:
                messages.append(CassandraHandler.message_base_from_entity(message))
            except Exception as e:
                logger.error(f"could not parse raw message: {str(e)}")
                logger.error(message.__dict__)
                logger.exception(e)
                self.env.capture_exception(sys.exc_info())

        return messages

    # noinspection PyMethodMayBeStatic
    def get_messages_in_group_only_from_user(
            self,
            group_id: str,
            user_stats: UserGroupStatsBase,
            query: MessageQuery
    ) -> List[MessageBase]:
        until = to_dt(query.until, allow_none=True)
        since = to_dt(query.since, allow_none=True)
        query_limit = query.per_page or DefaultValues.PER_PAGE

        batch_limit = query_limit * 10
        if batch_limit > 1000:
            batch_limit = 1000

        if since is None:
            since = user_stats.delete_before
            if is_non_zero(query.admin_id) and query.include_deleted:
                # limit to max 1 year ago for GDPR, scheduler will delete periodically, but don't show them here
                since = one_year_ago(since)

        # if not specified, use the last sent time (e.g. to get for first page results)
        if until is None:
            # until is not inclusive, so add 1 ms to include the last sent message
            until = user_stats.last_sent
            until += timedelta(milliseconds=1)

        raw_messages = self._get_messages_in_group_from_user(
            group_id,
            user_stats.user_id,
            until=until,
            since=since,
            limit=batch_limit,
            query_limit=query_limit
        )

        messages = self._try_parse_messages(raw_messages)
        return messages[:query_limit]

    def get_all_messages_in_group(self, group_id: str) -> List[MessageBase]:
        """
        internal api to get all history in a group for legal purposes
        """
        raw_messages = MessageModel.objects.filter(MessageModel.group_id == group_id).all()
        return self._try_parse_messages(raw_messages)

    # noinspection PyMethodMayBeStatic
    def get_messages_in_group_for_user(
            self,
            group_id: str,
            user_stats: UserGroupStatsBase,
            query: MessageQuery
    ) -> List[MessageBase]:
        until = to_dt(query.until, allow_none=True)
        since = to_dt(query.since, allow_none=True)

        statement = MessageModel.objects.filter(
            MessageModel.group_id == group_id
        )
        keep_order = True

        if until is not None:
            statement = statement.filter(
                MessageModel.created_at < until
            )

            creation_limit = user_stats.delete_before

            # only admins can see deleted messages
            if query and is_non_zero(query.admin_id) and query.include_deleted:
                # limit to max 1 year ago for GDPR, scheduler will delete periodically, but don't show them here
                creation_limit = one_year_ago(creation_limit)

            statement = statement.filter(
                MessageModel.created_at > creation_limit
            )

        elif since is not None:
            # only admins can see deleted messages
            if since < user_stats.delete_before:
                since = user_stats.delete_before

                if query and is_non_zero(query.admin_id) and query.include_deleted:
                    # limit to max 1 year ago for GDPR, scheduler will delete periodically, but don't show them here
                    since = one_year_ago(since)

            statement = statement.filter(MessageModel.created_at >= since)

            # default ordering is descending, so change to ascending when using 'since'
            statement = statement.order_by('created_at')
            keep_order = False

        raw_messages = statement.limit(query.per_page or DefaultValues.PER_PAGE).all()
        messages = self._try_parse_messages(raw_messages)

        # if since is None:
        if keep_order:
            return messages

        # since we need ascending order on cassandra query if we use 'since', reverse the results here
        return list(reversed(messages))

    def get_created_at_for_offset(self, group_id: str, offset: int):
        messages = (
            MessageModel.objects(
                MessageModel.group_id == group_id
            )
            .limit(offset)
            .all()
        )

        if not len(messages):
            return None

        return messages[-1].created_at

    # noinspection PyMethodMayBeStatic
    def count_messages_in_group_since(self, group_id: str, since: dt, query: AdminQuery = None) -> int:
        if query and is_non_zero(query.admin_id) and query.include_deleted:
            since = one_year_ago(since)

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

    # noinspection PyMethodMayBeStatic
    def count_attachments_in_group_since(self, group_id: str, since: dt, sender_id: int = -1) -> int:
        if sender_id > 0:
            attachments = (
                AttachmentModel.objects(
                    AttachmentModel.group_id == group_id
                )
                .filter(
                    AttachmentModel.created_at > since,
                )
                .limit(None)
                .all()
            )

            # can't filter on user_id above, since created_at is suing non-EQ relation
            return sum([1 for att in attachments if att.user_id == sender_id])

        return (
            AttachmentModel.objects(
                AttachmentModel.group_id == group_id,
            )
            .filter(
                AttachmentModel.created_at > since,
            )
            .limit(None)
            .count()
        )

    def _get_messages_in_group_from_user(
            self, group_id: str, user_id: int, until: dt, since: dt, limit: int = 1000, query_limit: int = -1
    ) -> List[MessageModel]:
        start = time()
        n_all_messages = 0
        messages_from_user = list()

        logger.info(f"cassandra count: group_id={group_id}, user_id={user_id}, until={until}, since={since}")

        while True:
            # until is not inclusive
            messages = self._get_batch_of_messages_in_group_since(
                group_id=group_id, until=until, since=since, limit=limit
            )
            logger.info(messages)

            for message in messages:
                if message.user_id == user_id:
                    messages_from_user.append(message)

            n_messages = len(messages)
            n_all_messages += n_messages

            logger.info(f"n_messages={n_messages}, n_all_messages={n_all_messages}")

            if not n_messages or n_messages < limit or len(messages_from_user) > query_limit > 0:
                elapsed = time() - start
                logger.info((
                    f"[{elapsed:.2f}s] fetched {n_all_messages} msgs"
                    f" in {group_id}; {len(messages_from_user)} messages were from user {user_id}"
                ))
                break
            else:
                until = messages[-1].created_at

        return messages_from_user

    # noinspection PyMethodMayBeStatic
    def count_messages_in_group_from_user_since(
            self, group_id: str, user_id: int, until: dt, since: dt, query: AdminQuery = None
    ) -> int:
        if query and is_non_zero(query.admin_id) and query.include_deleted:
            # limit to max 1 year ago for GDPR, scheduler will delete periodically, but don't show them here
            since = one_year_ago(since)

        # the user hasn't sent any message in this group yet
        if until is None:
            return 0

        messages_from_user = self._get_messages_in_group_from_user(group_id, user_id, until, since)
        return len(messages_from_user)

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
        user_id: int,
        query: DeleteAttachmentQuery
    ) -> Dict[str, List[MessageBase]]:
        group_to_atts = dict()
        start = time()

        for group_id, created_at in group_created_at:
            attachments = self.delete_attachments(group_id, created_at, user_id, query)

            if len(attachments):
                group_to_atts[group_id] = attachments

        elapsed = time() - start
        if elapsed > 5:
            n = len(group_to_atts)
            logger.info(
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

        if not len(messages):
            return

        logger.info(f"deleting {len(messages)} messages in group {group_id}...")
        self._delete_messages(messages, "messages")

    def delete_attachments_in_group_before(self, group_id: str, before: dt):
        attachments = (
            AttachmentModel.objects(
                AttachmentModel.group_id == group_id,
                AttachmentModel.created_at <= before,
            )
            .all()
        )

        logger.info(f"deleting {len(attachments)} attachments in group {group_id}...")
        self._delete_messages(attachments, "attachments")

    def delete_attachments(
        self,
        group_id: str,
        group_created_at: dt,
        user_id: int,
        query: DeleteAttachmentQuery
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

        attachment_msg_ids = {attachment.message_id for attachment in attachments}
        attachment_bases = [
            CassandraHandler.message_base_from_entity(attachment) for attachment in attachments
        ]

        # no un-deleted attachments in this group
        if not len(attachment_msg_ids):
            return list()

        messages = list()

        for attachment_type in MessageTypes.attachment_types:
            messages_all = (
                # can't restrict user_id here, since it's a primary key and we filter on a "non-EQ relation" created_at
                # using greater-than... so filter on user_id in python instead
                MessageModel.objects(
                    MessageModel.group_id == group_id,
                    MessageModel.created_at > group_created_at,
                    # no support for "IN" in cassandra orm, so run multiple queries
                    MessageModel.message_type == attachment_type
                )
                .allow_filtering()
                .all()
            )

            for message in messages_all:
                if message.user_id == user_id and message.message_id in attachment_msg_ids:
                    messages.append(message)

        payload_status = query.status
        if payload_status is None:
            payload_status = PayloadStatus.DELETED

        self._update_payload_status_to(messages, payload_status)
        self._delete_messages(attachments, "attachments")

        return attachment_bases

    def delete_attachment(
        self,
        group_id: str,
        group_created_at: dt,
        query: DeleteAttachmentQuery
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
        message = (
            MessageModel.objects(
                MessageModel.group_id == group_id,
                MessageModel.created_at > group_created_at,
                MessageModel.message_id == attachment.message_id,
            )
            .allow_filtering()
            .first()
        )

        if attachment is None:
            raise NoSuchAttachmentException(query.file_id)

        # to be returned
        attachment_base = CassandraHandler.message_base_from_entity(attachment)

        logger.info("deleting attachment: group_id={}, message_id={}, user_id={}".format(
            group_id, str(attachment.message_id), attachment.user_id
        ))

        payload_status = query.status
        if payload_status is None:
            payload_status = PayloadStatus.DELETED

        self._update_payload_status_to([message], payload_status)
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

        message.delete()

    def get_message_with_id(self, group_id: str, user_id: int, message_id: str, created_at: float):
        approx_after = arrow.get(created_at).shift(minutes=-1).datetime
        approx_before = arrow.get(created_at).shift(minutes=1).datetime

        message = (
            MessageModel.objects(
                MessageModel.group_id == group_id,
                MessageModel.user_id == user_id,
                MessageModel.created_at > approx_after,
                MessageModel.created_at <= approx_before,
                MessageModel.message_id == message_id,
            )
            .allow_filtering()
            .first()
        )

        if message is None:
            raise NoSuchMessageException(message_id)

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
        # filter by some minute before and after
        approx_date_after = arrow.get(created_at).shift(minutes=-30).datetime
        approx_date_before = arrow.get(created_at).shift(minutes=30).datetime

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

    # noinspection PyMethodMayBeStatic
    def store_message(self, group_id: str, user_id: int, query: SendMessageQuery) -> MessageBase:
        message = None

        # if the user is sending multiple images at the same time it may happen different servers create them
        # with the exact same milliseconds, which will cause primary key collision in cassandra (silently
        # losing all but one of the messages with the same milliseconds)
        if query.message_type in MessageTypes.attachment_types:
            inserted = False
            message_id = uuid()

            while not inserted:
                # recreate creation time on each try, until we don't have primary key collision anymore
                created_at = utcnow_dt()

                # can't use "if not exists" or serial consistency when using the ORM, so use a raw query
                results = self.session.execute(
                    "insert into messages (group_id, created_at, user_id, message_id, message_payload, message_type, context)" +
                    "values (%s, %s, %s, %s, %s, %s, %s)" +
                    "if not exists;",
                    (
                        UUID(group_id), created_at, user_id, message_id,
                        query.message_payload, query.message_type, query.context
                    ),
                    # this profile has serial consistency level set to 'serial', to make sure we don't do an UPSERT
                    execution_profile='transaction'
                ).all()

                inserted = results[0].applied
                if not inserted:
                    logger.warning(
                        f"found duplicate primary key when inserting image: " +
                        f"group_id={group_id}, user_id={user_id}, created_at={created_at}, message_id={message_id}"
                    )
                    # try again with a newly generated created_at
                    continue

                # querying by exact datetime seems to be shifty in cassandra, so just
                # filter by a minute before and after
                approx_date_after = arrow.get(created_at).shift(seconds=-1).datetime
                approx_date_before = arrow.get(created_at).shift(seconds=1).datetime

                # when using 'if not exists', the inserted row will not be returned,
                # so we have to query for it after insertion
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
        else:
            created_at = utcnow_dt()
            message = MessageModel.create(
                group_id=group_id,
                user_id=user_id,
                created_at=created_at,
                message_id=uuid(),
                message_payload=query.message_payload,
                message_type=query.message_type,
                context=query.context,
            )

        return CassandraHandler.message_base_from_entity(message)

    def edit_message(self, group_id: str, user_id: int, message_id: str, query: EditMessageQuery) -> MessageBase:
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
            context=query.context or message.context,
            message_payload=query.message_payload or message.message_payload,
            updated_at=now,
        )

        attachment = (
            AttachmentModel.objects(
                AttachmentModel.group_id == group_id,
                AttachmentModel.user_id == user_id,
                AttachmentModel.created_at > approx_date_after,
                AttachmentModel.created_at < approx_date_before,
                AttachmentModel.message_id == message_id,
            )
            .allow_filtering()
            .first()
        )

        # might not be an attachment
        if attachment is not None:
            attachment.update(
                message_type=attachment.message_type or message.message_type,
                context=query.context or attachment.context,
                message_payload=query.message_payload or attachment.message_payload,
                updated_at=now,
            )

        return CassandraHandler.message_base_from_entity(message)

    def _update_payload_status_to(self, messages: List[MessageModel], status: int):
        start = time()
        with BatchQuery() as b:
            for message in messages:
                try:
                    payload = json.loads(message.message_payload)
                    payload["status"] = status
                    message.message_payload = json.dumps(payload)
                except Exception as e:
                    logger.error("failed to update status for group {} message {}: '{}', payload was: {}".format(
                        message.group_id,
                        message.message_id,
                        str(e),
                        message.message_payload
                    ))
                    continue

                message.batch(b).save()

        elapsed = time() - start
        if elapsed > 1:
            logger.info(f"updated payload status of {len(message)} messages in {elapsed:.2f}s")

    def _delete_messages(self, messages, types: str) -> None:
        start = time()
        with BatchQuery() as b:
            for message in messages:
                message.batch(b).delete()

        elapsed = time() - start
        if elapsed > 1:
            logger.info(f"batch deleted {len(message)} {types} in {elapsed:.2f}s")

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
    def _get_batch_of_messages_in_group_since(
        self, group_id: str, until: dt, since: dt, limit=1000
    ) -> List[MessageModel]:
        return (
            MessageModel.objects(
                MessageModel.group_id == group_id,
                MessageModel.created_at < until,
                MessageModel.created_at > since,
            )
            .limit(limit)
            .all()
        )

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
            context=message.context,
            updated_at=message.updated_at,
            file_id=message.file_id,
        )
