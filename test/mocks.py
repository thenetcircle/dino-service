from datetime import datetime as dt, timedelta
from typing import Dict, List, Optional, Tuple
from uuid import uuid4 as uuid

import arrow

from dinofw.cache.redis import CacheRedis
from dinofw.db.rdbms.schemas import GroupBase, UserGroupBase
from dinofw.db.rdbms.schemas import UserGroupStatsBase
from dinofw.db.storage.schemas import MessageBase
from dinofw.endpoint import IClientPublishHandler, IClientPublisher
from dinofw.rest.models import AbstractQuery, AttachmentQuery
from dinofw.rest.models import CreateActionLogQuery
from dinofw.rest.models import CreateAttachmentQuery
from dinofw.rest.models import CreateGroupQuery
from dinofw.rest.models import EditMessageQuery
from dinofw.rest.models import GroupQuery
from dinofw.rest.models import MessageQuery
from dinofw.rest.models import SendMessageQuery
from dinofw.utils import trim_micros
from dinofw.utils import utcnow_dt
from dinofw.utils.exceptions import NoSuchGroupException
from dinofw.utils.exceptions import NoSuchAttachmentException


class FakeStorage:
    ACTION_TYPE_JOIN = 0
    ACTION_TYPE_LEAVE = 1

    def __init__(self, env):
        self.env = env
        self.messages_by_group = dict()
        self.attachments_by_group = dict()
        self.attachments_by_message = dict()
        self.action_log = dict()

    def delete_attachment(self, group_id: str, created_at: dt, query: AttachmentQuery) -> MessageBase:
        file_id = query.file_id
        att_copy = None

        for group, atts in self.attachments_by_group.items():
            for att in atts:
                if att.file_id == file_id:
                    att_copy = att.copy()
                    self.attachments_by_group[group].remove(att)
                    break

        for message, att in self.attachments_by_message.items():
            if att.file_id == file_id:
                del self.attachments_by_message[message]
                break

        if att_copy is None:
            raise NoSuchAttachmentException(query.file_id)

        return att_copy

    def delete_attachments(
        self,
        group_id: str,
        group_created_at: dt,
        user_id: int
    ) -> List[MessageBase]:
        attachments = list()

        if group_id in self.attachments_by_group:
            to_keep = list()

            for att in self.attachments_by_group[group_id]:
                if att.user_id == user_id:
                    attachments.append(att.copy())
                else:
                    to_keep.append(att)

            self.attachments_by_group[group_id] = to_keep

        to_keep = dict()
        for message, att in self.attachments_by_message.items():
            if att.user_id != user_id:
                to_keep[message] = att
        self.attachments_by_message = to_keep

        return attachments

    def delete_attachments_in_all_groups(
        self,
        group_created_at: List[Tuple[str, dt]],
        user_id: int
    ) -> Dict[str, List[MessageBase]]:
        attachments = dict()

        for group_id, created_at in group_created_at:
            attachments[group_id] = self.delete_attachments(group_id, created_at, user_id)

        return attachments

    def get_unread_in_group(self, group_id: str, user_id: int, last_read: dt) -> int:
        unread = self.env.cache.get_unread_in_group(group_id, user_id)
        if unread is not None:
            return unread

        unread = 0
        if group_id not in self.messages_by_group:
            return unread

        for message in self.messages_by_group[group_id]:
            if message.created_at > last_read:
                unread += 1

        return unread

    def create_action_logs(
        self, group_id: str, query: CreateActionLogQuery
    ) -> List[MessageBase]:
        if group_id not in self.action_log:
            self.action_log[group_id] = list()

        logs = list()

        for user_id in query.user_ids:
            log = MessageBase(
                group_id=group_id,
                created_at=arrow.utcnow().datetime,
                user_id=user_id,
                message_id=str(uuid()),
                message_type=query.action_type,
                admin_id=query.admin_id,
            )

            self.action_log[group_id].append(log)
            logs.append(log)

        return logs

    def store_attachment(
        self, group_id: str, user_id: int, message_id: str, query: CreateAttachmentQuery
    ) -> MessageBase:

        message_type = None
        for message in self.messages_by_group[group_id]:
            if message.message_id == message_id:
                message_type = message.message_type
                break

        if message_type is None:
            raise NoSuchAttachmentException(message_id)

        attachment = MessageBase(
            group_id=str(group_id),
            created_at=utcnow_dt(),
            user_id=user_id,
            file_id=query.file_id,
            message_id=message_id,
            message_payload=query.message_payload,
            message_type=message_type,
        )

        if group_id not in self.attachments_by_group:
            self.attachments_by_group[group_id] = list()

        self.attachments_by_group[group_id].append(attachment)
        self.attachments_by_message[message_id] = attachment

        return attachment

    def store_message(
        self, group_id: str, user_id: int, query: SendMessageQuery
    ) -> MessageBase:
        if group_id not in self.messages_by_group:
            self.messages_by_group[group_id] = list()

        now = utcnow_dt()

        message = MessageBase(
            group_id=group_id,
            created_at=now,
            user_id=user_id,
            message_id=str(uuid()),
            message_payload=query.message_payload,
            message_type=query.message_type,
        )

        self.messages_by_group[group_id].append(message)

        return message

    def create_join_action_log(
        self, group_id: str, users: Dict[int, float], action_time: dt
    ) -> List[MessageBase]:
        user_ids = [user_id for user_id, _ in users.items()]
        return self._create_action_log(
            group_id, user_ids, action_time, FakeStorage.ACTION_TYPE_JOIN
        )

    def create_leave_action_log(
        self, group_id: str, user_ids: [int], action_time: dt
    ) -> List[MessageBase]:
        return self._create_action_log(
            group_id, user_ids, action_time, FakeStorage.ACTION_TYPE_LEAVE
        )

    def _create_action_log(
        self, group_id: str, user_ids: List[int], action_time: dt, action_type: int
    ) -> List[MessageBase]:
        if group_id not in self.action_log:
            self.action_log[group_id] = list()

        new_logs = list()

        for user_id in user_ids:
            log = MessageBase(
                group_id=group_id,
                user_id=user_id,
                created_at=action_time,
                message_type=action_type,
                message_id=str(uuid()),
            )

            new_logs.append(log)
            self.action_log[group_id].append(log)

        return new_logs

    def get_messages_in_group(
        self, group_id: str, query: MessageQuery
    ) -> List[MessageBase]:
        if group_id not in self.messages_by_group:
            return list()

        messages = list()

        for message in self.messages_by_group[group_id]:
            messages.append(message)

            if len(messages) > query.per_page:
                break

        return messages

    def get_attachments_in_group_for_user(
        self, group_id: str, user_stats: UserGroupStatsBase, query: MessageQuery
    ) -> List[MessageBase]:
        if group_id not in self.attachments_by_group:
            return list()

        attachments = list()

        for attachment in self.attachments_by_group[group_id]:
            if attachment.created_at > user_stats.delete_before:
                attachments.append(attachment)

            if len(attachments) > query.per_page:
                break

        return attachments

    def get_attachment_from_file_id(self, group_id: str, created_at: dt, query: AttachmentQuery) -> MessageBase:
        if group_id not in self.attachments_by_group:
            raise NoSuchAttachmentException(query.file_id)

        for attachment in self.attachments_by_group[group_id]:
            if attachment.file_id == query.file_id:
                return attachment

        raise NoSuchAttachmentException(query.file_id)

    def get_messages_in_group_for_user(
        self, group_id: str, user_stats: UserGroupStatsBase, query: MessageQuery,
    ) -> List[MessageBase]:
        if group_id not in self.messages_by_group:
            return list()

        messages = list()

        for message in self.messages_by_group[group_id]:
            if message.created_at > user_stats.delete_before:
                messages.append(message)

            if len(messages) > query.per_page:
                break

        return messages

    def edit_message(
        self, group_id: str, message_id: str, query: EditMessageQuery
    ) -> Optional[MessageBase]:

        message = None

        for m in self.messages_by_group[group_id]:
            if m.message_id == message_id:
                message = m
                break

        if message is None:
            return None

        if query.message_payload is not None:
            message.message_payload = query.message_payload
        if query.message_type is not None:
            message.message_type = query.message_type
        if query.status is not None:
            message.status = query.status

        message.updated_at = arrow.utcnow().datetime

        return message

    def get_action_log_in_group(
        self, group_id: str, query: MessageQuery
    ) -> List[MessageBase]:
        logs = list()

        if group_id not in self.action_log:
            return list()

        for log in self.action_log[group_id]:
            logs.append(log)

            if len(logs) > query.per_page:
                break

        return logs

    def get_action_log_in_group_for_user(
        self, group_id: str, user_stats: UserGroupStatsBase, query: MessageQuery,
    ) -> List[MessageBase]:
        logs = list()

        if group_id not in self.action_log:
            return list()

        for log in self.action_log[group_id]:
            if log.created_at <= user_stats.delete_before:
                continue

            logs.append(log)

            if len(logs) > query.per_page:
                break

        return logs

    def count_messages_in_group_since(self, group_id: str, since: dt) -> int:
        if group_id not in self.messages_by_group:
            return 0

        messages = list()

        for message in self.messages_by_group[group_id]:
            if message.created_at <= since:
                continue

            messages.append(message)

        return len(messages)


class FakeDatabase:
    def __init__(self):
        self.groups = dict()
        self.stats = dict()
        self.last_sent = dict()

        beginning_of_1995 = 789_000_000
        self.long_ago = arrow.Arrow.utcfromtimestamp(beginning_of_1995).datetime

    def get_last_message_time_in_group(self, group_id: str, _) -> dt:
        if group_id not in self.groups:
            raise NoSuchGroupException(group_id)

        return self.groups[group_id].last_message_time

    def update_group_new_message(self, message: MessageBase, sent_time: dt, _, wakeup_users: bool = True) -> None:
        if message.group_id not in self.groups:
            return

        self.groups[message.group_id].last_message_time = sent_time
        self.groups[message.group_id].last_message_overview = message.message_payload
        self.groups[message.group_id].last_message_type = message.message_type
        self.groups[message.group_id].last_message_id = message.message_id

    def set_last_updated_at_for_all_in_group(self, group_id: str, _):
        now = arrow.utcnow().datetime

        for _, stats in self.stats.items():
            for stat in stats:
                if stat.group_id != group_id:
                    continue

                stat.last_updated_time = now

    def update_last_read_and_highlight_in_group_for_user(
        self, group_id: str, user_id: int, the_time: dt, _
    ) -> None:
        if user_id not in self.stats:
            return

        for stat in self.stats[user_id]:
            if stat.group_id != group_id:
                continue

            stat.last_read = the_time
            stat.highlight_time = the_time

    def create_group(self, owner_id: int, query: CreateGroupQuery, _) -> GroupBase:
        utc_now = arrow.utcnow()
        now = trim_micros(utc_now.datetime)
        created_at = trim_micros(utc_now.shift(seconds=-1).datetime)

        group = GroupBase(
            group_id=str(uuid()),
            name=query.group_name,
            group_type=query.group_type,
            last_message_time=now,
            first_message_time=now,
            created_at=created_at,
            updated_at=created_at,
            owner_id=owner_id,
            meta=query.meta,
            context=query.context,
            description=query.description,
        )

        self.groups[group.group_id] = group

        return group

    def get_last_sent_for_user(self, user_id: int, _) -> (str, float):
        if user_id not in self.last_sent:
            return None, None

        return self.last_sent[user_id]

    def set_last_sent_for_user(
        self, user_id: int, group_id: str, the_time: float, _
    ) -> None:
        self.last_sent[user_id] = group_id, the_time

    def count_group_types_for_user(
        self, user_id: int, query: GroupQuery, _
    ) -> List[Tuple[int, int]]:
        group_ids_for_user = set()
        group_types = dict()

        if user_id not in self.stats:
            return list()

        for stat in self.stats[user_id]:
            group_ids_for_user.add(stat.group_id)

        for group_id in group_ids_for_user:
            if group_id not in self.groups:
                continue

            group = self.groups[group_id]
            if group.group_type is None:
                continue

            if group.group_type not in group_types:
                group_types[group.group_type] = 0

            group_types[group.group_type] += 1

        return list(group_types.items())

    def get_groups_for_user(
        self, user_id: int, query: GroupQuery, _, count_receiver_unread: bool = True, receiver_stats: bool = False,
    ) -> List[UserGroupBase]:
        groups = list()

        if user_id not in self.stats:
            return list()

        for stat in self.stats[user_id]:
            users = self.get_user_ids_and_join_time_in_group(stat.group_id, None)

            if query.count_unread:
                user_count = self.count_users_in_group(stat.group_id, None)
            else:
                user_count = 0

            if stat.group_id not in self.groups:
                continue

            group = self.groups[stat.group_id]

            user_join_times: dict
            user_count: int
            unread_count: int

            groups.append(
                UserGroupBase(
                    group=group,
                    user_stats=stat,
                    user_count=user_count,
                    unread=-1,  # TODO: get from storage mock
                    receiver_unread=-1,
                    user_join_times=users,
                )
            )

            if len(groups) > query.per_page:
                break

        return groups

    def get_users_in_group(
        self, group_id: str, db
    ) -> (Optional[GroupBase], Optional[Dict[int, float]], Optional[int]):
        if group_id not in self.groups:
            raise NoSuchGroupException(group_id)

        group = self.groups[group_id]
        users = self.get_user_ids_and_join_time_in_group(group_id, db)
        user_count = self.count_users_in_group(group_id, db)

        return group, users, user_count

    def count_users_in_group(self, group_id: str, _) -> int:
        users = list()

        for user_id, stats in self.stats.items():
            for stat in stats:
                if stat.group_id == group_id:
                    users.append(user_id)

        return len(users)

    def update_user_stats_on_join_or_create_group(
        self, group_id: str, users: Dict[int, float], now: dt, _
    ) -> None:
        for user_id, _ in users.items():
            self.update_last_read_and_sent_in_group_for_user(
                group_id, user_id, now, None
            )

    def set_group_updated_at(self, group_id: str, now: dt, _) -> None:
        if group_id not in self.groups:
            return

        self.groups[group_id].updated_at = now

    def update_last_read_and_sent_in_group_for_user(
        self, group_id: str, user_id: int, created_at: dt, _
    ) -> None:
        to_add = UserGroupStatsBase(
            group_id=group_id,
            user_id=user_id,
            last_read=created_at,
            last_sent=created_at,
            delete_before=created_at,
            highlight_time=self.long_ago,
            join_time=created_at,
            last_updated_time=created_at,
            hide=False,
            pin=False,
            bookmark=False,
        )

        if user_id in self.stats:
            found_group = False

            for group_stats in self.stats[user_id]:
                if group_stats.group_id == group_id:
                    group_stats.last_read = created_at
                    group_stats.last_sent = created_at
                    found_group = True

            if not found_group:
                self.stats[user_id].append(to_add)
        else:
            self.stats[user_id] = [to_add]

    def remove_last_read_in_group_for_user(
        self, group_id: str, user_id: int, _
    ) -> None:
        if user_id not in self.stats:
            return

        old_stats = self.stats[user_id]
        new_stats = list()

        for old_stat in old_stats:
            if old_stat.group_id == group_id:
                continue

            new_stats.append(old_stat)

        self.stats[user_id] = new_stats

    def group_exists(self, group_id: str, _) -> bool:
        return group_id in self.groups

    def get_last_reads_in_group(self, group_id: str, _) -> Dict[int, float]:
        last_reads = dict()

        for user_id in self.stats:
            for group_stats in self.stats[user_id]:
                if group_stats.group_id == group_id:
                    last_reads[user_id] = AbstractQuery.to_ts(group_stats.last_read)

        return last_reads

    def get_user_ids_and_join_time_in_group(
        self, group_id: str, _=None
    ) -> Dict[int, float]:
        response = dict()

        for _, stats in self.stats.items():
            for stat in stats:
                if stat.group_id == group_id:
                    response[stat.user_id] = AbstractQuery.to_ts(stat.join_time)
                    break

        return response  # noqa

    def get_user_stats_in_group(
        self, group_id: str, user_id: int, _
    ) -> Optional[UserGroupStatsBase]:
        if user_id not in self.stats:
            return None

        for stat in self.stats[user_id]:
            if stat.group_id == group_id:
                return stat

        return None


class MockPublisher(IClientPublisher):
    def __init__(self):
        self.stream = list()

    def send(self, user_id: int, fields: dict) -> None:
        self.stream.append(fields)


class FakePublisherHandler(IClientPublishHandler):
    def __init__(self):
        self.sent_messages = dict()
        self.sent_attachments = dict()
        self.sent_deletions = dict()
        self.sent_reads = dict()

    def delete_attachments(
        self,
        group_id: str,
        attachments: List[MessageBase],
        user_ids: List[int],
        now: float
    ) -> None:
        data = FakePublisherHandler.event_for_delete_attachments(group_id, attachments, now)

        if group_id not in self.sent_deletions:
            self.sent_deletions[group_id] = list()

        self.sent_deletions[group_id].append(data)

    def message(self, message: MessageBase, user_ids: List[int]) -> None:
        if message.group_id not in self.sent_messages:
            self.sent_messages[message.group_id] = list()

        if message.group_id not in self.sent_attachments:
            self.sent_attachments[message.group_id] = dict()

        self.sent_messages[message.group_id].append(message)

    def attachment(self, attachment: MessageBase, user_ids: List[int]) -> None:
        pass

    def read(self, group_id: str, user_id: int, user_ids: List[int], now) -> None:
        for receiver in user_ids:
            if receiver not in self.sent_reads:
                self.sent_reads[receiver] = list()

            self.sent_reads[receiver].append((group_id, user_id, now))

    def group_change(self, group_base: GroupBase, user_ids: List[int]) -> None:
        pass

    def join(
        self, group_id: str, user_ids: List[int], joiner_id: int, now: float
    ) -> None:
        pass

    def leave(
        self, group_id: str, user_ids: List[int], leaver_id: int, now: float
    ) -> None:
        pass


class FakeEnv:
    class Config:
        def __init__(self):
            self.config = {
                "storage": {
                    "key_space": "dinofw",
                    "host": "maggie-cassandra-1,maggie-cassandra-2",
                },
                "kafka": {
                    "topic": "test",
                    "host": "localhost"
                }
            }

        def get(self, key, domain=None, default=None):
            if domain is None:
                if key not in self.config:
                    return default
                return self.config[key]

            if key not in self.config[domain]:
                return default

            return self.config[domain][key]

    def __init__(self):
        self.config = FakeEnv.Config()
        self.storage = FakeStorage(self)
        self.db = FakeDatabase()
        self.stats = None
        self.client_publisher = FakePublisherHandler()
        self.server_publisher = FakePublisherHandler()
        self.cache = CacheRedis(self, host="mock")

        from dinofw.rest.groups import GroupResource
        from dinofw.rest.users import UserResource
        from dinofw.rest.message import MessageResource

        class RestResources:
            group: GroupResource
            user: UserResource
            message: MessageResource

        self.rest = RestResources()
        self.rest.group = GroupResource(self)
        self.rest.user = UserResource(self)
        self.rest.message = MessageResource(self)

    def capture_exception(self, _):
        pass
