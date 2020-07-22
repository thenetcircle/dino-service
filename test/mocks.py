from datetime import datetime as dt
from typing import Dict
from uuid import uuid4 as uuid

import pytz

from dinofw.db.cassandra.schemas import MessageBase
from dinofw.db.rdbms.schemas import GroupBase
from dinofw.db.rdbms.schemas import UserGroupStatsBase
from dinofw.rest.server.models import CreateGroupQuery
from dinofw.rest.server.models import GroupQuery
from dinofw.rest.server.models import SendMessageQuery


class FakeStorage:
    def __init__(self):
        self.messages_by_group = dict()
        self.action_log = dict()

    def store_message(self, group_id: str, user_id: int, query: SendMessageQuery) -> MessageBase:
        if group_id not in self.messages_by_group:
            self.messages_by_group[group_id] = list()

        now = dt.utcnow()
        now = now.replace(tzinfo=pytz.UTC)

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


class FakeDatabase:
    def __init__(self):
        self.groups = dict()
        self.stats = dict()

    def update_group_new_message(self, message: MessageBase, sent_time: dt, _) -> None:
        if message.group_id not in self.groups:
            return

        self.groups[message.group_id].last_message_time = sent_time
        self.groups[message.group_id].last_message_overview = message.message_payload

    def create_group(
        self, owner_id: int, query: CreateGroupQuery, _
    ) -> GroupBase:
        created_at = dt.utcnow()
        created_at = created_at.replace(tzinfo=pytz.UTC)

        group = GroupBase(
            group_id=str(uuid()),
            name=query.group_name,
            group_type=query.group_type,
            last_message_time=created_at,
            created_at=created_at,
            owner_id=owner_id,
            group_meta=query.group_meta,
            group_context=query.group_context,
            description=query.description,
        )

        self.groups[group.group_id] = group

        return group

    def update_last_read_and_sent_in_group_for_user(
        self, user_id: int, group_id: str, created_at: dt, _
    ) -> None:

        if user_id in self.stats:
            self.stats[user_id].last_read = created_at
            self.stats[user_id].last_sent = created_at

        else:
            self.stats[user_id] = UserGroupStatsBase(
                group_id=group_id,
                user_id=user_id,
                last_read=created_at,
                last_sent=created_at,
                hide_before=created_at,
                join_time=created_at,
            )

    def get_user_ids_and_join_times_in_group(
        self, group_id: str, query: GroupQuery, _, skip_cache: bool = False
    ) -> Dict[int, float]:
        response = dict()

        for _, stat in self.stats.items():
            if stat.group_id == group_id:
                response[stat.user_id] = stat.join_time

        return response


class FakePublisher:
    def __init__(self):
        self.sent_messages = dict()

    def message(self, group_id, user_id, message, user_ids):
        if group_id not in self.sent_messages:
            self.sent_messages[group_id] = list()

        self.sent_messages[group_id].append(message)


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
        self.storage = FakeStorage()
        self.db = FakeDatabase()
        self.publisher = FakePublisher()
