import logging
import sys
from abc import ABC
from abc import abstractmethod
from typing import List

import redis

from dinofw.config import ConfigKeys
from dinofw.db.rdbms.schemas import GroupBase
from dinofw.db.storage.schemas import MessageBase
from dinofw.rest.server.models import AbstractQuery
from dinofw.utils import IPublisher


class BasePublisher(ABC):
    @abstractmethod
    def send(self, fields: dict) -> None:
        """
        published a bunch of fields to the configured stream

        :param fields: a dict of values for the event
        """


class MockPublisher(BasePublisher):
    def __init__(self):
        self.stream = list()

    def send(self, fields: dict) -> None:
        self.stream.append(fields)


class RedisPublisher(BasePublisher):
    def __init__(self, env, host: str, port: int = 6379, db: int = 0):
        if env.config.get(ConfigKeys.TESTING, default=False) or host == "mock":
            from fakeredis import FakeStrictRedis

            self.redis_pool = None
            self.redis_instance = FakeStrictRedis(host=host, port=port, db=db)
        else:
            self.redis_pool = redis.ConnectionPool(host=host, port=port, db=db)
            self.redis_instance = None

        self.consumer_stream = env.config.get(ConfigKeys.STREAM, domain=ConfigKeys.PUBLISHER).encode()
        self.consumer_group = env.config.get(ConfigKeys.GROUP, domain=ConfigKeys.PUBLISHER).encode()

        # the stream might not exist on first run
        try:
            self.redis.xgroup_create(
                name=self.consumer_stream,
                groupname=self.consumer_group,
                id=b"$",
                mkstream=False
            )
        except redis.exceptions.ResponseError:
            pass

    def send(self, fields: dict) -> None:
        values = {
            key: value
            for key, value in fields.items()
            if value is not None
        }
        self.redis.xadd(self.consumer_stream, values)

    @property
    def redis(self):
        if self.redis_pool is None:
            return self.redis_instance
        return redis.Redis(connection_pool=self.redis_pool)


class Publisher(IPublisher):
    def __init__(self, env, host: str, port: int = 6379, db: int = 0):
        self.env = env
        self.topic = self.env.config.get(ConfigKeys.TOPIC, domain=ConfigKeys.KAFKA)
        self.logger = logging.getLogger(__name__)

        if host == "mock":
            self.publisher = MockPublisher()
        else:
            self.publisher = RedisPublisher(env, host, port, db)

    def message(
        self, group_id: str, user_id: int, message: MessageBase, user_ids: List[int]
    ) -> None:
        fields = Publisher.message_base_to_fields(message, user_ids)

        try:
            self.publisher.send(fields)
        except Exception as e:
            self.logger.error(f"could not publish to redis stream: {str(e)}")
            self.logger.exception(e)
            self.env.capture_exception(sys.exc_info())

    def group_change(self, group_base: GroupBase, user_ids: List[int]) -> None:
        fields = Publisher.group_base_to_fields(group_base, user_ids)
        print('fields:')
        print(fields)

        try:
            self.publisher.send(fields)
        except Exception as e:
            self.logger.error(f"could not publish to redis stream: {str(e)}")
            self.logger.exception(e)
            self.env.capture_exception(sys.exc_info())

    def join(self, group_id: str, user_id: int) -> None:
        fields = {
            "event_type": "join",
            "group_id": group_id,
            "user_id": user_id,
        }

        try:
            self.publisher.send(fields)
        except Exception as e:
            self.logger.error(f"could not publish to redis stream: {str(e)}")
            self.logger.exception(e)
            self.env.capture_exception(sys.exc_info())

    def leave(self, group_id: str, user_id: int) -> None:
        fields = {
            "event_type": "leave",
            "group_id": group_id,
            "user_id": user_id,
        }

        try:
            self.publisher.send(fields)
        except Exception as e:
            self.logger.error(f"could not publish to redis stream: {str(e)}")
            self.logger.exception(e)
            self.env.capture_exception(sys.exc_info())

    @staticmethod
    def message_base_to_fields(message: MessageBase, user_ids: List[int]):
        return {
            "event_type": "message",
            "group_id": message.group_id,
            "sender_id": message.user_id,
            "message_id": message.message_id,
            "message_payload": message.message_payload,
            "message_type": message.message_type,
            "status": message.status,
            "updated_at": AbstractQuery.to_ts(message.updated_at, allow_none=True) or "",
            "created_at": AbstractQuery.to_ts(message.created_at),
            "user_ids": ",".join([str(user_id) for user_id in user_ids]),
        }

    @staticmethod
    def group_base_to_fields(group: GroupBase, user_ids: List[int]):
        return {
            "event_type": "group",
            "group_id": group.group_id,
            "name": group.name,
            "description": group.description,
            "created_at": AbstractQuery.to_ts(group.created_at),
            "updated_at": AbstractQuery.to_ts(group.updated_at, allow_none=True) or None,
            "last_message_time": AbstractQuery.to_ts(group.last_message_time, allow_none=True) or None,
            "last_message_overview": group.last_message_overview,
            "last_message_id": group.last_message_id,
            "status": group.status,
            "group_type": group.group_type,
            "owner_id": group.owner_id,
            "group_meta": group.group_meta,
            "group_weight": group.group_weight,
            "group_context": group.group_context,
            "user_ids": ",".join([str(user_id) for user_id in user_ids]),
        }

    @staticmethod
    def fields_to_message_base(fields: dict):
        # TODO: maybe we don't need to convert; just send the fields to the client, less bloated requests/resposes
        user_ids = [int(user_id) for user_id in fields["user_ids"].split(",")]

        return user_ids, MessageBase(
            group_id=fields["group_id"],
            user_id=fields["user_id"],
            message_id=fields["message_id"],
            message_payload=fields["message_payload"],
            status=fields["status"],
            message_type=fields["message_type"],
            created_at=AbstractQuery.to_dt(fields["created_at"]),
            updated_at=AbstractQuery.to_dt(fields["updated_at"], allow_none=True),
        )
