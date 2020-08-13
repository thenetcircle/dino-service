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


class IKafkaWriterFactory(ABC):
    @abstractmethod
    def create_producer(self, *args, **kwargs):
        """pass"""


class KafkaWriterFactory(IKafkaWriterFactory):
    """
    for mocking purposes
    """

    def create_producer(self, **kwargs):
        from kafka import KafkaProducer

        return KafkaProducer(**kwargs)


class MockProducer:
    def send(self, topic: str, event: dict, key: str = None):
        pass


class Publisher(IPublisher):
    def __init__(self, env, host: str, port: int = 6379, db: int = 0):
        self.env = env
        self.topic = self.env.config.get(ConfigKeys.TOPIC, domain=ConfigKeys.KAFKA)
        self.logger = logging.getLogger(__name__)

        if env.config.get(ConfigKeys.TESTING, default=False) or host == "mock":
            from fakeredis import FakeStrictRedis

            self.redis_pool = None
            self.redis_instance = FakeStrictRedis(host=host, port=port, db=db)
        else:
            self.redis_pool = redis.ConnectionPool(host=host, port=port, db=db)
            self.redis_instance = None

        self.consumer_stream = "dino_client_stream"
        self.consumer_group = "dino_client_group"

        # TODO: check that we don't recreate stuff unnecessarily with this command
        # XGROUP CREATE dino_client_stream dino_client_group $ MKSTREAM
        self.redis.xgroup_create(self.consumer_stream, self.consumer_group, id="$", mkstream=True)

    def message(
        self, group_id: str, user_id: int, message: MessageBase, user_ids: List[int]
    ) -> None:
        fields = Publisher.message_base_to_fields(message, user_ids)

        try:
            self.redis.xadd(self.consumer_stream, fields)
        except Exception as e:
            self.logger.error(f"could not publish to redis stream: {str(e)}")
            self.logger.exception(e)
            self.env.capture_exception(sys.exc_info())

    def group_change(self, group_base: GroupBase, user_ids: List[int]) -> None:
        pass  # TODO: implement

    @property
    def redis(self):
        if self.redis_pool is None:
            return self.redis_instance
        return redis.Redis(connection_pool=self.redis_pool)

    @staticmethod
    def message_base_to_fields(message: MessageBase, user_ids: List[int]):
        return {
            "group_id": message.group_id,
            "sender_id": message.user_id,
            "message_id": message.message_id,
            "message_payload": message.message_payload,
            "message_type": message.message_type,
            "status": message.status,
            "updated_at": AbstractQuery.to_ts(message.updated_at) or "",
            "created_at": AbstractQuery.to_ts(message.created_at),
            "user_ids": ",".join([str(user_id) for user_id in user_ids])
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
