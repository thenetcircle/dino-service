import logging
import sys
from abc import ABC
from abc import abstractmethod
from typing import List

from gmqtt import Client as MQTTClient
from gmqtt.mqtt.constants import MQTTv50

from dinofw.config import ConfigKeys
from dinofw.db.rdbms.schemas import GroupBase
from dinofw.db.storage.schemas import MessageBase
from dinofw.rest.server.models import AbstractQuery
from dinofw.utils import IPublisher


class BasePublisher(ABC):
    @abstractmethod
    def send(self, user_id: int, fields: dict) -> None:
        """
        publish a bunch of fields to the configured stream
        """


class MockPublisher(BasePublisher):
    def __init__(self):
        self.stream = list()

    def send(self, user_id: int, fields: dict) -> None:
        self.stream.append(fields)


class MqttPublisher(BasePublisher):
    def __init__(self, env):
        self.env = env
        self.mqtt_host = env.config.get(ConfigKeys.HOST, domain=ConfigKeys.MQTT)
        self.mqtt_port = env.config.get(ConfigKeys.PORT, domain=ConfigKeys.MQTT)
        self.mqtt_ttl = int(env.config.get(ConfigKeys.TTL, domain=ConfigKeys.MQTT))

        # TODO: unique client id
        self.mqtt = MQTTClient("client-id")

    async def setup(self):
        await self.mqtt.connect(
            self.mqtt_host,
            port=self.mqtt_port,
            version=MQTTv50
        )

    def send(self, user_id: int, fields: dict) -> None:
        data = {
            key: value
            for key, value in fields.items()
            if value is not None
        }
        self.mqtt.publish(
            message_or_topic=str(user_id),
            payload=data,
            qos=1,
            message_expiry_interval=self.mqtt_ttl
        )


class Publisher(IPublisher):
    def __init__(self, env):
        self.env = env
        self.topic = self.env.config.get(ConfigKeys.TOPIC, domain=ConfigKeys.KAFKA)
        self.logger = logging.getLogger(__name__)
        self.publisher = MqttPublisher(env)

    async def setup(self):
        await self.publisher.setup()

    def message(self, group_id: str, message: MessageBase, user_ids: List[int]) -> None:
        data = Publisher.message_base_to_fields(message)
        self.send(user_ids, data)

    def group_change(self, group_base: GroupBase, user_ids: List[int]) -> None:
        data = Publisher.group_base_to_fields(group_base, user_ids)
        self.send(user_ids, data)

    def join(self, group_id: str, user_ids: List[int], joiner_id: int, now: float) -> None:
        data = {
            "event_type": "join",
            "created_at": now,
            "group_id": group_id,
            "user_id": joiner_id,
        }
        self.send(user_ids, data)

    def leave(self, group_id: str, user_ids: List[int], leaver_id: int, now: float) -> None:
        data = {
            "event_type": "leave",
            "created_at": now,
            "group_id": group_id,
            "user_id": leaver_id,
        }
        self.send(user_ids, data)

    def send(self, user_ids, data):
        for user_id in user_ids:
            try:
                self.publisher.send(user_id, data)
            except Exception as e:
                self.logger.error(f"could not handle message: {str(e)}")
                self.logger.exception(e)
                self.env.capture_exception(sys.exc_info())

    @staticmethod
    def message_base_to_fields(message: MessageBase):
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
            "meta": group.meta,
            "weight": group.weight,
            "context": group.context,
            "user_ids": ",".join([str(user_id) for user_id in user_ids]),
        }
