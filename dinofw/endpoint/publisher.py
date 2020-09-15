import logging
import sys
from typing import List

from gmqtt import Client as MQTTClient
from gmqtt.mqtt.constants import MQTTv50

from dinofw.utils.config import ConfigKeys
from dinofw.db.rdbms.schemas import GroupBase
from dinofw.db.storage.schemas import MessageBase
from dinofw.endpoint import IPublishHandler
from dinofw.endpoint import IPublisher


class MqttPublisher(IPublisher):
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


class PublishHandler(IPublishHandler):
    def __init__(self, env):
        self.env = env
        self.logger = logging.getLogger(__name__)
        self.publisher = MqttPublisher(env)

    async def setup(self):
        await self.publisher.setup()

    def message(self, group_id: str, message: MessageBase, user_ids: List[int]) -> None:
        data = PublishHandler.message_base_to_event(message)
        self.send(user_ids, data)

    def read(self, group_id: str, user_id: int, user_ids: List[int]) -> None:
        # TODO: publish to clients
        pass

    def group_change(self, group_base: GroupBase, user_ids: List[int]) -> None:
        data = PublishHandler.group_base_to_event(group_base, user_ids)
        self.send(user_ids, data)

    def join(self, group_id: str, user_ids: List[int], joiner_id: int, now: float) -> None:
        data = PublishHandler.create_simple_event("join", group_id, joiner_id, now)
        self.send(user_ids, data)

    def leave(self, group_id: str, user_ids: List[int], leaver_id: int, now: float) -> None:
        data = PublishHandler.create_simple_event("leave", group_id, leaver_id, now)
        self.send(user_ids, data)

    def send(self, user_ids, data):
        for user_id in user_ids:
            try:
                self.publisher.send(user_id, data)
            except Exception as e:
                self.logger.error(f"could not handle message: {str(e)}")
                self.logger.exception(e)
                self.env.capture_exception(sys.exc_info())
