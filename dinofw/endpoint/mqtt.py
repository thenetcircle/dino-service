import logging
import sys
from typing import List
from uuid import uuid4 as uuid

from gmqtt import Client as MQTTClient
from gmqtt.mqtt.constants import MQTTv50

from dinofw.db.rdbms.schemas import GroupBase
from dinofw.db.storage.schemas import MessageBase
from dinofw.endpoint import EventTypes
from dinofw.endpoint import IClientPublishHandler
from dinofw.endpoint import IClientPublisher
from dinofw.utils.config import ConfigKeys


class MqttPublisher(IClientPublisher):
    def __init__(self, env):
        self.env = env
        self.mqtt_host = env.config.get(ConfigKeys.HOST, domain=ConfigKeys.MQTT)
        self.mqtt_port = env.config.get(ConfigKeys.PORT, domain=ConfigKeys.MQTT)
        self.mqtt_ttl = int(env.config.get(ConfigKeys.TTL, domain=ConfigKeys.MQTT))

        client_id = str(uuid()).split("-")[0]
        client_id = f"dino-ms-{client_id}"

        self.mqtt = MQTTClient(
            client_id=client_id,
            session_expiry_interval=60
        )

    async def setup(self):
        await self.mqtt.connect(
            self.mqtt_host,
            port=self.mqtt_port,
            version=MQTTv50
        )

    def send(self, user_id: int, fields: dict, qos: int = 1) -> None:
        data = {
            key: value
            for key, value in fields.items()
            if value is not None
        }
        self.mqtt.publish(
            message_or_topic=str(user_id),
            payload=data,
            qos=qos,
            message_expiry_interval=self.mqtt_ttl
        )


class MqttPublishHandler(IClientPublishHandler):
    def __init__(self, env):
        self.env = env
        self.logger = logging.getLogger(__name__)
        self.publisher = MqttPublisher(env)

    async def setup(self):
        try:
            await self.publisher.setup()
        except Exception as e:
            self.logger.error(f"count not connect to mqtt: {str(e)}")
            self.logger.exception(e)

    def message(self, message: MessageBase, user_ids: List[int]) -> None:
        data = MqttPublishHandler.message_base_to_event(message)
        self.send(user_ids, data)

    def attachment(self, attachment: MessageBase, user_ids: List[int]) -> None:
        data = MqttPublishHandler.message_base_to_event(attachment)
        self.send(user_ids, data)

    def read(self, group_id: str, user_id: int, user_ids: List[int], now: float) -> None:
        # only send read receipt to 1v1 groups
        if len(user_ids) > 2:
            return

        data = MqttPublishHandler.read_to_event(group_id, user_id, now)
        self.send(user_ids, data)

    def delete_attachments(
        self,
        group_id: str,
        attachments: List[MessageBase],
        user_ids: List[int],
        now: float
    ) -> None:
        data = MqttPublishHandler.event_for_delete_attachments(group_id, attachments, now)

        # we're sending deletion events async, and gmqtt can't store qos > 0
        # without an eventloop, which we don't have since starlette runs
        # BackgroundTasks in a thread executor... might be able to get around
        # it by using something like this, but has to be tested:
        #
        #     loop = asyncio.new_event_loop()
        #     asyncio.set_event_loop(loop)
        #
        #     loop.run_until_complete(do_stuff(i))
        #     loop.close()
        #
        # for now, just use qos of 0 for deletion events, not the end of the
        # world if they aren't delivered
        self.send(user_ids, data, qos=0)

    def group_change(self, group_base: GroupBase, user_ids: List[int]) -> None:
        data = MqttPublishHandler.group_base_to_event(group_base, user_ids)
        self.send(user_ids, data)

    def join(self, group_id: str, user_ids: List[int], joiner_id: int, now: float) -> None:
        data = MqttPublishHandler.create_simple_event(EventTypes.JOIN, group_id, now, joiner_id)
        self.send(user_ids, data)

    def leave(self, group_id: str, user_ids: List[int], leaver_id: int, now: float) -> None:
        data = MqttPublishHandler.create_simple_event(EventTypes.LEAVE, group_id, now, leaver_id)
        self.send(user_ids, data)

    def send(self, user_ids, data, qos: int = 1):
        for user_id in user_ids:
            try:
                self.publisher.send(user_id, data, qos)
            except Exception as e:
                self.logger.error(f"could not handle message: {str(e)}")
                self.logger.exception(e)
                self.env.capture_exception(sys.exc_info())
