import os
import socket
import sys
from typing import List

import bcrypt
import redis
from gmqtt import Client as MQTTClient
from gmqtt.mqtt.constants import MQTTv50

from loguru import logger
from dinofw.db.rdbms.schemas import GroupBase
from dinofw.db.storage.schemas import MessageBase
from dinofw.endpoint import EventTypes
from dinofw.endpoint import IClientPublishHandler
from dinofw.endpoint import IClientPublisher
from dinofw.utils.config import ConfigKeys


class MqttPublisher(IClientPublisher):
    def __init__(self, env):
        self.env = env
        self.environment = env.config.get(ConfigKeys.ENVIRONMENT, default="test")
        self.mqtt_host = env.config.get(ConfigKeys.HOST, domain=ConfigKeys.MQTT)
        self.mqtt_port = env.config.get(ConfigKeys.PORT, domain=ConfigKeys.MQTT)
        self.mqtt_ttl = int(env.config.get(ConfigKeys.TTL, domain=ConfigKeys.MQTT))

        if "," in self.mqtt_host:
            self.mqtt_host = self.mqtt_host.split(",")[0]

        # needs to be unique for each worker and node
        pid = os.getpid()
        client_id = socket.gethostname().split(".")[0]
        client_id = f"dinoms-{client_id}-{pid}"

        self.mqtt = MQTTClient(
            client_id=client_id,
            session_expiry_interval=60,

            # 'receive_maximum' is defined as: "The Client uses this value to limit the number
            # of QoS 1 and QoS 2 publications that it is willing to process concurrently."
            #
            # default is 2**16-1 = 65535, which was reached during stress testing, but
            # unfortunately we can't increase it because gmqtt builds a struct of this value,
            # and the format is 'H', which can't handle more than 65k:
            #
            #   struct.error: 'H' format requires 0 <= number <= 65535
            receive_maximum=2 ** 16 - 1,
        )
        self.set_auth_credentials(env, client_id)

    def set_auth_credentials(self, env, client_id: str) -> None:
        username = env.config.get(ConfigKeys.USER, domain=ConfigKeys.MQTT, default="")
        password = env.config.get(ConfigKeys.PASSWORD, domain=ConfigKeys.MQTT, default="")

        # not using any auth in lab environment
        if username == "":
            return

        mqtt_redis_host = env.config.get(ConfigKeys.HOST, domain=ConfigKeys.MQTT_AUTH)
        mqtt_redis_db = int(env.config.get(ConfigKeys.DB, domain=ConfigKeys.MQTT_AUTH, default=0))
        mqtt_redis_port = 6379

        if ":" in mqtt_redis_host:
            mqtt_redis_host, mqtt_redis_port = mqtt_redis_host.split(":", 1)
            mqtt_redis_port = int(mqtt_redis_port)

        r_client = redis.Redis(
            host=mqtt_redis_host,
            port=mqtt_redis_port,
            db=mqtt_redis_db,
        )

        salt = bcrypt.gensalt()
        hashed_pwd = str(bcrypt.hashpw(bytes(password, "utf-8"), salt), "utf-8")

        # vernemq only supports 2a, not 2b which python's bcrypt
        # produces (hashes are exactly the same regardless, it's
        # a relic from an old OpenBSD bug):
        #
        #   The version $2b$ is not "better" or "stronger" than $2a$.
        #   It is a remnant of one particular buggy implementation of
        #   BCrypt. But since BCrypt canonically belongs to OpenBSD,
        #   they get to change the version marker to whatever they want.
        #
        #   There is no difference between 2a, 2x, 2y, and 2b. If you
        #   wrote your implementation correctly, they all output the
        #   same result.
        #
        # reference: https://stackoverflow.com/questions/15733196/where-2x-prefix-are-used-in-bcrypt
        hashed_pwd = f"$2a${hashed_pwd[4:]}"
        # subscribe_acl = "[{\"pattern\":\"write dms/+\"}]"

        # this is the format that vernemq expects to be in redis; also
        # we don't set a publisher/subscriber acl pattern here, since
        # this user needs to be able to publish to everyone
        mqtt_key = f"[\"\",\"{client_id}\",\"{username}\"]"
        # mqtt_value = "{\"passhash\":\"" + hashed_pwd + "\",\"subscribe_acl\":" + subscribe_acl + "}"
        mqtt_value = "{\"passhash\":\"" + hashed_pwd + "}"

        # need to set it every time, since it has to be unique and
        # pid will change for each worker on startup
        r_client.set(mqtt_key, mqtt_value)

        self.mqtt.set_auth_credentials(
            username=username,
            password=password,
        )

    async def setup(self):
        await self.mqtt.connect(
            self.mqtt_host,
            port=self.mqtt_port,
            version=MQTTv50
        )

    def send(self, user_id: int, fields: dict, qos: int = 1) -> None:
        if self.mqtt is None:
            return

        data = {
            key: value
            for key, value in fields.items()
            if value is not None
        }
        try:
            self.mqtt.publish(
                message_or_topic=f"dms/{self.environment}-{user_id}",
                payload=data,
                qos=qos,
                message_expiry_interval=self.mqtt_ttl
            )
        except Exception as e:
            logger.error(f"could not publish to mqtt: {str(e)}")
            logger.exception(e)


class MqttPublishHandler(IClientPublishHandler):
    def __init__(self, env):
        self.env = env
        self.publisher = MqttPublisher(env)

    async def setup(self):
        try:
            await self.publisher.setup()
        except Exception as e:
            logger.error(f"could not connect to mqtt: {str(e)}")
            logger.exception(e)

    def message(self, message: MessageBase, user_ids: List[int]) -> None:
        data = MqttPublishHandler.message_base_to_event(message, event_type=EventTypes.MESSAGE)
        self.send(user_ids, data)

    def action_log(self, message: MessageBase, user_ids: List[int]) -> None:
        data = MqttPublishHandler.message_base_to_event(message, event_type=EventTypes.ACTION_LOG)
        self.send(user_ids, data)

    def attachment(self, attachment: MessageBase, user_ids: List[int]) -> None:
        data = MqttPublishHandler.message_base_to_event(attachment, event_type=EventTypes.ATTACHMENT)
        self.send(user_ids, data)

    def edit(self, message: MessageBase, user_ids: List[int]) -> None:
        data = MqttPublishHandler.message_base_to_event(message, event_type=EventTypes.EDIT)
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
        # world if they aren't delivered, and vernemq will upgrade the qos for
        # us as specified in the configuration:
        #
        #     upgrade_outgoing_qos = on
        self.send(user_ids, data, qos=0)

    def group_change(self, group_base: GroupBase, user_ids: List[int]) -> None:
        data = MqttPublishHandler.group_base_to_event(group_base, user_ids)
        self.send(user_ids, data)

    def join(self, group_id: str, user_ids: List[int], joiner_ids: List[int], now: float) -> None:
        data = MqttPublishHandler.create_simple_event(EventTypes.JOIN, group_id, now, user_ids=joiner_ids)
        self.send(user_ids, data)

    def leave(self, group_id: str, user_ids: List[int], leaver_id: int, now: float) -> None:
        data = MqttPublishHandler.create_simple_event(EventTypes.LEAVE, group_id, now, user_id=leaver_id)
        self.send(user_ids, data)

    def send(self, user_ids, data, qos: int = 1):
        for user_id in user_ids:
            try:
                self.publisher.send(user_id, data, qos)
            except Exception as e:
                logger.error(f"could not handle message: {str(e)}")
                logger.exception(e)
                self.env.capture_exception(sys.exc_info())
