import logging
import sys
from abc import ABC
from typing import Set

import redis
from gmqtt import Client as MQTTClient
from gmqtt.mqtt.constants import MQTTv50

from dinofw.config import ConfigKeys


class RequestBuilder:
    @staticmethod
    def parse(message: dict) -> (Set[str], dict):
        user_ids = message["user_ids"]
        user_ids = set(user_ids.split(","))
        del message["user_ids"]

        return user_ids, message


class IStreamReader(ABC):
    pass


class StreamReader(IStreamReader):
    def __init__(self, env, host: str, port: int = 6379, db: int = 0):
        self.env = env
        self.logger = logging.getLogger(__name__)

        if env.config.get(ConfigKeys.TESTING, default=False) or host == "mock":
            from fakeredis import FakeStrictRedis

            self.redis_pool = None
            self.redis_instance = FakeStrictRedis(host=host, port=port, db=db)
        else:
            self.redis_pool = redis.ConnectionPool(host=host, port=port, db=db)
            self.redis_instance = None

        self.consumer_stream = env.config.get(ConfigKeys.STREAM, domain=ConfigKeys.PUBLISHER)
        self.consumer_group = env.config.get(ConfigKeys.GROUP, domain=ConfigKeys.PUBLISHER)

        self.mqtt_host = env.config.get(ConfigKeys.HOST, domain=ConfigKeys.MQTT)
        self.mqtt_port = env.config.get(ConfigKeys.PORT, domain=ConfigKeys.MQTT)

        # TODO: unique client id
        self.mqtt = MQTTClient("client-id")

    async def setup(self):
        # TODO: check that we don't recreate stuff unnecessarily with this command
        self.redis.xgroup_create(self.consumer_stream, self.consumer_group, id="$", mkstream=True)

        await self.mqtt.connect(self.mqtt_host, port=self.mqtt_port, version=MQTTv50)

    async def consume(self):
        while True:
            try:
                await self.try_to_consume()
            except Exception as e:
                self.logger.error(f"could not consume message: {str(e)}")
                self.logger.exception(e)
                self.env.capture_exception(sys.exc_info())

    async def try_to_consume(self):
        # TODO: each node needs a unique name
        # TODO: need to keep track of last id each loop?

        for stream, messages in self.redis.xreadgroup(
            groupname=self.consumer_group,
            consumername="foo",
            streams={
                self.consumer_stream: "$"  # "$" means last message
            },
            noack=False,
            block=50,  # ms to wait if no new messages
        ):
            for message in messages:
                await self.handle_message(message)

    async def handle_message(self, message) -> None:
        try:
            user_ids, data = RequestBuilder.parse(message)
        except Exception as e:
            self.logger.error(f"could not parse message: {str(e)}")
            self.logger.exception(e)
            self.env.capture_exception(sys.exc_info())
            return

        for user_id in user_ids:
            try:
                await self.publish_message(user_id, data)
            except Exception as e:
                self.logger.error(f"could not handle message: {str(e)}")
                self.logger.exception(e)
                self.env.capture_exception(sys.exc_info())

    async def publish_message(self, user_id: str, data: dict):
        self.mqtt.publish(
            message_or_topic=user_id,
            payload=data,
            qos=1,
            message_expiry_interval=10
        )

    @property
    def redis(self) -> redis.Redis:
        if self.redis_pool is None:
            return self.redis_instance
        return redis.Redis(connection_pool=self.redis_pool)
