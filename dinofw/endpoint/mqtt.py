import asyncio
import json
import socket
import sys
from datetime import datetime as dt
from typing import List

import bcrypt
import psutil
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
from dinofw.utils.convert import group_base_to_event
from dinofw.utils.convert import message_base_to_event
from dinofw.utils.convert import read_to_event


def get_worker_index():
    """
    to reuse client ids but still keep them unique among the workers/servers
    """
    this_process = psutil.Process()
    this_pid = this_process.pid

    siblings = [
        p.pid for p in
        this_process.parent().children()
    ]

    siblings = sorted(siblings)
    worker_index = siblings.index(this_pid)

    return worker_index


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
        worker_index = get_worker_index()
        hostname = socket.gethostname().split(".")[0]
        client_id = f"dinoms-{hostname}-{worker_index}"
        logger.debug(f"using mqtt client id '{client_id}'")

        self.mqtt = MQTTClient(
            client_id=client_id,
            session_expiry_interval=60,
            clean_session=True,

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
        username = env.config.get(ConfigKeys.USER, domain=ConfigKeys.MQTT, default="").strip()
        password = env.config.get(ConfigKeys.PASSWORD, domain=ConfigKeys.MQTT, default="").strip()
        auth_type = env.config.get(ConfigKeys.TYPE, domain=ConfigKeys.MQTT_AUTH, default="redis")

        # auth disabled
        if username == "" or auth_type == "disabled":
            logger.debug("mqtt auth is disabled")
            return

        if auth_type == "redis":
            self.set_auth_redis(env, client_id, username, password)
        elif auth_type == "mysql":
            self.set_auth_mysql(env, client_id, username, password)
        else:
            raise ValueError(f"unknown MQTT auth type: {auth_type}")

        self.mqtt.set_auth_credentials(
            username=username,
            password=password,
        )

    def set_auth_redis(self, env, client_id, username, password):
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
        publish_acl = '[{"pattern":"dms/' + self.environment + '/+"}]'

        # this is the format that vernemq expects to be in redis; also
        # we don't set a publisher/subscriber acl pattern here, since
        # this user needs to be able to publish to everyone
        mqtt_key = f'["","{client_id}","{username}"]'
        mqtt_value = '{"passhash":"' + hashed_pwd + '","publish_acl":' + publish_acl + '}'

        # need to set it every time, since it has to be unique and
        # pid will change for each worker on startup
        r_client.set(mqtt_key, mqtt_value)
        logger.debug(f"set mqtt auth in redis to key {mqtt_key} and value {mqtt_value}")

    def set_auth_mysql(self, env, client_id, username, password):
        import MySQLdb
        import hashlib

        mqtt_mysql_host = env.config.get(ConfigKeys.HOST, domain=ConfigKeys.MQTT_AUTH)
        mqtt_mysql_db = env.config.get(ConfigKeys.DB, domain=ConfigKeys.MQTT_AUTH)
        mqtt_mysql_user = env.config.get(ConfigKeys.USER, domain=ConfigKeys.MQTT_AUTH)
        mqtt_mysql_pass = env.config.get(ConfigKeys.PASSWORD, domain=ConfigKeys.MQTT_AUTH)

        db = MySQLdb.connect(
            host=mqtt_mysql_host,
            user=mqtt_mysql_user,
            passwd=mqtt_mysql_pass,
            db=mqtt_mysql_db
        )

        cur = db.cursor()
        cur.execute(
            "insert into vmq_auth_acl(mountpoint,client_id,username,password,publish_acl,subscribe_acl) values(%s,%s,%s,%s,%s,%s)",
            ("", client_id, username, hashlib.sha256(password.encode()).hexdigest(), '[{"pattern":"#"}]', '[{"pattern":"#"}]')
        )
        db.commit()
        db.close()

    async def setup(self):
        logger.debug(f"mqtt connecting to host {self.mqtt_host} port {self.mqtt_port}")
        await self.mqtt.connect(
            self.mqtt_host,
            port=self.mqtt_port,
            version=MQTTv50
        )
        logger.debug("mqtt connected successfully!")

    async def stop(self):
        if self.mqtt is not None:
            await self.mqtt.disconnect()

    def send(self, user_id: int, fields: dict, qos: int = 0) -> None:
        if self.mqtt is None:
            logger.warning("mqtt instance is none!")
            return

        data = {
            key: value
            for key, value in fields.items()
            if value is not None
        }

        logger.debug(f"sending mqtt event to user {user_id}: {json.dumps(data)}")
        try:
            self.mqtt.publish(
                message_or_topic=f"dms/{self.environment}/{user_id}",
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

    async def stop(self):
        await self.publisher.stop()

    def action_log(self, message: MessageBase, user_ids: List[int]) -> None:
        data = message_base_to_event(message, event_type=EventTypes.ACTION_LOG)
        self.send(user_ids, data)

    def attachment(self, attachment: MessageBase, user_ids: List[int], group: GroupBase) -> None:
        data = message_base_to_event(
            attachment,
            event_type=EventTypes.ATTACHMENT,
            group=group
        )
        self.send(user_ids, data)

    def edit(self, message: MessageBase, user_ids: List[int]) -> None:
        data = message_base_to_event(message, event_type=EventTypes.EDIT)
        self.send(user_ids, data)

    def read(self, group_id: str, user_id: int, user_ids: List[int], now: dt, bookmark: bool) -> None:
        # only send read receipt to 1v1 groups
        if len(user_ids) > 2:
            return

        data = read_to_event(group_id, user_id, now, bookmark)
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
        # world if they aren't delivered, and vernemq should upgrade the qos for
        # us anyway as specified in the configuration:
        #
        #     upgrade_outgoing_qos = on
        self.send(user_ids, data, qos=0)

    def group_change(self, group_base: GroupBase, user_ids: List[int]) -> None:
        data = group_base_to_event(group_base, user_ids)
        self.send(user_ids, data)

    def join(self, group_id: str, user_ids: List[int], joiner_ids: List[int], now: float) -> None:
        data = MqttPublishHandler.create_simple_event(EventTypes.JOIN, group_id, now, user_ids=joiner_ids)
        self.send(user_ids, data)

    def leave(self, group_id: str, user_ids: List[int], leaver_id: int, now: float) -> None:
        data = MqttPublishHandler.create_simple_event(EventTypes.LEAVE, group_id, now, user_id=leaver_id)
        self.send(user_ids, data)

    def send(self, user_ids, data, qos: int = 0):
        for user_id in user_ids:
            try:
                self.publisher.send(user_id, data, qos)
            except Exception as e:
                logger.error(f"could not handle message: {str(e)}")
                logger.exception(e)
                self.env.capture_exception(sys.exc_info())

    def send_to_one(self, user_id: int, data, qos: int = 0):
        try:
            self.publisher.send(user_id, data, qos)
        except Exception as e:
            logger.error(f"could not handle message: {str(e)}")
            logger.exception(e)
            self.env.capture_exception(sys.exc_info())
