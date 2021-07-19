import json
import logging
import sys
from abc import ABC
from abc import abstractmethod
from datetime import datetime as dt
from typing import List

from loguru import logger
from strict_rfc3339 import timestamp_to_rfc3339_utcoffset

from dinofw.db.storage.schemas import MessageBase
from dinofw.endpoint import IServerPublishHandler
from dinofw.endpoint import IServerPublisher
from dinofw.rest.queries import AbstractQuery
from dinofw.utils import split_into_chunks
from dinofw.utils.activity import ActivityBuilder
from dinofw.utils.config import ConfigKeys

logging.getLogger("kafka").setLevel(logging.WARNING)
logging.getLogger("kafka.conn").setLevel(logging.WARNING)


def to_rfc3339(date: dt):
    return timestamp_to_rfc3339_utcoffset(AbstractQuery.to_ts(date))


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


class KafkaPublisher(IServerPublisher):
    def __init__(self, env):
        self.env = env

        self.writer_factory = None
        self.dropped_event_log = None
        self.topic = None
        self.producer = None

    def setup(self):
        self.writer_factory = KafkaWriterFactory()
        self.dropped_event_log = self.create_loggers()

        bootstrap_servers = self.env.config.get(ConfigKeys.HOST, domain=ConfigKeys.KAFKA)

        self.topic = self.env.config.get(ConfigKeys.TOPIC, domain=ConfigKeys.KAFKA)
        self.producer = self.writer_factory.create_producer(
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            bootstrap_servers=bootstrap_servers,
        )

    def send(self, data: dict) -> None:
        try:
            key = data.get("actor", dict()).get("id", None)
            if key is not None:
                key = bytes(key, "utf-8")

            self.try_to_publish(data, key=key)
        except Exception as e:
            logger.error("could not publish response: {}".format(str(e)))
            logger.exception(e)
            self.env.capture_exception(sys.exc_info())
            self.drop_msg(data)

    def try_to_publish(self, message, key: bytes = None) -> None:
        if key is None:
            self.producer.send(self.topic, message)
        else:
            self.producer.send(self.topic, message, key=key)

    def create_loggers(self):
        def _create_logger(_path: str, _name: str) -> logging.Logger:
            msg_formatter = logging.Formatter("%(asctime)s: %(message)s")

            msg_handler = logging.FileHandler(_path)
            msg_handler.setFormatter(msg_formatter)

            msg_logger = logging.getLogger(_name)
            msg_logger.setLevel(logging.INFO)
            msg_logger.addHandler(msg_handler)
            return msg_logger

        d_event_path = self.env.config.get(
            ConfigKeys.DROPPED_EVENT_FILE,
            domain=ConfigKeys.KAFKA,
            default="dropped-events.log",
        )

        return _create_logger(d_event_path, "DroppedEvents")

    def drop_msg(self, message):
        try:
            self.dropped_event_log.info(message)
        except Exception as e:
            logger.error("could not log dropped message: {}".format(str(e)))
            logger.exception(e)
            self.env.capture_exception(sys.exc_info())


class KafkaPublishHandler(IServerPublishHandler):
    def __init__(self, env):
        self.env = env
        self.publisher = KafkaPublisher(env)

    def setup(self):
        self.publisher.setup()

    def delete_attachments(
        self,
        group_id: str,
        attachments: List[MessageBase],
        user_ids: List[int],
        now: float
    ) -> None:
        # batch it in case there's a ton of images, don't want the events to become too large
        for attachments_chunk in split_into_chunks(attachments, 100):
            event = self.generate_event(group_id, attachments_chunk)
            logger.info("sending event to kafka:")
            logger.info(event)
            self.publisher.send(event)

    def generate_event(self, group_id: str, attachments: List[MessageBase]) -> dict:
        # same owner for all attachments
        owner_id = attachments[0].user_id

        return ActivityBuilder.enrich(self.env, {
            "actor": {
                "id": str(owner_id),
            },
            "verb": "delete",
            "title": "messenger.attachment.delete",
            "object": {
                "objectType": "files",
                "attachments": [{
                    "objectType": str(attachment.message_type),
                    "content": attachment.message_payload,
                    "id": attachment.message_id,
                    "published": to_rfc3339(attachment.created_at)
                } for attachment in attachments]
            },
            "target": {
                "objectType": "group",
                "content": group_id,
            },
        })
