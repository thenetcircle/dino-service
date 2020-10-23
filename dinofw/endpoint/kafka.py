import json
import logging
import sys
from abc import abstractmethod
from abc import ABC
from typing import List

from dinofw.endpoint import IServerPublishHandler
from dinofw.endpoint import IServerPublisher
from dinofw.utils.config import ConfigKeys

logging.getLogger("kafka").setLevel(logging.WARNING)
logging.getLogger("kafka.conn").setLevel(logging.WARNING)


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
        self.logger = logging.getLogger(__name__)
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
            self.logger.error("could not publish response: {}".format(str(e)))
            self.logger.exception(e)
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
            default="/var/log/dino/dropped-events.log",
        )

        return _create_logger(d_event_path, "DroppedEvents")

    def drop_msg(self, message):
        try:
            self.dropped_event_log.info(message)
        except Exception as e:
            self.logger.error("could not log dropped message: {}".format(str(e)))
            self.logger.exception(e)
            self.env.capture_exception(sys.exc_info())


class KafkaPublishHandler(IServerPublishHandler):
    def __init__(self, env):
        self.env = env
        self.logger = logging.getLogger(__name__)
        self.publisher = KafkaPublisher(env)

    def delete_attachments(
        self,
        group_id: str,
        message_ids: List[str],
        file_ids: List[str],
        user_ids: List[int],
        now: float
    ) -> None:
        pass  # TODO: implement
