import json
import logging
import sys
from abc import ABC
from abc import abstractmethod
from typing import List

from dinofw.config import ConfigKeys
from dinofw.db.cassandra.schemas import MessageBase
from dinofw.utils import IPublisher
from dinofw.utils.activity import ActivityBuilder


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
    def __init__(self, env, mock=False):
        self.env = env
        self.topic = self.env.config.get(ConfigKeys.TOPIC, domain=ConfigKeys.KAFKA)
        self.logger = logging.getLogger(__name__)

        if mock:
            self.producer = MockProducer()
            return

        self.writer_factory = KafkaWriterFactory()

        bootstrap_servers = self.env.config.get(ConfigKeys.HOST, domain=ConfigKeys.KAFKA).split(",")
        self.producer = self.writer_factory.create_producer(
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            bootstrap_servers=bootstrap_servers,
        )

    def message(self, group_id: str, user_id: int, message: MessageBase, user_ids: List[int]) -> None:
        event = ActivityBuilder.activity_for_client_api_send(group_id, user_id, message, user_ids)

        try:
            self.producer.send(self.topic, event, key=group_id)
        except Exception as e:
            self.logger.error("could not publish response: {}".format(str(e)))
            self.logger.exception(e)
            self.env.capture_exception(sys.exc_info())
