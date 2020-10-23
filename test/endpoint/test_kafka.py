import arrow

from dinofw.db.storage.schemas import MessageBase
from dinofw.endpoint.kafka import KafkaPublishHandler
from dinofw.utils.config import MessageTypes
from test.base import BaseTest
from uuid import uuid4 as uuid


class TestKafkaPublisher(BaseTest):
    def test_generate_event(self):
        messages = [MessageBase(
            file_id=file_id,
            group_id=BaseTest.GROUP_ID,
            user_id=BaseTest.USER_ID,
            created_at=arrow.utcnow().datetime,
            message_id=str(uuid()),
            message_type=MessageTypes.IMAGE,
        ) for file_id in [f"{str(uuid()).replace('-', '').upper()}.jpg" for _ in range(10)]]

        handler = KafkaPublishHandler(self.fake_env)
        event = handler.generate_event(BaseTest.GROUP_ID, messages)

        self.assertEqual(len(messages), len(event["object"]["attachments"]))
