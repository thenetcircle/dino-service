from dinofw.endpoint.kafka import KafkaPublishHandler
from test.base import BaseTest
from uuid import uuid4 as uuid
# from activitystreams import parse as parse_activity


class TestKafkaPublisher(BaseTest):
    def test_generate_event(self):
        file_ids = [f"{str(uuid()).replace('-', '')}".upper() for _ in range(10)]

        handler = KafkaPublishHandler(self.fake_env)
        event = handler.generate_event(BaseTest.GROUP_ID, file_ids)
        # activity = parse_activity(event)

        # self.assertEqual(10, len(activity.object.attachments))
        self.assertEqual(10, len(event["object"]["attachments"]))
