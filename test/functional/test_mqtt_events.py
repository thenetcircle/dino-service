from dinofw.db.rdbms.schemas import GroupBase
from dinofw.db.storage.schemas import MessageBase
from dinofw.utils import convert
from dinofw.utils import to_ts
from dinofw.utils import utcnow_dt
from test.base import BaseTest
from test.functional.base_functional import BaseServerRestApi


class TestMqttEvents(BaseServerRestApi):
    def test_read_event_sent_with_ms_timestamps(self):
        self.assert_groups_for_user(0)
        self.assert_mqtt_read_events(BaseTest.OTHER_USER_ID, 0)
        group_message = self.send_1v1_message(user_id=BaseTest.USER_ID, receiver_id=8888)
        group_id = group_message["group_id"]

        self.send_1v1_message(user_id=BaseTest.USER_ID, receiver_id=BaseTest.OTHER_USER_ID)
        self.send_1v1_message(user_id=BaseTest.OTHER_USER_ID, receiver_id=BaseTest.USER_ID)

        self.histories_for(group_id, user_id=BaseTest.USER_ID)

        # should have a read receipt event from USER_ID now
        self.assert_mqtt_read_events(BaseTest.OTHER_USER_ID, 1)

    def test_read_event_formatting(self):
        now_dt = utcnow_dt()
        now_ts = to_ts(now_dt)

        event = convert.read_to_event("group_id", BaseTest.USER_ID, now_dt)

        self.assertEqual(event["peer_last_read"], int(now_ts * 1000))
        self.assertEqual(13, len(str(event["peer_last_read"])))

    def test_message_event_formatting(self):
        now_dt = utcnow_dt()
        now_ts = to_ts(now_dt)

        message = MessageBase(
            group_id="group_id",
            created_at=now_dt,
            user_id=BaseTest.USER_ID,
            message_id="msg_id",
            message_type=1,
            file_id="file_id",
            message_payload="payload",
            context="context",
            updated_at=now_dt
        )

        event = convert.message_base_to_event(message)

        for key in {"updated_at", "updated_at"}:
            self.assertEqual(event[key], int(now_ts * 1000))
            self.assertEqual(13, len(str(event[key])))

    def test_group_event_formatting(self):
        now_dt = utcnow_dt()
        now_ts = to_ts(now_dt)

        group = GroupBase(
            group_id="group_id",
            name="name",
            description="desc",
            created_at=now_dt,
            updated_at=now_dt,
            first_message_time=now_dt,
            last_message_time=now_dt,
            last_message_id="1",
            last_message_overview="asdf",
            last_message_type=1,
            last_message_user_id=BaseTest.USER_ID,
            status=1,
            group_type=1,
            owner_id=1,
            meta=1
        )

        event = convert.group_base_to_event(group)

        for key in {"updated_at", "updated_at", "last_message_time"}:
            self.assertEqual(event[key], int(now_ts * 1000))
            self.assertEqual(13, len(str(event[key])))
