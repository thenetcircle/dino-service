import time

import arrow

from dinofw.utils.config import GroupTypes
from test.base import BaseTest
from test.functional.base_functional import BaseServerRestApi


class TestServerRestApi(BaseServerRestApi):
    def test_export_messages_until_now(self):
        group_id = self.create_and_join_group(
            user_id=BaseTest.USER_ID,
            group_type=GroupTypes.PUBLIC_ROOM
        )
        self.user_joins_group(
            group_id=group_id,
            user_id=BaseTest.OTHER_USER_ID
        )

        msgs_to_send = 5
        self.send_message_to_group_from(group_id=group_id, amount=msgs_to_send)

        # 5 messages plus 1 action log for the second user joining
        self.assert_messages_in_group(group_id=group_id, amount=msgs_to_send + 1)

        messages = self.export_messages_in_group(group_id=group_id)
        self.assertEqual(len(messages), msgs_to_send + 1)

        until = messages[-1]["created_at"]
        time.sleep(0.05)
        self.send_message_to_group_from(group_id=group_id, amount=msgs_to_send)

        # second batch is included
        messages = self.export_messages_in_group(group_id=group_id)
        self.assertEqual(len(messages), msgs_to_send * 2 + 1)

        # the second batch of 5 messages should now not be included in the export results
        messages = self.export_messages_in_group(group_id=group_id, until=until)
        self.assertEqual(len(messages), msgs_to_send + 1)

    def test_export_messages_with_limit(self):
        group_id = self.create_and_join_group(
            user_id=BaseTest.USER_ID,
            group_type=GroupTypes.PUBLIC_ROOM
        )
        self.user_joins_group(
            group_id=group_id,
            user_id=BaseTest.OTHER_USER_ID
        )

        limit = 7
        msgs_to_send = limit * 2

        self.send_message_to_group_from(group_id=group_id, amount=msgs_to_send)

        # 14 messages plus 1 action log for the second user joining
        self.assert_messages_in_group(group_id=group_id, amount=msgs_to_send + 1)

        # test per_page/limit
        messages = self.export_messages_in_group(group_id=group_id, per_page=limit)
        self.assertEqual(len(messages), limit)

    def test_export_messages_since_and_until_different_order(self):
        group_id = self.create_and_join_group(
            user_id=BaseTest.USER_ID,
            group_type=GroupTypes.PUBLIC_ROOM
        )
        self.user_joins_group(
            group_id=group_id,
            user_id=BaseTest.OTHER_USER_ID
        )

        limit = 1
        msgs_to_send = 3

        self.send_message_to_group_from(group_id=group_id, amount=msgs_to_send)
        self.assert_messages_in_group(group_id=group_id, amount=msgs_to_send + 1)

        time.sleep(0.05)
        until = arrow.utcnow().float_timestamp

        messages_until = self.export_messages_in_group(group_id=group_id, per_page=limit, until=until)
        messages_since = self.export_messages_in_group(group_id=group_id, per_page=limit, since=0)

        self.assertNotEqual(messages_until[0]["message_id"], messages_since[0]["message_id"])