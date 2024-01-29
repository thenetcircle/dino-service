import json
import time

import arrow

from dinofw.rest.queries import AbstractQuery
from dinofw.utils import utcnow_ts, to_dt, users_to_group_id
from dinofw.utils.config import MessageTypes, ErrorCodes
from test.base import BaseTest
from test.functional.base_functional import BaseServerRestApi


class TestIncludeDeletedMsgs(BaseServerRestApi):
    def test_include_deleted_in_history(self):
        group_id = users_to_group_id(BaseTest.USER_ID, BaseTest.OTHER_USER_ID)
        creation_time = -1

        for _ in range(5):
            msg = self.send_1v1_message()
            creation_time = msg["created_at"]

        self.assertEqual(5, len(self.histories_for(group_id)["messages"]))

        self.update_delete_before(
            group_id=group_id,
            delete_before=creation_time
        )

        self.assertEqual(0, len(self.histories_for(group_id)["messages"]))

        for _ in range(5):
            self.send_1v1_message()

        self.assertEqual(5, len(self.histories_for(group_id)["messages"]))

        # including deleted we should get the previous 5 plus the new 5
        self.assertEqual(10, len(self.histories_for(
            group_id=group_id,
            admin=True,
            include_deleted=True
        )["messages"]))
