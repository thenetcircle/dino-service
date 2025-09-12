import time

from test.base import BaseTest
from test.functional.base_db import BaseDatabaseTest
from test.functional.base_functional import BaseServerRestApi


class TestUpdateUserStats(BaseServerRestApi):
    @BaseServerRestApi.init_db_session
    async def test_set_last_updated_at_one_group(self):
        # create one group
        m1 = await self.send_1v1_message(receiver_id=1234)

        s1_before = await self.get_user_stats(group_id=m1["group_id"])
        self.assertEqual(s1_before["stats"]["last_updated_time"], m1["created_at"])

        time.sleep(0.05)

        session = self.env.db_session
        await self.env.db.set_last_updated_at_on_all_stats_related_to_user(
            BaseDatabaseTest.USER_ID, db=session
        )

        s1_after = await self.get_user_stats(group_id=m1["group_id"])
        self.assertGreater(s1_after["stats"]["last_updated_time"], s1_before["stats"]["last_updated_time"])
        self.assertGreater(s1_after["stats"]["last_updated_time"], m1["created_at"])

    @BaseServerRestApi.init_db_session
    async def test_set_last_updated_at_two_groups(self):
        # create two group
        m1 = await self.send_1v1_message(receiver_id=2345)
        m2 = await self.send_1v1_message(receiver_id=4567)

        s1_before = await self.get_user_stats(group_id=m1["group_id"])
        s2_before = await self.get_user_stats(group_id=m2["group_id"])
        self.assertEqual(s1_before["stats"]["last_updated_time"], m1["created_at"])
        self.assertEqual(s2_before["stats"]["last_updated_time"], m2["created_at"])

        time.sleep(0.05)

        session = self.env.db_session
        await self.env.db.set_last_updated_at_on_all_stats_related_to_user(
            BaseDatabaseTest.USER_ID, db=session
        )

        s1_after = await self.get_user_stats(group_id=m1["group_id"])
        s2_after = await self.get_user_stats(group_id=m2["group_id"])
        self.assertGreater(s1_after["stats"]["last_updated_time"], s1_before["stats"]["last_updated_time"])
        self.assertGreater(s2_after["stats"]["last_updated_time"], s2_before["stats"]["last_updated_time"])

        self.assertGreater(s1_after["stats"]["last_updated_time"], m1["created_at"])
        self.assertGreater(s2_after["stats"]["last_updated_time"], m2["created_at"])

    @BaseServerRestApi.init_db_session
    async def test_update_last_read_uses_fast_path(self):
        m1 = await self.send_1v1_message(receiver_id=BaseTest.OTHER_USER_ID)

        s1 = await self.get_user_stats(group_id=m1["group_id"], user_id=BaseTest.OTHER_USER_ID)
        self.assertEqual(1, s1["stats"]["unread"])

        time.sleep(0.05)
        await self.update_last_read(m1["group_id"], BaseTest.OTHER_USER_ID)

        s1 = await self.get_user_stats(group_id=m1["group_id"], user_id=BaseTest.OTHER_USER_ID)
        self.assertEqual(0, s1["stats"]["unread"])
