from dinofw.utils.config import GroupStatus
from test.base import BaseTest
from test.functional.base_functional import BaseServerRestApi

import asyncio

class TestCountGroups(BaseServerRestApi):
    async def test_count_groups_including_deleted(self):
        await self.send_1v1_message(user_id=BaseTest.USER_ID, receiver_id=BaseTest.OTHER_USER_ID)
        await self.send_1v1_message(user_id=BaseTest.USER_ID, receiver_id=BaseTest.THIRD_USER_ID)
        msg = await self.send_1v1_message(user_id=BaseTest.USER_ID, receiver_id=BaseTest.FOURTH_USER_ID)

        await asyncio.sleep(0.02)

        groups = await self.groups_for_user(BaseTest.USER_ID)
        self.assertEqual(3, len(groups))

        # we should call `delete_all_groups()`, but that will async delete the groups, which makes this test flaky
        await self.user_leaves_group(msg["group_id"], BaseTest.USER_ID)

        # without specifying deleted, the default of False should count only non-deleted groups
        stats = await self.get_global_user_stats(BaseTest.USER_ID)
        self.assertEqual(2, stats["one_to_one_amount"])

        # with deleted=None, both deleted and non-deleted groups should be returned
        stats = await self.get_global_user_stats(BaseTest.USER_ID, set_deleted_to=-1)
        self.assertEqual(3, stats["one_to_one_amount"])

        # with deleted=True, it should return only one group, the one we deleted above
        stats = await self.get_global_user_stats(BaseTest.USER_ID, set_deleted_to=1)
        self.assertEqual(1, stats["one_to_one_amount"])

        # with deleted=False, it should return the two non-deleted groups
        stats = await self.get_global_user_stats(BaseTest.USER_ID, set_deleted_to=0)
        self.assertEqual(2, stats["one_to_one_amount"])
