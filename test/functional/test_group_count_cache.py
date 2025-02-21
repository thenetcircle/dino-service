from test.base import BaseTest
from test.functional.base_functional import BaseServerRestApi


class TestGroupCountCache(BaseServerRestApi):
    async def test_count_increases(self):
        the_count = await self.env.cache.get_count_group_types_for_user(user_id=BaseTest.USER_ID, hidden=False)
        self.assertIsNone(the_count)

        await self.send_1v1_message(
            user_id=BaseTest.USER_ID,
            receiver_id=BaseTest.OTHER_USER_ID
        )
        await self.get_global_user_stats(BaseTest.USER_ID, hidden=False, count_unread=True)
        the_count = await self.env.cache.get_count_group_types_for_user(user_id=BaseTest.USER_ID, hidden=False)
        self.assertEqual([(1, 1), (0, 0), (2, 0)], the_count)

        await self.send_1v1_message(
            user_id=BaseTest.USER_ID,
            receiver_id=BaseTest.THIRD_USER_ID
        )
        the_count = await self.env.cache.get_count_group_types_for_user(user_id=BaseTest.USER_ID, hidden=False)
        self.assertEqual([(1, 2), (0, 0), (2, 0)], the_count)
