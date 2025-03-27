import arrow

from test.base import BaseTest
from test.functional.base_functional import BaseServerRestApi


class TestBookmark(BaseServerRestApi):
    async def test_removing_bookmark_resets_unread_count(self):
        await self.assert_groups_for_user(0)
        group_message = await self.send_1v1_message(
            user_id=BaseTest.USER_ID,
            receiver_id=BaseTest.OTHER_USER_ID
        )
        group_id = group_message["group_id"]

        stats = (await self.groups_for_user(BaseTest.OTHER_USER_ID, count_unread=True))[0]["stats"]
        self.assertEqual(1, stats["unread"])
        self.assertEqual(False, stats["bookmark"])

        await self.bookmark_group(group_id, bookmark=True, user_id=BaseTest.OTHER_USER_ID)
        stats = (await self.groups_for_user(BaseTest.OTHER_USER_ID, count_unread=True))[0]["stats"]

        # unread here is 2 but actually should be 1, cause a user can't bookmark a group without opening it first,
        # but in this test just check that unread increased by 1
        self.assertEqual(2, stats["unread"])
        self.assertEqual(True, stats["bookmark"])

        await self.bookmark_group(group_id, bookmark=False, user_id=BaseTest.OTHER_USER_ID)
        stats = (await self.groups_for_user(BaseTest.OTHER_USER_ID, count_unread=True))[0]["stats"]
        self.assertEqual(0, stats["unread"])
        self.assertEqual(False, stats["bookmark"])

    async def test_removing_bookmark_resets_highlight(self):
        await self.assert_groups_for_user(0)
        group_message = await self.send_1v1_message(
            user_id=BaseTest.USER_ID,
            receiver_id=BaseTest.OTHER_USER_ID
        )
        group_id = group_message["group_id"]

        stats = (await self.groups_for_user(BaseTest.OTHER_USER_ID, count_unread=True))[0]["stats"]
        self.assertEqual(1, stats["unread"])
        self.assertEqual(self.long_ago, stats["highlight_time"])

        # highlight the group
        await self.highlight_group_for_user(
            group_id,
            user_id=BaseTest.OTHER_USER_ID,
            highlight_time=arrow.utcnow().float_timestamp
        )

        stats = (await self.groups_for_user(BaseTest.OTHER_USER_ID, count_unread=True))[0]["stats"]
        self.assertEqual(1, stats["unread"])
        self.assertEqual(False, stats["bookmark"])
        self.assertLess(self.long_ago, stats["highlight_time"])

        # add the bookmark
        await self.bookmark_group(group_id, bookmark=True, user_id=BaseTest.OTHER_USER_ID)

        stats = (await self.groups_for_user(BaseTest.OTHER_USER_ID, count_unread=True))[0]["stats"]

        # unread here is 2 but actually should be 1, cause a user can't bookmark a group without opening it first,
        # but in this test just check that unread increased by 1
        self.assertEqual(2, stats["unread"])
        self.assertEqual(True, stats["bookmark"])
        self.assertLess(self.long_ago, stats["highlight_time"])

        receiver_stats = (await self.groups_for_user(BaseTest.USER_ID, count_unread=True))[0]["stats"]
        self.assertEqual(stats["highlight_time"], receiver_stats["receiver_highlight_time"])
        self.assertLess(self.long_ago, receiver_stats["receiver_highlight_time"])

        # remove the bookmark should reset highlight time and unread count
        await self.bookmark_group(group_id, bookmark=False, user_id=BaseTest.OTHER_USER_ID)

        stats = (await self.groups_for_user(BaseTest.OTHER_USER_ID, count_unread=True))[0]["stats"]
        self.assertEqual(0, stats["unread"])
        self.assertEqual(False, stats["bookmark"])
        self.assertEqual(self.long_ago, stats["highlight_time"])

        receiver_stats = (await self.groups_for_user(BaseTest.USER_ID, count_unread=True))[0]["stats"]
        self.assertEqual(stats["highlight_time"], receiver_stats["receiver_highlight_time"])
        self.assertEqual(self.long_ago, receiver_stats["receiver_highlight_time"])

    async def test_removing_bookmark_resets_last_read(self):
        await self.assert_groups_for_user(0)
        group_message = await self.send_1v1_message(
            user_id=BaseTest.USER_ID,
            receiver_id=BaseTest.OTHER_USER_ID
        )
        group_id = group_message["group_id"]

        stats = (await self.groups_for_user(BaseTest.OTHER_USER_ID, count_unread=True))[0]["stats"]
        self.assertEqual(1, stats["unread"])
        self.assertEqual(self.long_ago, stats["highlight_time"])

        await self.histories_for(group_id, user_id=BaseTest.OTHER_USER_ID)

        stats = (await self.groups_for_user(BaseTest.OTHER_USER_ID, count_unread=True))[0]["stats"]
        last_read_time = stats["last_read_time"]

        # add the bookmark
        await self.bookmark_group(group_id, bookmark=True, user_id=BaseTest.OTHER_USER_ID)

        stats = (await self.groups_for_user(BaseTest.OTHER_USER_ID, count_unread=True))[0]["stats"]
        self.assertEqual(True, stats["bookmark"])
        self.assertEqual(last_read_time, stats["last_read_time"])

        # send another message
        await self.send_1v1_message(
            user_id=BaseTest.USER_ID,
            receiver_id=BaseTest.OTHER_USER_ID
        )

        # last read time should be same as before
        stats = (await self.groups_for_user(BaseTest.OTHER_USER_ID, count_unread=True))[0]["stats"]
        self.assertEqual(True, stats["bookmark"])
        self.assertEqual(last_read_time, stats["last_read_time"])

        # remove the bookmark should reset last read time
        await self.bookmark_group(group_id, bookmark=False, user_id=BaseTest.OTHER_USER_ID)

        stats = (await self.groups_for_user(BaseTest.OTHER_USER_ID, count_unread=True))[0]["stats"]
        self.assertEqual(False, stats["bookmark"])
        self.assertLess(last_read_time, stats["last_read_time"])
