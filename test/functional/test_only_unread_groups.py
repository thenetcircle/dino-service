from test.base import BaseTest
from test.functional.base_functional import BaseServerRestApi


class TestOnlyUnreadGroups(BaseServerRestApi):
    async def test_only_unread(self):
        await self.assert_groups_for_user(0)
        await self.send_1v1_message(user_id=BaseTest.USER_ID, receiver_id=8888)
        await self.send_1v1_message(user_id=BaseTest.USER_ID, receiver_id=9999)
        await self.send_1v1_message(user_id=9999, receiver_id=BaseTest.USER_ID)

        groups = await self.groups_for_user(user_id=BaseTest.USER_ID, count_unread=False, only_unread=False)
        self.assertEqual(2, len(groups))

        groups = await self.groups_for_user(user_id=BaseTest.USER_ID, count_unread=False, only_unread=True)
        self.assertEqual(1, len(groups))

        groups = await self.groups_for_user(user_id=BaseTest.USER_ID, count_unread=True, only_unread=False)
        self.assertEqual(2, len(groups))

        groups = await self.groups_for_user(user_id=BaseTest.USER_ID, count_unread=True, only_unread=True)
        self.assertEqual(1, len(groups))

    async def test_hide_only_unread(self):
        await self.assert_groups_for_user(0)
        group_message = await self.send_1v1_message(user_id=BaseTest.USER_ID, receiver_id=8888)
        group_id = group_message["group_id"]

        await self.send_1v1_message(user_id=BaseTest.USER_ID, receiver_id=9999)
        await self.send_1v1_message(user_id=8888, receiver_id=BaseTest.USER_ID)

        groups = await self.groups_for_user(user_id=BaseTest.USER_ID, count_unread=True, only_unread=True)
        self.assertEqual(1, len(groups))

        await self.update_hide_group_for(group_id, hide=True, user_id=BaseTest.USER_ID)
        await self.assert_hidden_for_user(True, group_id, user_id=BaseTest.USER_ID)

        groups = await self.groups_for_user(user_id=BaseTest.USER_ID, count_unread=True, only_unread=True)
        self.assertEqual(0, len(groups))

        await self.histories_for(group_id, user_id=BaseTest.USER_ID)
        await self.assert_hidden_for_user(False, group_id, user_id=BaseTest.USER_ID)

        groups = await self.groups_for_user(user_id=BaseTest.USER_ID, count_unread=True, only_unread=False)
        self.assertEqual(2, len(groups))

        groups = await self.groups_for_user(user_id=BaseTest.USER_ID, count_unread=True, only_unread=True)
        self.assertEqual(0, len(groups))

        await self.send_1v1_message(user_id=8888, receiver_id=BaseTest.USER_ID)

        await self.assert_hidden_for_user(False, group_id, user_id=BaseTest.USER_ID)
        groups = await self.groups_for_user(user_id=BaseTest.USER_ID, count_unread=True, only_unread=True)
        self.assertEqual(1, len(groups))

        await self.update_hide_group_for(group_id, hide=True, user_id=BaseTest.USER_ID)
        await self.assert_hidden_for_user(True, group_id, user_id=BaseTest.USER_ID)

        groups = await self.groups_for_user(user_id=BaseTest.USER_ID, count_unread=True, only_unread=True)
        self.assertEqual(0, len(groups))
