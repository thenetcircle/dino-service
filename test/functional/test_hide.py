from test.base import BaseTest
from test.functional.base_functional import BaseServerRestApi


class TestHideGroup(BaseServerRestApi):
    async def test_bookmark_remains_when_hide_removed(self):
        await self.assert_groups_for_user(0)
        group_message = await self.send_1v1_message(
            user_id=BaseTest.USER_ID,
            receiver_id=BaseTest.OTHER_USER_ID
        )
        group_id = group_message["group_id"]

        await self.assert_groups_for_user(1)
        await self.assert_total_unread_count(user_id=BaseTest.USER_ID, unread_count=0)

        await self.bookmark_group(group_id=group_id, bookmark=True, user_id=BaseTest.USER_ID)
        await self.assert_bookmarked_for_user(bookmark=True, group_id=group_id, user_id=BaseTest.USER_ID)

        # TODO: remove bookmark when hiding a group? helps with unread count
        await self.update_hide_group_for(group_id, hide=True, user_id=BaseTest.USER_ID)
        await self.assert_hidden_for_user(hidden=True, group_id=group_id, user_id=BaseTest.USER_ID)

        # hidden groups should not be included
        await self.assert_groups_for_user(0)

        await self.assert_total_unread_count(user_id=BaseTest.USER_ID, unread_count=0)
        await self.send_1v1_message(
            user_id=BaseTest.OTHER_USER_ID,
            receiver_id=BaseTest.USER_ID
        )

        # should now be un-hidden, and one unread message (bookmark only counts as
        # unread if there's no actual unread messages)
        await self.assert_groups_for_user(1)

        # bookmark is removed on a new message
        await self.assert_total_unread_count(user_id=BaseTest.USER_ID, unread_count=1)

        # should still be bookmarked
        await self.assert_bookmarked_for_user(bookmark=True, group_id=group_id, user_id=BaseTest.USER_ID)
        await self.assert_hidden_for_user(hidden=False, group_id=group_id, user_id=BaseTest.USER_ID)
