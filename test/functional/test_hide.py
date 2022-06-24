from test.base import BaseTest
from test.functional.base_functional import BaseServerRestApi


class TestHideGroup(BaseServerRestApi):
    def test_bookmark_remains_when_hide_removed(self):
        self.assert_groups_for_user(0)
        group_message = self.send_1v1_message(
            user_id=BaseTest.USER_ID,
            receiver_id=BaseTest.OTHER_USER_ID
        )
        group_id = group_message["group_id"]

        self.assert_groups_for_user(1)
        self.assert_total_unread_count(user_id=BaseTest.USER_ID, unread_count=0)

        self.bookmark_group(group_id=group_id, bookmark=True, user_id=BaseTest.USER_ID)
        self.assert_bookmarked_for_user(bookmark=True, group_id=group_id, user_id=BaseTest.USER_ID)

        # TODO: remove bookmark when hiding a group? helps with unread count
        self.update_hide_group_for(group_id, hide=True, user_id=BaseTest.USER_ID)
        self.assert_hidden_for_user(hidden=True, group_id=group_id, user_id=BaseTest.USER_ID)

        # hidden groups should not be included
        self.assert_groups_for_user(0)

        self.assert_total_unread_count(user_id=BaseTest.USER_ID, unread_count=0)
        self.send_1v1_message(
            user_id=BaseTest.OTHER_USER_ID,
            receiver_id=BaseTest.USER_ID
        )

        # should now be un-hidden, and one two unread messages (bookmark plus the new message)
        self.assert_groups_for_user(1)
        self.assert_total_unread_count(user_id=BaseTest.USER_ID, unread_count=2)

        # should still be bookmarked
        self.assert_bookmarked_for_user(bookmark=True, group_id=group_id, user_id=BaseTest.USER_ID)
        self.assert_hidden_for_user(hidden=False, group_id=group_id, user_id=BaseTest.USER_ID)
