import json
import time

import arrow

from dinofw.utils import utcnow_ts, to_dt
from dinofw.utils.config import MessageTypes, ErrorCodes
from test.base import BaseTest
from test.functional.base_functional import BaseServerRestApi


class TestServerRestApi(BaseServerRestApi):
    async def test_get_groups_for_user_before_joining(self):
        await self.assert_groups_for_user(0)

    async def test_get_groups_for_user_after_joining(self):
        await self.create_and_join_group()
        await self.assert_groups_for_user(1)

    async def test_leaving_a_group(self):
        await self.assert_groups_for_user(0)

        group_id = await self.create_and_join_group()
        await self.assert_groups_for_user(1)

        await self.user_leaves_group(group_id)
        await self.assert_groups_for_user(0)

    async def test_another_user_joins_group(self):
        await self.assert_groups_for_user(0, user_id=BaseTest.USER_ID)
        await self.assert_groups_for_user(0, user_id=BaseTest.OTHER_USER_ID)

        # first user joins, check that other user isn't in any groups
        group_id = await self.create_and_join_group()
        await self.assert_groups_for_user(1, user_id=BaseTest.USER_ID)
        await self.assert_groups_for_user(0, user_id=BaseTest.OTHER_USER_ID)

        # other user also joins, check that both are in a group now
        await self.user_joins_group(group_id, user_id=BaseTest.OTHER_USER_ID)
        await self.assert_groups_for_user(1, user_id=BaseTest.USER_ID)
        await self.assert_groups_for_user(1, user_id=BaseTest.OTHER_USER_ID)

    async def test_users_in_group(self):
        await self.assert_groups_for_user(0, user_id=BaseTest.USER_ID)
        await self.assert_groups_for_user(0, user_id=BaseTest.OTHER_USER_ID)

        # first user joins, check that other user isn't in any groups
        group_id = await self.create_and_join_group()
        await self.assert_groups_for_user(1, user_id=BaseTest.USER_ID)
        await self.assert_groups_for_user(0, user_id=BaseTest.OTHER_USER_ID)

        # other user also joins, check that both are in a group now
        await self.user_joins_group(group_id, user_id=BaseTest.OTHER_USER_ID)
        await self.assert_groups_for_user(1, user_id=BaseTest.USER_ID)
        await self.assert_groups_for_user(1, user_id=BaseTest.OTHER_USER_ID)

    async def test_update_user_statistics_in_group(self):
        group_id = await self.create_and_join_group()

        now_ts = await self.update_user_stats_to_now(group_id, BaseTest.USER_ID)
        user_stats = await self.get_user_stats(group_id, BaseTest.USER_ID)

        self.assertEqual(group_id, user_stats["stats"]["group_id"])
        self.assertEqual(BaseTest.USER_ID, user_stats["stats"]["user_id"])
        self.assertEqual(now_ts, user_stats["stats"]["last_read_time"])

    async def test_group_unhidden_on_new_message_for_all_users(self):
        # both users join a new group
        group_id = await self.create_and_join_group(BaseTest.USER_ID)
        await self.user_joins_group(group_id, BaseTest.OTHER_USER_ID)

        # the group should not be hidden for either user at this time
        await self.assert_hidden_for_user(False, group_id, BaseTest.USER_ID)
        await self.assert_hidden_for_user(False, group_id, BaseTest.OTHER_USER_ID)

        # both users should have the group in the list
        await self.assert_groups_for_user(1, user_id=BaseTest.USER_ID)
        await self.assert_groups_for_user(1, user_id=BaseTest.OTHER_USER_ID)

        # hide the group for the other user
        await self.update_hide_group_for(group_id, True, BaseTest.OTHER_USER_ID)

        # make sure the group is hidden for the other user
        await self.assert_hidden_for_user(False, group_id, BaseTest.USER_ID)
        await self.assert_hidden_for_user(True, group_id, BaseTest.OTHER_USER_ID)

        # other user doesn't have any groups since he hid it
        await self.assert_groups_for_user(1, user_id=BaseTest.USER_ID)
        await self.assert_groups_for_user(0, user_id=BaseTest.OTHER_USER_ID)

        # sending a message should un-hide the group for all users in it
        await self.send_message_to_group_from(group_id, BaseTest.USER_ID)

        # should not be hidden anymore for any user
        await self.assert_hidden_for_user(False, group_id, BaseTest.USER_ID)
        await self.assert_hidden_for_user(False, group_id, BaseTest.OTHER_USER_ID)

        # both users have 1 group now since none is hidden anymore
        await self.assert_groups_for_user(1, user_id=BaseTest.USER_ID)
        await self.assert_groups_for_user(1, user_id=BaseTest.OTHER_USER_ID)

    async def test_one_user_deletes_some_history(self):
        # both users join a new group
        group_id = await self.create_and_join_group(BaseTest.USER_ID)
        await self.user_joins_group(group_id, BaseTest.OTHER_USER_ID)

        # 'join' action log should exist for both user
        await self.assert_messages_in_group(group_id, user_id=BaseTest.USER_ID, amount=1)
        await self.assert_messages_in_group(
            group_id, user_id=BaseTest.OTHER_USER_ID, amount=1
        )

        # each user sends 4 messages each, then we delete some of them for one user
        messages_to_send_each = 4

        await self.send_message_to_group_from(
            group_id, user_id=BaseTest.USER_ID, amount=messages_to_send_each
        )
        messages = await self.send_message_to_group_from(
            group_id,
            user_id=BaseTest.OTHER_USER_ID,
            amount=messages_to_send_each,
        )

        # first user deletes the first 5 messages in the group
        await self.update_delete_before(
            group_id, delete_before=messages[0]["created_at"], user_id=BaseTest.USER_ID
        )

        # first user should have 3, since we deleted everything before the other user's
        # first message (including that first message); second user should have all 8
        # since he/she didn't delete anything plus 1 more for the 'join' action log
        await self.assert_messages_in_group(
            group_id, user_id=BaseTest.USER_ID, amount=messages_to_send_each - 1
        )
        await self.assert_messages_in_group(
            group_id, user_id=BaseTest.OTHER_USER_ID, amount=messages_to_send_each * 2 + 1
        )

    async def test_joining_a_group_changes_last_update_time(self):
        group_id = await self.create_and_join_group(BaseTest.USER_ID)
        group = await self.get_group(group_id)
        last_update_time = group["group"]["updated_at"]

        # update time should have changed from sending a new message
        await self.send_message_to_group_from(group_id, user_id=BaseTest.USER_ID)
        group = await self.get_group(group_id)
        self.assertNotEqual(group["group"]["updated_at"], last_update_time)

        # update time should now have changed
        await self.user_joins_group(group_id, BaseTest.OTHER_USER_ID)
        group = await self.get_group(group_id)
        self.assertNotEqual(group["group"]["updated_at"], last_update_time)

    async def test_total_unread_count_changes_when_user_read_time_changes(self):
        group_id1 = await self.create_and_join_group(BaseTest.USER_ID)

        await self.user_joins_group(group_id1, BaseTest.OTHER_USER_ID)
        await self.send_message_to_group_from(
            group_id1, user_id=BaseTest.USER_ID, amount=10
        )
        await self.assert_total_unread_count(user_id=BaseTest.USER_ID, unread_count=0)
        await self.assert_total_unread_count(user_id=BaseTest.OTHER_USER_ID, unread_count=10)

        group_id2 = await self.create_and_join_group(BaseTest.USER_ID)

        await self.user_joins_group(group_id2, BaseTest.OTHER_USER_ID)
        await self.send_message_to_group_from(
            group_id2, user_id=BaseTest.USER_ID, amount=10
        )
        await self.assert_total_unread_count(user_id=BaseTest.USER_ID, unread_count=0)
        await self.assert_total_unread_count(user_id=BaseTest.OTHER_USER_ID, unread_count=20)

        # sending a message should mark the group as "read"
        await self.send_message_to_group_from(group_id2, user_id=BaseTest.OTHER_USER_ID)
        await self.assert_total_unread_count(user_id=BaseTest.USER_ID, unread_count=1)
        # TODO: this is 20 when running, check where it fails
        await self.assert_total_unread_count(user_id=BaseTest.OTHER_USER_ID, unread_count=10)

        # first user should now have 2 unread
        await self.send_message_to_group_from(group_id2, user_id=BaseTest.OTHER_USER_ID)
        await self.assert_total_unread_count(user_id=BaseTest.USER_ID, unread_count=2)
        await self.assert_total_unread_count(user_id=BaseTest.OTHER_USER_ID, unread_count=10)

        # first user should now have 3 unread
        await self.send_message_to_group_from(group_id1, user_id=BaseTest.OTHER_USER_ID)
        await self.assert_total_unread_count(user_id=BaseTest.USER_ID, unread_count=3)
        await self.assert_total_unread_count(user_id=BaseTest.OTHER_USER_ID, unread_count=0)

    async def test_pin_group_changes_ordering(self):
        group_id1 = await self.create_and_join_group(BaseTest.USER_ID)
        group_id2 = await self.create_and_join_group(BaseTest.USER_ID)

        await self.send_message_to_group_from(group_id1, user_id=BaseTest.USER_ID)

        # group 2 should now be on top
        await self.send_message_to_group_from(group_id2, user_id=BaseTest.USER_ID)
        await self.assert_order_of_groups(BaseTest.USER_ID, group_id2, group_id1)

        # should be in the other order after pinning
        await self.pin_group_for(group_id1, BaseTest.USER_ID)
        await self.assert_order_of_groups(BaseTest.USER_ID, group_id1, group_id2)

        # should not change order since group 1 is pinned
        await self.send_message_to_group_from(group_id2, user_id=BaseTest.USER_ID)
        await self.assert_order_of_groups(BaseTest.USER_ID, group_id1, group_id2)

        # after unpinning the group with the latest message should be first
        await self.unpin_group_for(group_id1, BaseTest.USER_ID)
        await self.assert_order_of_groups(BaseTest.USER_ID, group_id2, group_id1)

    async def test_group_exists_when_leaving(self):
        groups = await self.groups_for_user(BaseTest.USER_ID)
        self.assertEqual(0, len(groups))

        group_id = await self.create_and_join_group(BaseTest.USER_ID)
        groups = await self.groups_for_user(BaseTest.USER_ID)
        self.assertEqual(1, len(groups))

        await self.user_leaves_group(group_id)
        groups = await self.groups_for_user(BaseTest.USER_ID)
        self.assertEqual(0, len(groups))

    async def test_highlight_group_for_other_user(self):
        group_id1 = await self.create_and_join_group(BaseTest.USER_ID)
        group_id2 = await self.create_and_join_group(BaseTest.USER_ID)

        await self.user_joins_group(group_id1, BaseTest.OTHER_USER_ID)
        await self.user_joins_group(group_id2, BaseTest.OTHER_USER_ID)

        await self.send_message_to_group_from(group_id1, user_id=BaseTest.USER_ID)
        await self.send_message_to_group_from(group_id2, user_id=BaseTest.USER_ID)
        await self.assert_order_of_groups(BaseTest.USER_ID, group_id2, group_id1)

        await self.highlight_group_for_user(group_id1, BaseTest.USER_ID)
        await self.assert_order_of_groups(BaseTest.USER_ID, group_id1, group_id2)

    async def test_highlight_makes_group_unhidden(self):
        group_id = await self.create_and_join_group(BaseTest.USER_ID)
        await self.user_joins_group(group_id, BaseTest.OTHER_USER_ID)

        # just joined a group, should have one
        groups = await self.groups_for_user(BaseTest.USER_ID)
        self.assertEqual(1, len(groups))

        # after hiding we should not have any groups anymore
        await self.update_hide_group_for(group_id, hide=True, user_id=BaseTest.USER_ID)
        groups = await self.groups_for_user(BaseTest.USER_ID)
        self.assertEqual(0, len(groups))

        # make sure it becomes unhidden if highlighted by someone
        await self.highlight_group_for_user(group_id, BaseTest.USER_ID)
        groups = await self.groups_for_user(BaseTest.USER_ID)
        self.assertEqual(1, len(groups))

    async def _test_delete_highlight_changes_order(self):
        # TODO: don't delete hightlight, just call the history api for the group

        group_id1 = await self.create_and_join_group(BaseTest.USER_ID)
        group_id2 = await self.create_and_join_group(BaseTest.USER_ID)

        await self.user_joins_group(group_id1, BaseTest.OTHER_USER_ID)
        await self.user_joins_group(group_id2, BaseTest.OTHER_USER_ID)

        await self.send_message_to_group_from(group_id1, user_id=BaseTest.USER_ID)
        await self.send_message_to_group_from(group_id2, user_id=BaseTest.USER_ID)
        await self.assert_order_of_groups(BaseTest.USER_ID, group_id2, group_id1)

        await self.highlight_group_for_user(group_id1, BaseTest.USER_ID)
        await self.assert_order_of_groups(BaseTest.USER_ID, group_id1, group_id2)

        # back to normal
        await self.delete_highlight_group_for_user(group_id1, BaseTest.USER_ID)
        await self.assert_order_of_groups(BaseTest.USER_ID, group_id2, group_id1)

    async def _test_highlight_ordered_higher_than_pin(self):
        # TODO: don't delete hightlight, just call the history api for the group

        group_1 = await self.create_and_join_group(BaseTest.USER_ID)
        group_2 = await self.create_and_join_group(BaseTest.USER_ID)
        group_3 = await self.create_and_join_group(BaseTest.USER_ID)

        # first send a message to each group with a short delay
        for group_id in [group_1, group_2, group_3]:
            await self.send_message_to_group_from(
                group_id, user_id=BaseTest.USER_ID
            )

        # last group to receive a message should be on top
        await self.assert_order_of_groups(BaseTest.USER_ID, group_3, group_2, group_1)

        # pinned a group should put it at the top
        await self.pin_group_for(group_2, user_id=BaseTest.USER_ID)
        await self.assert_order_of_groups(BaseTest.USER_ID, group_2, group_3, group_1)

        # highlight has priority over pinning, so group 1 should be above group 2 now
        await self.highlight_group_for_user(group_1, user_id=BaseTest.USER_ID)
        await self.assert_order_of_groups(BaseTest.USER_ID, group_1, group_2, group_3)

        # sending a message to group 3 should not change anything, since 1 and 2 are highlighted and pinned respectively
        await self.send_message_to_group_from(group_3, user_id=BaseTest.USER_ID)
        await self.assert_order_of_groups(BaseTest.USER_ID, group_1, group_2, group_3)

        # group 2 and 3 are pinned, but 3 has more recent message now
        await self.pin_group_for(group_3, user_id=BaseTest.USER_ID)
        await self.assert_order_of_groups(BaseTest.USER_ID, group_1, group_3, group_2)

        # group 1 has now the older message and not highlighted so should be at the bottom
        await self.delete_highlight_group_for_user(group_1, BaseTest.USER_ID)
        await self.assert_order_of_groups(BaseTest.USER_ID, group_3, group_2, group_1)

        # group 2 should be on top after highlighting it
        await self.highlight_group_for_user(group_2, user_id=BaseTest.USER_ID)
        await self.assert_order_of_groups(BaseTest.USER_ID, group_2, group_3, group_1)

        # group 1 has a more recent highlight than group 2
        await self.highlight_group_for_user(group_1, user_id=BaseTest.USER_ID)
        await self.assert_order_of_groups(BaseTest.USER_ID, group_1, group_2, group_3)

        # pinning group 2 should not change anything since highlight has priority over pinning
        await self.pin_group_for(group_2, user_id=BaseTest.USER_ID)
        await self.assert_order_of_groups(BaseTest.USER_ID, group_1, group_2, group_3)

    async def test_change_group_name(self):
        await self.create_and_join_group(BaseTest.USER_ID)

        groups = await self.groups_for_user(BaseTest.USER_ID)
        original_name = groups[0]["group"]["name"]
        self.assertIsNotNone(original_name)

        new_name = "new test name for group"
        await self.edit_group(groups[0]["group"]["group_id"], name=new_name)

        groups = await self.groups_for_user(BaseTest.USER_ID)
        self.assertEqual(groups[0]["group"]["name"], new_name)
        self.assertNotEqual(original_name, new_name)

    async def test_change_group_owner(self):
        await self.create_and_join_group(BaseTest.USER_ID)

        groups = await self.groups_for_user(BaseTest.USER_ID)
        self.assertEqual(BaseTest.USER_ID, groups[0]["group"]["owner_id"])

        await self.edit_group(groups[0]["group"]["group_id"], owner=BaseTest.OTHER_USER_ID)
        groups = await self.groups_for_user(BaseTest.USER_ID)
        self.assertEqual(BaseTest.OTHER_USER_ID, groups[0]["group"]["owner_id"])

    async def test_receiver_unread_count(self):
        await self.send_1v1_message(
            user_id=BaseTest.USER_ID, receiver_id=BaseTest.OTHER_USER_ID
        )
        groups = await self.groups_for_user(user_id=BaseTest.USER_ID, count_unread=True)

        self.assertEqual(groups[0]["stats"]["unread"], 0)
        self.assertEqual(groups[0]["stats"]["receiver_unread"], 1)

        await self.send_1v1_message(
            user_id=BaseTest.USER_ID, receiver_id=BaseTest.OTHER_USER_ID
        )
        groups = await self.groups_for_user(user_id=BaseTest.USER_ID, count_unread=True)

        self.assertEqual(groups[0]["stats"]["unread"], 0)
        self.assertEqual(groups[0]["stats"]["receiver_unread"], 2)

        groups = await self.groups_for_user(user_id=BaseTest.OTHER_USER_ID, count_unread=True)

        self.assertEqual(groups[0]["stats"]["unread"], 2)
        self.assertEqual(groups[0]["stats"]["receiver_unread"], 0)

        await self.send_1v1_message(
            user_id=BaseTest.OTHER_USER_ID, receiver_id=BaseTest.USER_ID
        )
        groups = await self.groups_for_user(user_id=BaseTest.OTHER_USER_ID, count_unread=True)

        self.assertEqual(groups[0]["stats"]["unread"], 0)
        self.assertEqual(groups[0]["stats"]["receiver_unread"], 1)

    async def test_unread_count_is_negative_if_query_says_do_not_count(self):
        await self.send_1v1_message(
            user_id=BaseTest.USER_ID, receiver_id=BaseTest.OTHER_USER_ID
        )
        groups = await self.groups_for_user(user_id=BaseTest.USER_ID, count_unread=False, receiver_stats=False)

        self.assertEqual(groups[0]["stats"]["unread"], -1)
        self.assertEqual(groups[0]["stats"]["receiver_unread"], -1)

    async def test_last_updated_at_changes_on_send_msg(self):
        await self.send_1v1_message(user_id=BaseTest.USER_ID, receiver_id=BaseTest.OTHER_USER_ID)
        groups = await self.groups_for_user(user_id=BaseTest.USER_ID, count_unread=False)
        last_updated_at = groups[0]["stats"]["last_updated_time"]

        await self.send_1v1_message(
            user_id=BaseTest.USER_ID, receiver_id=BaseTest.OTHER_USER_ID
        )

        groups = await self.groups_for_user(user_id=BaseTest.USER_ID, count_unread=False)
        self.assertGreater(groups[0]["stats"]["last_updated_time"], last_updated_at)

    async def test_create_action_log_in_all_groups_for_user(self):
        group_1 = (await self.send_1v1_message())["group_id"]
        group_2 = await self.create_and_join_group()
        group_3 = await self.create_and_join_group()

        stats = await self.get_global_user_stats(hidden=False)
        self.assertEqual(2, stats["group_amount"])
        self.assertEqual(1, stats["one_to_one_amount"])

        # this group should only have 1 message, which we sent
        self.assertEqual(1, len((await self.histories_for(group_1))["messages"]))

        # these two should have no messages
        self.assertEqual(0, len((await self.histories_for(group_2))["messages"]))
        self.assertEqual(0, len((await self.histories_for(group_3))["messages"]))

        await self.create_action_log_in_all_groups_for_user()

        # all groups should now have one extra message, the action log we created
        self.assertEqual(2, len((await self.histories_for(group_1))["messages"]))
        self.assertEqual(1, len((await self.histories_for(group_2))["messages"]))
        self.assertEqual(1, len((await self.histories_for(group_3))["messages"]))

    async def test_last_updated_at_changes_on_highlight(self):
        await self.send_1v1_message(user_id=BaseTest.USER_ID, receiver_id=BaseTest.OTHER_USER_ID)
        groups = await self.groups_for_user(user_id=BaseTest.USER_ID, count_unread=False)
        last_updated_at = groups[0]["stats"]["last_updated_time"]

        await self.highlight_group_for_user(
            group_id=groups[0]["group"]["group_id"],
            user_id=BaseTest.USER_ID
        )

        groups = await self.groups_for_user(user_id=BaseTest.USER_ID, count_unread=False)
        self.assertGreater(groups[0]["stats"]["last_updated_time"], last_updated_at)

    async def test_last_updated_at_changes_on_bookmark_true(self):
        await self.send_1v1_message(user_id=BaseTest.USER_ID, receiver_id=BaseTest.OTHER_USER_ID)
        groups = await self.groups_for_user(user_id=BaseTest.USER_ID, count_unread=False)
        last_updated_at = groups[0]["stats"]["last_updated_time"]

        await self.bookmark_group(
            group_id=groups[0]["group"]["group_id"],
            user_id=BaseTest.USER_ID,
            bookmark=True,
        )

        groups = await self.groups_for_user(user_id=BaseTest.USER_ID, count_unread=False)
        self.assertGreater(groups[0]["stats"]["last_updated_time"], last_updated_at)

    async def test_last_updated_at_changes_on_bookmark_false(self):
        await self.send_1v1_message(user_id=BaseTest.USER_ID, receiver_id=BaseTest.OTHER_USER_ID)
        groups = await self.groups_for_user(user_id=BaseTest.USER_ID, count_unread=False)
        last_updated_at = groups[0]["stats"]["last_updated_time"]

        await self.bookmark_group(
            group_id=groups[0]["group"]["group_id"],
            user_id=BaseTest.USER_ID,
            bookmark=True,
        )

        groups = await self.groups_for_user(user_id=BaseTest.USER_ID, count_unread=False)
        bookmarked_updated_at = groups[0]["stats"]["last_updated_time"]
        self.assertGreater(bookmarked_updated_at, last_updated_at)

        await self.bookmark_group(
            group_id=groups[0]["group"]["group_id"],
            user_id=BaseTest.USER_ID,
            bookmark=False,
        )

        groups = await self.groups_for_user(user_id=BaseTest.USER_ID, count_unread=False)
        not_bookmarked_updated_at = groups[0]["stats"]["last_updated_time"]
        self.assertGreater(not_bookmarked_updated_at, bookmarked_updated_at)

    async def test_last_updated_at_changes_on_hide_true(self):
        await self.send_1v1_message(user_id=BaseTest.USER_ID, receiver_id=BaseTest.OTHER_USER_ID)
        groups = await self.groups_for_user(user_id=BaseTest.USER_ID, count_unread=False)
        last_updated_at = groups[0]["stats"]["last_updated_time"]

        await self.update_hide_group_for(
            group_id=groups[0]["group"]["group_id"],
            user_id=BaseTest.USER_ID,
            hide=True
        )

        groups = await self.groups_for_user(user_id=BaseTest.USER_ID, count_unread=False, hidden=True)
        self.assertGreater(groups[0]["stats"]["last_updated_time"], last_updated_at)

    async def test_last_updated_at_changes_on_hide_false(self):
        await self.send_1v1_message(user_id=BaseTest.USER_ID, receiver_id=BaseTest.OTHER_USER_ID)
        groups = await self.groups_for_user(user_id=BaseTest.USER_ID, count_unread=False)
        last_updated_at = groups[0]["stats"]["last_updated_time"]

        await self.update_hide_group_for(
            group_id=groups[0]["group"]["group_id"],
            user_id=BaseTest.USER_ID,
            hide=True
        )

        groups = await self.groups_for_user(user_id=BaseTest.USER_ID, count_unread=False, hidden=True)
        hidden_updated_at = groups[0]["stats"]["last_updated_time"]
        self.assertGreater(hidden_updated_at, last_updated_at)

        await self.update_hide_group_for(
            group_id=groups[0]["group"]["group_id"],
            user_id=BaseTest.USER_ID,
            hide=False
        )

        groups = await self.groups_for_user(user_id=BaseTest.USER_ID, count_unread=False)
        not_hidden_updated_at = groups[0]["stats"]["last_updated_time"]
        self.assertGreater(not_hidden_updated_at, hidden_updated_at)

    async def test_last_updated_at_changes_on_name_change(self):
        await self.send_1v1_message(user_id=BaseTest.USER_ID, receiver_id=BaseTest.OTHER_USER_ID)
        groups = await self.groups_for_user(user_id=BaseTest.USER_ID, count_unread=False)
        last_updated_at = groups[0]["stats"]["last_updated_time"]

        new_name = "new test name for group"
        await self.edit_group(groups[0]["group"]["group_id"], name=new_name)

        groups = await self.groups_for_user(user_id=BaseTest.USER_ID, count_unread=False)
        new_updated_at = groups[0]["stats"]["last_updated_time"]
        self.assertGreater(new_updated_at, last_updated_at)

    async def test_last_updated_at_changed_on_update_attachment(self):
        group_message = await self.send_1v1_message(message_type=MessageTypes.IMAGE)
        self.assertEqual(MessageTypes.IMAGE, group_message["message_type"])
        groups = await self.groups_for_user(user_id=BaseTest.USER_ID, count_unread=False)
        last_updated_at = groups[0]["stats"]["last_updated_time"]

        await self.update_attachment(
            group_message["message_id"], group_message["created_at"]
        )

        groups = await self.groups_for_user(user_id=BaseTest.USER_ID, count_unread=False)
        new_updated_at = groups[0]["stats"]["last_updated_time"]
        self.assertGreater(new_updated_at, last_updated_at)

    async def test_last_updated_at_not_changed_on_create_attachment(self):
        await self.send_1v1_message(user_id=BaseTest.USER_ID, receiver_id=BaseTest.OTHER_USER_ID)
        groups = await self.groups_for_user(user_id=BaseTest.USER_ID, count_unread=False)
        last_updated_at = groups[0]["stats"]["last_updated_time"]

        group_message = await self.send_1v1_message(message_type=MessageTypes.IMAGE)
        self.assertEqual(MessageTypes.IMAGE, group_message["message_type"])

        groups = await self.groups_for_user(user_id=BaseTest.USER_ID, count_unread=False)
        create_attachment_updated_at = groups[0]["stats"]["last_updated_time"]

        # TODO: do we want to update it on template creation as well as when processing is done?
        self.assertGreater(create_attachment_updated_at, last_updated_at)

    async def test_last_updated_at_changes_on_no_more_unread(self):
        await self.send_1v1_message(
            user_id=BaseTest.USER_ID, receiver_id=BaseTest.OTHER_USER_ID
        )

        groups = await self.groups_for_user(user_id=BaseTest.OTHER_USER_ID)
        last_updated_at = groups[0]["stats"]["last_updated_time"]

        await self.histories_for(
            groups[0]["group"]["group_id"], user_id=BaseTest.OTHER_USER_ID
        )

        groups = await self.groups_for_user(user_id=BaseTest.OTHER_USER_ID)
        self.assertGreater(groups[0]["stats"]["last_updated_time"], last_updated_at)

    async def test_last_updated_at_changes_on_new_message_other_user(self):
        await self.send_1v1_message(
            user_id=BaseTest.USER_ID, receiver_id=BaseTest.OTHER_USER_ID
        )

        groups = await self.groups_for_user(
            user_id=BaseTest.OTHER_USER_ID, count_unread=False
        )
        last_updated_at = groups[0]["stats"]["last_updated_time"]

        await self.send_1v1_message(
            user_id=BaseTest.USER_ID, receiver_id=BaseTest.OTHER_USER_ID
        )

        groups = await self.groups_for_user(
            user_id=BaseTest.OTHER_USER_ID, count_unread=False
        )
        self.assertGreater(groups[0]["stats"]["last_updated_time"], last_updated_at)

    async def test_get_groups_updated_since(self):
        when = utcnow_ts() - 100

        groups = await self.groups_updated_since(user_id=BaseTest.OTHER_USER_ID, since=when)
        self.assertEqual(0, len(groups))

        await self.send_1v1_message(
            user_id=BaseTest.USER_ID, receiver_id=BaseTest.OTHER_USER_ID
        )

        groups = await self.groups_updated_since(user_id=BaseTest.OTHER_USER_ID, since=when)
        self.assertEqual(1, len(groups))

        groups = await self.groups_updated_since(
            user_id=BaseTest.OTHER_USER_ID, since=when + 500
        )
        self.assertEqual(0, len(groups))

    async def test_update_attachment(self):
        group_message = await self.send_1v1_message(message_type=MessageTypes.IMAGE)
        self.assertEqual(MessageTypes.IMAGE, group_message["message_type"])

        history = await self.histories_for(group_message["group_id"])
        all_attachments = await self.attachments_for(group_message["group_id"])

        # a 'placeholder' message should have been created, but no attachment
        self.assertEqual(1, len(history["messages"]))
        self.assertEqual(0, len(all_attachments))

        attachment = await self.update_attachment(
            group_message["message_id"], group_message["created_at"]
        )
        history = await self.histories_for(group_message["group_id"])
        all_attachments = await self.attachments_for(group_message["group_id"])

        # now the message should have been updated, and the attachment created
        self.assertEqual(group_message["message_id"], attachment["message_id"])
        self.assertNotEqual(attachment["created_at"], attachment["updated_at"])
        self.assertEqual(1, len(history["messages"]))
        self.assertEqual(1, len(all_attachments))

    async def test_count_group_types_in_user_stats(self):
        stats = await self.get_global_user_stats(hidden=False)
        self.assertEqual(0, stats["group_amount"])
        self.assertEqual(0, stats["one_to_one_amount"])

        await self.send_1v1_message()

        stats = await self.get_global_user_stats(hidden=False)
        self.assertEqual(0, stats["group_amount"])
        self.assertEqual(1, stats["one_to_one_amount"])

        await self.create_and_join_group()

        stats = await self.get_global_user_stats(hidden=False)
        self.assertEqual(1, stats["group_amount"])
        self.assertEqual(1, stats["one_to_one_amount"])

        await self.create_and_join_group()

        stats = await self.get_global_user_stats(hidden=False)
        self.assertEqual(2, stats["group_amount"])
        self.assertEqual(1, stats["one_to_one_amount"])

        await self.send_1v1_message(receiver_id=8844)

        stats = await self.get_global_user_stats(hidden=False)
        self.assertEqual(2, stats["group_amount"])
        self.assertEqual(2, stats["one_to_one_amount"])

    async def test_user_stats_group_read_and_send_times(self):
        stats = await self.get_global_user_stats(hidden=False)
        self.assertEqual(0, stats["group_amount"])
        self.assertEqual(0, stats["one_to_one_amount"])

        group_message = await self.send_1v1_message()
        stats = await self.get_global_user_stats(hidden=False)

        last_sent_time_first = stats["last_sent_time"]
        last_sent_group_id = stats["last_sent_group_id"]

        self.assertEqual(group_message["group_id"], last_sent_group_id)
        self.assertIsNotNone(last_sent_time_first)

        group_message = await self.send_1v1_message()
        stats = await self.get_global_user_stats(hidden=False)

        last_sent_time_second = stats["last_sent_time"]
        last_sent_group_id = stats["last_sent_group_id"]

        self.assertEqual(group_message["group_id"], last_sent_group_id)
        self.assertNotEqual(last_sent_time_first, last_sent_time_second)

    async def test_create_attachment_updates_group_overview(self):
        group_message = await self.send_1v1_message()

        histories = await self.histories_for(group_message["group_id"])
        self.assertEqual(1, len(histories["messages"]))

        groups = await self.groups_for_user()
        last_msg_overview = groups[0]["group"]["last_message_overview"]

        await self.update_attachment(group_message["message_id"], group_message["created_at"])

        groups = await self.groups_for_user()
        new_msg_overview = groups[0]["group"]["last_message_overview"]

        self.assertNotEqual(last_msg_overview, new_msg_overview)

        histories = await self.histories_for(group_message["group_id"])
        self.assertEqual(1, len(histories["messages"]))

    async def test_receiver_highlight_exists_in_group_list(self):
        group_message = await self.send_1v1_message()

        groups = await self.groups_for_user()
        receiver_highlight_time = groups[0]["stats"]["receiver_highlight_time"]
        self.assertIsNotNone(receiver_highlight_time)

        await self.highlight_group_for_user(group_message["group_id"], user_id=BaseTest.OTHER_USER_ID)

        groups = await self.groups_for_user()
        new_receiver_highlight_time = groups[0]["stats"]["receiver_highlight_time"]

        self.assertNotEqual(receiver_highlight_time, new_receiver_highlight_time)

    async def test_receiver_hide_exists_in_group_list(self):
        group_message = await self.send_1v1_message()

        groups = await self.groups_for_user()
        receiver_hide = groups[0]["stats"]["receiver_hide"]
        hide = groups[0]["stats"]["hide"]
        self.assertFalse(receiver_hide)
        self.assertFalse(hide)

        await self.update_hide_group_for(group_message["group_id"], hide=True, user_id=BaseTest.OTHER_USER_ID)

        groups = await self.groups_for_user()
        new_receiver_hide = groups[0]["stats"]["receiver_hide"]
        new_hide = groups[0]["stats"]["hide"]

        self.assertTrue(new_receiver_hide)
        self.assertFalse(new_hide)

    async def test_receiver_delete_before_in_group_list(self):
        group_message = await self.send_1v1_message()

        groups = await self.groups_for_user()
        receiver_delete_before = groups[0]["stats"]["receiver_delete_before"]
        delete_before = groups[0]["stats"]["delete_before"]
        self.assertEqual(receiver_delete_before, delete_before)

        delete_time = utcnow_ts()
        await self.update_delete_before(group_message["group_id"], delete_time, user_id=BaseTest.OTHER_USER_ID)

        groups = await self.groups_for_user()
        new_receiver_delete_before = groups[0]["stats"]["receiver_delete_before"]
        new_delete_before = groups[0]["stats"]["delete_before"]

        self.assertEqual(delete_before, new_delete_before)
        self.assertEqual(delete_time, new_receiver_delete_before)
        self.assertNotEqual(receiver_delete_before, new_receiver_delete_before)

    async def test_update_bookmark(self):
        group_message = await self.send_1v1_message()

        groups = await self.groups_for_user()
        bookmark = groups[0]["stats"]["bookmark"]
        self.assertFalse(bookmark)

        await self.bookmark_group(group_message["group_id"], bookmark=True)

        groups = await self.groups_for_user()
        bookmark = groups[0]["stats"]["bookmark"]
        self.assertTrue(bookmark)

        await self.bookmark_group(group_message["group_id"], bookmark=False)

        groups = await self.groups_for_user()
        bookmark = groups[0]["stats"]["bookmark"]
        self.assertFalse(bookmark)

    async def test_mark_all_groups_as_read_removes_bookmark(self):
        group_message = await self.send_1v1_message()
        await self.bookmark_group(group_message["group_id"], bookmark=True)

        stats = (await self.groups_for_user())[0]["stats"]
        self.assertTrue(stats["bookmark"])

        await self.mark_as_read()

        stats = (await self.groups_for_user())[0]["stats"]
        self.assertFalse(stats["bookmark"])

    async def test_mark_all_groups_as_read_resets_count(self):
        await self.send_1v1_message(user_id=BaseTest.OTHER_USER_ID, receiver_id=BaseTest.USER_ID)

        stats = (await self.groups_for_user(count_unread=True))[0]["stats"]
        self.assertEqual(1, stats["unread"])

        await self.mark_as_read()

        stats = (await self.groups_for_user(count_unread=True))[0]["stats"]
        self.assertEqual(0, stats["unread"])

    async def test_groups_for_user_only_unread_includes_bookmarks(self):
        group_message = await self.send_1v1_message()
        groups = await self.groups_for_user(only_unread=True)
        self.assertEqual(0, len(groups))

        await self.bookmark_group(group_message["group_id"], bookmark=True)

        groups = await self.groups_for_user(only_unread=True)
        self.assertEqual(1, len(groups))
        self.assertEqual(group_message["group_id"], groups[0]["group"]["group_id"])

    async def test_bookmark_removed_on_get_histories(self):
        group_message = await self.send_1v1_message()
        groups = await self.groups_for_user(only_unread=True)
        self.assertEqual(0, len(groups))

        await self.bookmark_group(group_message["group_id"], bookmark=True)
        groups = await self.groups_for_user(only_unread=True)
        self.assertEqual(1, len(groups))

        await self.histories_for(group_message["group_id"])

        groups = await self.groups_for_user(only_unread=True)
        self.assertEqual(0, len(groups))

    async def test_new_message_wakeup_users(self):
        group_message = await self.send_1v1_message()
        groups = await self.groups_for_user(only_unread=True)
        self.assertEqual(0, len(groups))

        await self.update_hide_group_for(group_message["group_id"], hide=True)

        # make sure it's hidden
        group_and_stats = await self.groups_for_user(hidden=True)
        self.assertEqual(1, len(group_and_stats))
        self.assertTrue(group_and_stats[0]["stats"]["hide"])

        # should still be hidden
        group_and_stats = await self.groups_for_user(hidden=True)
        self.assertEqual(1, len(group_and_stats))
        self.assertTrue(group_and_stats[0]["stats"]["hide"])

        group_and_stats = await self.groups_for_user()
        self.assertEqual(0, len(group_and_stats))

        # try to wake up the users
        await self.send_1v1_message(user_id=BaseTest.OTHER_USER_ID, receiver_id=BaseTest.USER_ID)

        # should have woken up now
        group_and_stats = await self.groups_for_user()
        self.assertEqual(1, len(group_and_stats))
        self.assertFalse(group_and_stats[0]["stats"]["hide"])

    async def test_new_message_resets_delete_before(self):
        group_message = await self.send_1v1_message(user_id=BaseTest.USER_ID, receiver_id=BaseTest.OTHER_USER_ID)

        group_and_stats = await self.groups_for_user()
        join_time = group_and_stats[0]["stats"]["join_time"]
        delete_before_original = group_and_stats[0]["stats"]["delete_before"]

        # delete before is 1 second before join time, otherwise when creating groups without
        # sending a message to it, the group would not show up in `/groups` api
        self.assertEqual(join_time, delete_before_original + 1)

        yesterday = round(arrow.utcnow().shift(days=-1).float_timestamp, 3)
        await self.update_delete_before(group_message["group_id"], delete_before=yesterday)

        group_and_stats = await self.groups_updated_since(user_id=BaseTest.USER_ID, since=1560000000)
        delete_before_updated = group_and_stats[0]["stats"]["delete_before"]

        self.assertEqual(yesterday, delete_before_updated)
        self.assertNotEqual(join_time, delete_before_updated)

        # update it again so the next message resets it to join_time
        now = utcnow_ts()
        await self.update_delete_before(group_message["group_id"], delete_before=now)

        # should not reset 'delete_before'
        await self.send_1v1_message(user_id=BaseTest.OTHER_USER_ID, receiver_id=BaseTest.USER_ID)

        group_and_stats = await self.groups_for_user()
        delete_before_auto_updated = group_and_stats[0]["stats"]["delete_before"]

        self.assertNotEqual(join_time, delete_before_auto_updated)
        self.assertEqual(now, delete_before_auto_updated)

    async def test_create_action_log_updating_delete_before(self):
        group_message = await self.send_1v1_message(user_id=BaseTest.USER_ID, receiver_id=BaseTest.OTHER_USER_ID)
        group_id = group_message["group_id"]

        histories = await self.histories_for(group_id)
        self.assertEqual(1, len(histories["messages"]))

        yesterday = round(arrow.utcnow().shift(days=-1).float_timestamp, 3)
        await self.update_delete_before(group_id, delete_before=yesterday, create_action_log=True)

        histories = await self.histories_for(group_id)
        self.assertEqual(2, len(histories["messages"]))

    async def test_create_action_log_automatically_created_group(self):
        # this group doesn't exist yet
        log = await self.create_action_log(
            user_id=BaseTest.USER_ID,
            receiver_id=BaseTest.OTHER_USER_ID
        )

        self.assertIsNotNone(log["group_id"])

    async def test_delete_all_groups_for_user(self):
        await self.send_1v1_message()

        groups = await self.groups_for_user()
        self.assertEqual(1, len(groups))

        await self.leave_all_groups()

        groups = await self.groups_for_user()
        self.assertEqual(0, len(groups))

        await self.send_1v1_message(receiver_id=1000)
        await self.send_1v1_message(receiver_id=1001)
        await self.send_1v1_message(receiver_id=1002)
        await self.send_1v1_message(receiver_id=1003)

        groups = await self.groups_for_user()
        self.assertEqual(4, len(groups))

        await self.leave_all_groups()

        groups = await self.groups_for_user()
        self.assertEqual(0, len(groups))

    async def test_get_attachment_from_file_id_returns_no_such_attachment(self):
        group_message = await self.send_1v1_message(message_type=MessageTypes.IMAGE)
        attachment = await self.attachment_for_file_id(group_message["group_id"], BaseTest.FILE_ID, assert_response=False)
        self.assert_error(attachment, ErrorCodes.NO_SUCH_ATTACHMENT)

    async def test_get_attachment_from_file_id_returns_no_such_group(self):
        attachment = await self.attachment_for_file_id(BaseTest.GROUP_ID, BaseTest.FILE_ID, assert_response=False)
        self.assert_error(attachment, ErrorCodes.NO_SUCH_GROUP)

    async def test_get_attachment_from_file_id_returns_ok(self):
        group_message = await self.send_1v1_message(message_type=MessageTypes.IMAGE)
        message_id = group_message["message_id"]
        created_at = group_message["created_at"]
        group_id = group_message["group_id"]

        await self.update_attachment(message_id, created_at, payload=json.dumps({
            "file_id": BaseTest.FILE_ID,
            "context": BaseTest.FILE_CONTEXT
        }))

        attachment = await self.attachment_for_file_id(group_id, BaseTest.FILE_ID)

        self.assertIsNotNone(attachment)
        self.assertIn(BaseTest.FILE_ID, attachment["message_payload"])
        self.assertEqual(attachment["message_type"], MessageTypes.IMAGE)

    async def test_read_receipt_published_when_opening_conversation(self):
        group_message = await self.send_1v1_message()
        self.assertEqual(0, len(self.env.client_publisher.sent_reads))

        await self.histories_for(group_message["group_id"], user_id=BaseTest.OTHER_USER_ID)

        # USER_ID should have gotten a read-receipt from OTHER_USER_ID
        self.assertEqual(1, len(self.env.client_publisher.sent_reads[BaseTest.USER_ID]))
        self.assertEqual(BaseTest.OTHER_USER_ID, self.env.client_publisher.sent_reads[BaseTest.USER_ID][0][1])

    async def test_read_receipt_not_duplicated_when_opening_conversation(self):
        group_message = await self.send_1v1_message()
        self.assertEqual(0, len(self.env.client_publisher.sent_reads))

        await self.histories_for(group_message["group_id"], user_id=BaseTest.OTHER_USER_ID)

        # USER_ID should have gotten a read-receipt from OTHER_USER_ID
        self.assertEqual(1, len(self.env.client_publisher.sent_reads[BaseTest.USER_ID]))
        self.assertEqual(BaseTest.OTHER_USER_ID, self.env.client_publisher.sent_reads[BaseTest.USER_ID][0][1])

        await self.histories_for(group_message["group_id"], user_id=BaseTest.OTHER_USER_ID)

        # should not have another one
        self.assertEqual(1, len(self.env.client_publisher.sent_reads[BaseTest.USER_ID]))
        self.assertEqual(BaseTest.OTHER_USER_ID, self.env.client_publisher.sent_reads[BaseTest.USER_ID][0][1])

    async def test_hidden_groups_is_not_counted_in_user_stats_api(self):
        group_message0 = await self.send_1v1_message(receiver_id=4444)
        group_message1 = await self.send_1v1_message(receiver_id=5555)

        group_id0 = group_message0["group_id"]
        group_id1 = group_message1["group_id"]

        stats = await self.get_global_user_stats(hidden=False)
        self.assertEqual(0, stats["group_amount"])
        self.assertEqual(2, stats["one_to_one_amount"])

        await self.update_hide_group_for(group_id0, hide=True)

        stats = await self.get_global_user_stats(hidden=False)
        self.assertEqual(1, stats["one_to_one_amount"])

        await self.update_hide_group_for(group_id1, hide=True)

        stats = await self.get_global_user_stats(hidden=False)
        self.assertEqual(0, stats["one_to_one_amount"])

        await self.update_hide_group_for(group_id0, hide=False)

        stats = await self.get_global_user_stats(hidden=False)
        self.assertEqual(1, stats["one_to_one_amount"])

    async def test_hidden_groups_is_counted_in_user_stats_api_if_specified_in_request(self):
        group_message0 = await self.send_1v1_message(receiver_id=4444)
        group_message1 = await self.send_1v1_message(receiver_id=5555)
        await self.send_1v1_message(receiver_id=6666)

        group_id0 = group_message0["group_id"]
        group_id1 = group_message1["group_id"]

        stats = await self.get_global_user_stats(hidden=False)
        self.assertEqual(0, stats["group_amount"])
        self.assertEqual(3, stats["one_to_one_amount"])

        await self.update_hide_group_for(group_id0, hide=True)
        stats = await self.get_global_user_stats(hidden=False)
        self.assertEqual(2, stats["one_to_one_amount"])
        stats = await self.get_global_user_stats(hidden=True)
        self.assertEqual(1, stats["one_to_one_amount"])

        await self.update_hide_group_for(group_id1, hide=True)
        stats = await self.get_global_user_stats(hidden=False)
        self.assertEqual(1, stats["one_to_one_amount"])
        stats = await self.get_global_user_stats(hidden=True)
        self.assertEqual(2, stats["one_to_one_amount"])

    async def test_hidden_groups_is_not_counted_if_not_specified(self):
        group_message0 = await self.send_1v1_message(receiver_id=4444)
        group_message1 = await self.send_1v1_message(receiver_id=5555)
        await self.send_1v1_message(receiver_id=6666)

        group_id0 = group_message0["group_id"]
        group_id1 = group_message1["group_id"]

        stats = await self.get_global_user_stats(hidden=None)
        self.assertEqual(0, stats["group_amount"])
        self.assertEqual(3, stats["one_to_one_amount"])

        await self.update_hide_group_for(group_id0, hide=True)
        stats = await self.get_global_user_stats(hidden=False)
        self.assertEqual(2, stats["one_to_one_amount"])
        stats = await self.get_global_user_stats(hidden=True)
        self.assertEqual(1, stats["one_to_one_amount"])

        # None means both hidden and visible together
        stats = await self.get_global_user_stats(hidden=None)
        self.assertEqual(3, stats["one_to_one_amount"])

        await self.update_hide_group_for(group_id1, hide=True)
        stats = await self.get_global_user_stats(hidden=False)
        self.assertEqual(1, stats["one_to_one_amount"])
        stats = await self.get_global_user_stats(hidden=True)
        self.assertEqual(2, stats["one_to_one_amount"])

        # None means both visible and hidden
        stats = await self.get_global_user_stats(hidden=None)
        self.assertEqual(3, stats["one_to_one_amount"])

    async def test_until_param_excluded_matching_group_in_list(self):
        await self.send_1v1_message(receiver_id=22)
        await self.assert_groups_for_user(1)

        # need a different timestamp on the other group
        time.sleep(0.01)

        await self.send_1v1_message(receiver_id=44)
        await self.assert_groups_for_user(2)

        time.sleep(0.01)

        all_groups = await self.groups_for_user()
        self.assertEqual(2, len(all_groups))

        groups = await self.groups_for_user(until=all_groups[1]["group"]["last_message_time"])
        self.assertEqual(0, len(groups))

        groups = await self.groups_for_user(until=all_groups[0]["group"]["last_message_time"])
        self.assertEqual(1, len(groups))

    async def test_get_group_information(self):
        group_message = await self.send_1v1_message()

        # defaults to -1 if not counting
        info = await self.get_group_info(group_message["group_id"], count_messages=False)
        self.assertEqual(-1, info["message_amount"])

        info = await self.get_group_info(group_message["group_id"], count_messages=True)
        self.assertEqual(1, info["message_amount"])

        # should be two now
        group_message = await self.send_1v1_message()
        info = await self.get_group_info(group_message["group_id"], count_messages=True)
        self.assertEqual(2, info["message_amount"])

    async def test_unread_groups_amount_in_user_stats(self):
        # default is to count
        stats = await self.get_global_user_stats()
        self.assertEqual(0, stats["unread_groups_amount"])

        stats = await self.get_global_user_stats(count_unread=True)
        self.assertEqual(0, stats["unread_groups_amount"])

        stats = await self.get_global_user_stats(count_unread=False)
        self.assertEqual(-1, stats["unread_groups_amount"])

        await self.send_1v1_message(user_id=50, receiver_id=BaseTest.USER_ID)
        stats = await self.get_global_user_stats(count_unread=True)
        self.assertEqual(1, stats["unread_groups_amount"])

        await self.send_1v1_message(user_id=51, receiver_id=BaseTest.USER_ID)
        stats = await self.get_global_user_stats(count_unread=True)
        self.assertEqual(2, stats["unread_groups_amount"])

        # not a new group so should not change number of unread groups
        await self.send_1v1_message(user_id=51, receiver_id=BaseTest.USER_ID)
        stats = await self.get_global_user_stats(count_unread=True)
        self.assertEqual(2, stats["unread_groups_amount"])

    async def test_join_existing_group(self):
        users = [BaseTest.USER_ID, 50]
        other_users = [51, 52, 53, 54]

        group = await self.create_and_join_group(
            BaseTest.USER_ID, users=users
        )

        await self.user_joins_group(group, other_users[0])

    @BaseServerRestApi.init_db_session
    async def test_get_groups_with_undeleted_messages(self):
        groups = list()
        users = [BaseTest.USER_ID, 50, 51, 52, 53, 54]

        # first create some groups and send some messages
        for _ in list(range(5)):
            groups.append(await self.create_and_join_group(
                BaseTest.USER_ID, users=users
            ))
            time.sleep(0.01)
            await self.send_message_to_group_from(groups[0], BaseTest.USER_ID)

        # check that initially there should be now groups to consider for deleting
        # messages
        to_del = await self.env.db.get_groups_with_undeleted_messages(self.env.db_session)
        self.assertEqual(0, len(to_del))

        delete_time = utcnow_ts()

        # of only one user has delete_before > first_message_time, the group should
        # not be considered for message deletion (since other users haven't changed
        # their delete_before)
        await self.update_delete_before(groups[0], delete_time, users[0])
        to_del = await self.env.db.get_groups_with_undeleted_messages(self.env.db_session)
        self.assertEqual(0, len(to_del))

        # if all users have a delete_before > first_message_time, then the deletion
        # query should find it
        for user in users:
            await self.update_delete_before(groups[0], delete_time, user)

        to_del = await self.env.db.get_groups_with_undeleted_messages(self.env.db_session)
        self.assertEqual(1, len(to_del))

        # returns a list of tuples: [(group_id, min(delete_before)),]
        self.assertEqual(to_del[0][0], groups[0])
        self.assertEqual(to_del[0][1], to_dt(delete_time))

    async def test_total_unread_count_after_deleting_all_group(self):
        await self.send_1v1_message(user_id=1000)
        await self.send_1v1_message(user_id=1001)
        await self.send_1v1_message(user_id=1002)
        await self.send_1v1_message(user_id=1003)

        stats = await self.get_global_user_stats(user_id=BaseTest.OTHER_USER_ID)
        self.assertEqual(4, stats["one_to_one_amount"])
        self.assertEqual(4, stats["unread_amount"])

        await self.delete_all_groups(user_id=BaseTest.OTHER_USER_ID)

        stats = await self.get_global_user_stats(user_id=BaseTest.OTHER_USER_ID)
        self.assertEqual(0, stats["one_to_one_amount"])
        self.assertEqual(0, stats["unread_amount"])

    async def test_total_unread_count_after_hiding_one_group(self):
        msg = await self.send_1v1_message(user_id=1000)
        await self.send_1v1_message(user_id=1001)
        await self.send_1v1_message(user_id=1002)
        await self.send_1v1_message(user_id=1003)

        stats = await self.get_global_user_stats(user_id=BaseTest.OTHER_USER_ID)
        self.assertEqual(4, stats["one_to_one_amount"])
        self.assertEqual(4, stats["unread_amount"])

        await self.update_hide_group_for(group_id=msg["group_id"], hide=True, user_id=BaseTest.OTHER_USER_ID)

        stats = await self.get_global_user_stats(user_id=BaseTest.OTHER_USER_ID, hidden=False)
        self.assertEqual(3, stats["one_to_one_amount"])
        self.assertEqual(3, stats["unread_amount"])

        stats = await self.get_global_user_stats(user_id=BaseTest.OTHER_USER_ID, hidden=None)
        self.assertEqual(4, stats["one_to_one_amount"])
        self.assertEqual(3, stats["unread_amount"])

    async def test_total_unread_count_after_deleting_one_group(self):
        msg = await self.send_1v1_message(user_id=1000)
        await self.send_1v1_message(user_id=1001)
        await self.send_1v1_message(user_id=1002)
        await self.send_1v1_message(user_id=1003)

        stats = await self.get_global_user_stats(user_id=BaseTest.OTHER_USER_ID)
        self.assertEqual(4, stats["one_to_one_amount"])
        self.assertEqual(4, stats["unread_amount"])

        await self.update_delete_before(
            group_id=msg["group_id"],
            delete_before=arrow.utcnow().timestamp()+1,
            user_id=BaseTest.OTHER_USER_ID
        )

        stats = await self.get_global_user_stats(user_id=BaseTest.OTHER_USER_ID)
        self.assertEqual(3, stats["one_to_one_amount"])
        self.assertEqual(3, stats["unread_amount"])

    async def test_total_unread_count_after_bookmarking_one_group(self):
        # send 3 msgs to this group, which we'll bookmark later
        msg = await self.send_1v1_message(user_id=1000)
        await self.send_1v1_message(user_id=1000)
        await self.send_1v1_message(user_id=1000)

        group_id = msg["group_id"]

        await self.send_1v1_message(user_id=1001)
        await self.send_1v1_message(user_id=1002)
        await self.send_1v1_message(user_id=1003)

        stats = await self.get_global_user_stats(user_id=BaseTest.OTHER_USER_ID)
        self.assertEqual(4, stats["one_to_one_amount"])
        self.assertEqual(6, stats["unread_amount"])

        # need to read a group before bookmarking it
        await self.update_last_read(group_id, user_id=BaseTest.OTHER_USER_ID)

        # make sure the unread count was reduced by 3
        stats = await self.get_global_user_stats(user_id=BaseTest.OTHER_USER_ID)
        self.assertEqual(4, stats["one_to_one_amount"])
        self.assertEqual(3, stats["unread_amount"])

        await self.bookmark_group(
            group_id=group_id,
            bookmark=True,
            user_id=BaseTest.OTHER_USER_ID
        )

        # bookmarking will increase unread count by 1 for the bookmarked group
        stats = await self.get_global_user_stats(user_id=BaseTest.OTHER_USER_ID)
        self.assertEqual(4, stats["one_to_one_amount"])
        self.assertEqual(4, stats["unread_amount"])

        await self.bookmark_group(
            group_id=group_id,
            bookmark=False,
            user_id=BaseTest.OTHER_USER_ID
        )

        # removing the bookmarking reduces unread by 1
        stats = await self.get_global_user_stats(user_id=BaseTest.OTHER_USER_ID)
        self.assertEqual(4, stats["one_to_one_amount"])
        self.assertEqual(3, stats["unread_amount"])
