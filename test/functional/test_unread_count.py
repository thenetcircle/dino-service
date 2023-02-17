from uuid import uuid4 as uuid

import arrow

from dinofw.rest.queries import CreateActionLogQuery, UserStatsQuery
from dinofw.rest.queries import MessageQuery
from dinofw.rest.queries import SendMessageQuery
from dinofw.rest.queries import UpdateUserGroupStats
from dinofw.utils.config import GroupTypes
from dinofw.utils.config import MessageTypes

from test.base import BaseTest
from test.base import async_test
from test.functional.base_db import BaseDatabaseTest
from test.functional.base_functional import BaseServerRestApi


class TestUnreadCount(BaseServerRestApi):
    @async_test
    async def test_unread_count_not_updated_if_notifications_disabled(self):
        session = self.env.session_maker()
        send_query = SendMessageQuery(
            receiver_id=BaseTest.OTHER_USER_ID,
            message_type=MessageTypes.MESSAGE,
            message_payload="some message"
        )

        message = await self.env.rest.message.send_message_to_user(BaseTest.USER_ID, send_query, session)
        group_id = message.group_id
        self.assert_unread_amount_and_groups(BaseDatabaseTest.OTHER_USER_ID, 1, 1, session)
        self.assert_cached_unread_for_group(BaseDatabaseTest.OTHER_USER_ID, group_id, 1)

        # disable notifications
        await self.env.rest.group.update_user_group_stats(
            group_id, BaseTest.OTHER_USER_ID, UpdateUserGroupStats(notifications=False), session
        )
        await self.env.rest.message.send_message_to_user(BaseTest.USER_ID, send_query, session)
        self.assert_unread_amount_and_groups(BaseDatabaseTest.OTHER_USER_ID, 1, 1, session)
        self.assert_cached_unread_for_group(BaseDatabaseTest.OTHER_USER_ID, group_id, 1)

        # enable notifications again
        await self.env.rest.group.update_user_group_stats(
            group_id, BaseTest.OTHER_USER_ID, UpdateUserGroupStats(notifications=True), session
        )
        await self.env.rest.message.send_message_to_user(BaseTest.USER_ID, send_query, session)
        self.assert_unread_amount_and_groups(BaseDatabaseTest.OTHER_USER_ID, 2, 1, session)
        self.assert_cached_unread_for_group(BaseDatabaseTest.OTHER_USER_ID, group_id, 2)

    @async_test
    async def test_unread_count_0_and_1(self):
        session = self.env.session_maker()

        await self.env.rest.message.send_message_to_user(
            BaseTest.USER_ID,
            SendMessageQuery(
                receiver_id=BaseTest.OTHER_USER_ID,
                message_type=MessageTypes.MESSAGE,
                message_payload="some message"
            ),
            session
        )

        unread_amount, n_unread_groups = self.env.rest.user.count_unread(
            BaseDatabaseTest.USER_ID, session
        )
        self.assertEqual(0, unread_amount)
        self.assertEqual(0, n_unread_groups)

        unread_amount, n_unread_groups = self.env.rest.user.count_unread(
            BaseDatabaseTest.OTHER_USER_ID, session
        )
        self.assertEqual(1, unread_amount)
        self.assertEqual(1, n_unread_groups)

    @async_test
    async def test_count_total_unread(self):
        session = self.env.session_maker()

        unread_count, unread_groups = self.env.db.count_total_unread(BaseTest.USER_ID, session)
        self.assertEqual(0, unread_count)
        self.assertEqual(0, len(unread_groups))

        await self.env.rest.message.send_message_to_user(
            BaseTest.OTHER_USER_ID,
            SendMessageQuery(
                receiver_id=BaseTest.USER_ID,
                message_type=MessageTypes.MESSAGE,
                message_payload="some message"
            ),
            session
        )

        unread_count, unread_groups = self.env.db.count_total_unread(BaseTest.USER_ID, session)
        self.assertEqual(1, unread_count)
        self.assertEqual(1, len(unread_groups))

    @async_test
    async def test_count_total_unread_included_bookmark(self):
        session = self.env.session_maker()

        unread_count, unread_groups = self.env.db.count_total_unread(BaseTest.USER_ID, session)
        self.assertEqual(0, unread_count)
        self.assertEqual(0, len(unread_groups))

        await self.env.rest.message.send_message_to_user(
            BaseTest.OTHER_USER_ID,
            SendMessageQuery(
                receiver_id=BaseTest.USER_ID,
                message_type=MessageTypes.MESSAGE,
                message_payload="some message"
            ),
            session
        )

        # send 3 messages, so we know it's the bookmarking that gives +1 unread
        group_id = None
        for _ in range(3):
            message = await self.env.rest.message.send_message_to_user(
                BaseTest.OTHER_USER_ID + 1,
                SendMessageQuery(
                    receiver_id=BaseTest.USER_ID,
                    message_type=MessageTypes.MESSAGE,
                    message_payload="some message"
                ),
                session
            )
            group_id = message.group_id

        # two groups, one each
        unread_count, unread_groups = self.env.db.count_total_unread(BaseTest.USER_ID, session)
        self.assertEqual(4, unread_count)
        self.assertEqual(2, len(unread_groups))

        # mark as read, so should have 0 unread for this group, 1 unread in total
        await self.env.rest.group.histories(
            group_id, BaseTest.USER_ID, MessageQuery(per_page=30, since=0), session
        )

        unread_count, unread_groups = self.env.db.count_total_unread(BaseTest.USER_ID, session)
        self.assertEqual(1, unread_count)
        self.assertEqual(1, len(unread_groups))

        # bookmark the one we just read
        await self.env.rest.group.update_user_group_stats(
            group_id, BaseTest.USER_ID, UpdateUserGroupStats(bookmark=True), session
        )

        # should have 1 unread message, and 1 bookmarked without unread (counting as 1 unread)
        unread_count, unread_groups = self.env.db.count_total_unread(BaseTest.USER_ID, session)
        self.assertEqual(2, unread_count)
        self.assertEqual(2, len(unread_groups))

    @async_test
    async def test_count_total_unread_cached_not_none(self):
        session = self.env.session_maker()

        cached_unread_count, cached_unread_groups = self.env.cache.get_total_unread_count(BaseTest.USER_ID)
        self.assertIsNone(cached_unread_count)
        self.assertIsNone(cached_unread_groups)

        await self.env.rest.message.send_message_to_user(
            BaseTest.OTHER_USER_ID,
            SendMessageQuery(
                receiver_id=BaseTest.USER_ID,
                message_type=MessageTypes.MESSAGE,
                message_payload="some message"
            ),
            session
        )

        unread_count, unread_groups = self.env.rest.user.count_unread(BaseTest.USER_ID, session)
        self.assertEqual(1, unread_count)
        self.assertEqual(1, unread_groups)

        cached_unread_count, cached_unread_groups = self.env.cache.get_total_unread_count(BaseTest.USER_ID)
        self.assertEqual(unread_count, cached_unread_count)
        self.assertEqual(unread_groups, cached_unread_groups)

    def test_count_total_unread_cached_is_none(self):
        session = self.env.session_maker()

        cached_unread_count, cached_unread_groups = self.env.cache.get_total_unread_count(BaseTest.USER_ID)
        self.assertIsNone(cached_unread_count)
        self.assertIsNone(cached_unread_groups)

        unread_count, unread_groups = self.env.rest.user.count_unread(BaseTest.USER_ID, session)
        self.assertEqual(0, unread_count)
        self.assertEqual(0, unread_groups)

        cached_unread_count, cached_unread_groups = self.env.cache.get_total_unread_count(BaseTest.USER_ID)
        self.assertIsNone(cached_unread_count)
        self.assertIsNone(cached_unread_groups)

    def test_count_total_unread_increase_cache(self):
        session = self.env.session_maker()

        cached_unread_count, cached_unread_groups = self.env.cache.get_total_unread_count(BaseTest.USER_ID)
        self.assertIsNone(cached_unread_count)
        self.assertIsNone(cached_unread_groups)

        unread_count, unread_groups = self.env.rest.user.count_unread(BaseTest.USER_ID, session)
        self.assertEqual(0, unread_count)
        self.assertEqual(0, unread_groups)

        self.env.cache.increase_total_unread_message_count([BaseTest.USER_ID], 1)

        unread_count, unread_groups = self.env.rest.user.count_unread(BaseTest.USER_ID, session)
        self.assertEqual(1, unread_count)
        self.assertEqual(0, unread_groups)

        self.env.cache.add_unread_group([BaseTest.USER_ID], str(uuid()))

        unread_count, unread_groups = self.env.rest.user.count_unread(BaseTest.USER_ID, session)
        self.assertEqual(1, unread_count)
        self.assertEqual(1, unread_groups)

    @async_test
    async def test_cached_unread_count_increases_on_new_message(self):
        session = self.env.session_maker()

        cached_unread_count, cached_unread_groups = self.env.cache.get_total_unread_count(BaseTest.USER_ID)
        self.assertIsNone(cached_unread_count)
        self.assertIsNone(cached_unread_groups)

        await self.env.rest.message.send_message_to_user(
            BaseTest.OTHER_USER_ID,
            SendMessageQuery(
                receiver_id=BaseTest.USER_ID,
                message_type=MessageTypes.MESSAGE,
                message_payload="some message"
            ),
            session
        )
        # we don't increase cached about if it's already None in redis
        cached_unread_count, cached_unread_groups = self.env.cache.get_total_unread_count(BaseTest.USER_ID)
        self.assertIsNone(cached_unread_count)
        self.assertIsNone(cached_unread_groups)

        # force a count to cache the real values
        await self.env.rest.user.get_user_stats(BaseTest.USER_ID, UserStatsQuery(count_unread=True), session)

        cached_unread_count, cached_unread_groups = self.env.cache.get_total_unread_count(BaseTest.USER_ID)
        self.assertEqual(1, cached_unread_count)
        self.assertEqual(1, cached_unread_groups)

    @async_test
    async def test_cached_unread_count_decreases_on_getting_history(self):
        session = self.env.session_maker()

        cached_unread_count, cached_unread_groups = self.env.cache.get_total_unread_count(BaseTest.USER_ID)
        self.assertIsNone(cached_unread_count)
        self.assertIsNone(cached_unread_groups)

        message = await self.env.rest.message.send_message_to_user(
            BaseTest.OTHER_USER_ID,
            SendMessageQuery(
                receiver_id=BaseTest.USER_ID,
                message_type=MessageTypes.MESSAGE,
                message_payload="some message"
            ),
            session
        )
        group_id = message.group_id

        # we don't increase cached about if it's already None in redis
        cached_unread_count, cached_unread_groups = self.env.cache.get_total_unread_count(BaseTest.USER_ID)
        self.assertIsNone(cached_unread_count)
        self.assertIsNone(cached_unread_groups)

        # force a count to cache the real values
        await self.env.rest.user.get_user_stats(BaseTest.USER_ID, UserStatsQuery(count_unread=True), session)

        cached_unread_count, cached_unread_groups = self.env.cache.get_total_unread_count(BaseTest.USER_ID)
        self.assertEqual(1, cached_unread_count)
        self.assertEqual(1, cached_unread_groups)

        # mark as read, so should have 0 unread for this group, 1 unread in total
        await self.env.rest.group.histories(
            group_id, BaseTest.USER_ID, MessageQuery(per_page=30, since=0), session
        )

        cached_unread_count, cached_unread_groups = self.env.cache.get_total_unread_count(BaseTest.USER_ID)
        self.assertEqual(0, cached_unread_count)
        self.assertEqual(0, cached_unread_groups)

    @async_test
    async def test_cached_unread_count_decreases_on_updating_last_read(self):
        session = self.env.session_maker()

        cached_unread_count, cached_unread_groups = self.env.cache.get_total_unread_count(BaseTest.USER_ID)
        self.assertIsNone(cached_unread_count)
        self.assertIsNone(cached_unread_groups)

        message = await self.env.rest.message.send_message_to_user(
            BaseTest.OTHER_USER_ID,
            SendMessageQuery(
                receiver_id=BaseTest.USER_ID,
                message_type=MessageTypes.MESSAGE,
                message_payload="some message"
            ),
            session
        )
        group_id = message.group_id

        # we don't increase cached about if it's already None in redis
        cached_unread_count, cached_unread_groups = self.env.cache.get_total_unread_count(BaseTest.USER_ID)
        self.assertIsNone(cached_unread_count)
        self.assertIsNone(cached_unread_groups)

        # force a count to cache the real values
        await self.env.rest.user.get_user_stats(BaseTest.USER_ID, UserStatsQuery(count_unread=True), session)

        cached_unread_count, cached_unread_groups = self.env.cache.get_total_unread_count(BaseTest.USER_ID)
        self.assertEqual(1, cached_unread_count)
        self.assertEqual(1, cached_unread_groups)

        # mark as read, so should have 0 unread for this group, 1 unread in total
        await self.env.rest.group.update_user_group_stats(
            group_id, BaseTest.USER_ID, UpdateUserGroupStats(last_read_time=arrow.utcnow().timestamp()), session
        )

        cached_unread_count, cached_unread_groups = self.env.cache.get_total_unread_count(BaseTest.USER_ID)
        self.assertEqual(0, cached_unread_count)
        self.assertEqual(0, cached_unread_groups)

    @async_test
    async def test_cached_unread_count_changes_on_bookmark(self):
        session = self.env.session_maker()

        cached_unread_count, cached_unread_groups = self.env.cache.get_total_unread_count(BaseTest.USER_ID)
        self.assertIsNone(cached_unread_count)
        self.assertIsNone(cached_unread_groups)

        message = await self.env.rest.message.send_message_to_user(
            BaseTest.OTHER_USER_ID,
            SendMessageQuery(
                receiver_id=BaseTest.USER_ID,
                message_type=MessageTypes.MESSAGE,
                message_payload="some message"
            ),
            session
        )
        group_id = message.group_id

        # we don't increase cached about if it's already None in redis
        cached_unread_count, cached_unread_groups = self.env.cache.get_total_unread_count(BaseTest.USER_ID)
        self.assertIsNone(cached_unread_count)
        self.assertIsNone(cached_unread_groups)

        # force a count to cache the real values
        await self.env.rest.user.get_user_stats(BaseTest.USER_ID, UserStatsQuery(count_unread=True), session)

        cached_unread_count, cached_unread_groups = self.env.cache.get_total_unread_count(BaseTest.USER_ID)
        self.assertEqual(1, cached_unread_count)
        self.assertEqual(1, cached_unread_groups)

        # mark as read, so should have 0 unread for this group, 1 unread in total
        await self.env.rest.group.histories(
            group_id, BaseTest.USER_ID, MessageQuery(per_page=30, since=0), session
        )

        cached_unread_count, cached_unread_groups = self.env.cache.get_total_unread_count(BaseTest.USER_ID)
        self.assertEqual(0, cached_unread_count)
        self.assertEqual(0, cached_unread_groups)

        # when bookmarking, unread should go up if there's 0 actually unread messages
        await self.env.rest.group.update_user_group_stats(
            group_id, BaseTest.USER_ID, UpdateUserGroupStats(bookmark=True), session
        )
        cached_unread_count, cached_unread_groups = self.env.cache.get_total_unread_count(BaseTest.USER_ID)
        self.assertEqual(1, cached_unread_count)
        self.assertEqual(1, cached_unread_groups)

        # removing bookmark it should go back to 0
        await self.env.rest.group.update_user_group_stats(
            group_id, BaseTest.USER_ID, UpdateUserGroupStats(bookmark=False), session
        )
        cached_unread_count, cached_unread_groups = self.env.cache.get_total_unread_count(BaseTest.USER_ID)
        self.assertEqual(0, cached_unread_count)
        self.assertEqual(0, cached_unread_groups)

    @async_test
    async def test_cached_unread_count_decrease_by_one_when_getting_history_and_bookmarked(self):
        session = self.env.session_maker()

        cached_unread_count, cached_unread_groups = self.env.cache.get_total_unread_count(BaseTest.USER_ID)
        self.assertIsNone(cached_unread_count)
        self.assertIsNone(cached_unread_groups)

        message = await self.env.rest.message.send_message_to_user(
            BaseTest.OTHER_USER_ID,
            SendMessageQuery(
                receiver_id=BaseTest.USER_ID,
                message_type=MessageTypes.MESSAGE,
                message_payload="some message"
            ),
            session
        )
        group_id = message.group_id

        # we don't increase cached about if it's already None in redis
        cached_unread_count, cached_unread_groups = self.env.cache.get_total_unread_count(BaseTest.USER_ID)
        self.assertIsNone(cached_unread_count)
        self.assertIsNone(cached_unread_groups)

        # force a count to cache the real values
        await self.env.rest.user.get_user_stats(BaseTest.USER_ID, UserStatsQuery(count_unread=True), session)

        cached_unread_count, cached_unread_groups = self.env.cache.get_total_unread_count(BaseTest.USER_ID)
        self.assertEqual(1, cached_unread_count)
        self.assertEqual(1, cached_unread_groups)

        # mark as read, so should have 0 unread for this group, 1 unread in total
        await self.env.rest.group.histories(
            group_id, BaseTest.USER_ID, MessageQuery(per_page=30, since=0), session
        )

        cached_unread_count, cached_unread_groups = self.env.cache.get_total_unread_count(BaseTest.USER_ID)
        self.assertEqual(0, cached_unread_count)
        self.assertEqual(0, cached_unread_groups)

        # when bookmarking, unread should go up if there's 0 actually unread messages
        await self.env.rest.group.update_user_group_stats(
            group_id, BaseTest.USER_ID, UpdateUserGroupStats(bookmark=True), session
        )
        cached_unread_count, cached_unread_groups = self.env.cache.get_total_unread_count(BaseTest.USER_ID)
        self.assertEqual(1, cached_unread_count)
        self.assertEqual(1, cached_unread_groups)

        # mark as read, so should have 0 unread for this group, 1 unread in total
        await self.env.rest.group.histories(
            group_id, BaseTest.USER_ID, MessageQuery(per_page=30, since=0), session
        )
        cached_unread_count, cached_unread_groups = self.env.cache.get_total_unread_count(BaseTest.USER_ID)
        self.assertEqual(0, cached_unread_count)
        self.assertEqual(0, cached_unread_groups)

    @async_test
    async def test_cached_unread_count_decrease_by_one_when_updating_last_read_and_bookmarked(self):
        session = self.env.session_maker()

        cached_unread_count, cached_unread_groups = self.env.cache.get_total_unread_count(BaseTest.USER_ID)
        self.assertIsNone(cached_unread_count)
        self.assertIsNone(cached_unread_groups)

        message = await self.env.rest.message.send_message_to_user(
            BaseTest.OTHER_USER_ID,
            SendMessageQuery(
                receiver_id=BaseTest.USER_ID,
                message_type=MessageTypes.MESSAGE,
                message_payload="some message"
            ),
            session
        )
        group_id = message.group_id

        # we don't increase cached about if it's already None in redis
        cached_unread_count, cached_unread_groups = self.env.cache.get_total_unread_count(BaseTest.USER_ID)
        self.assertIsNone(cached_unread_count)
        self.assertIsNone(cached_unread_groups)

        # force a count to cache the real values
        await self.env.rest.user.get_user_stats(BaseTest.USER_ID, UserStatsQuery(count_unread=True), session)

        cached_unread_count, cached_unread_groups = self.env.cache.get_total_unread_count(BaseTest.USER_ID)
        self.assertEqual(1, cached_unread_count)
        self.assertEqual(1, cached_unread_groups)

        # mark as read, so should have 0 unread for this group, 1 unread in total
        await self.env.rest.group.histories(
            group_id, BaseTest.USER_ID, MessageQuery(per_page=30, since=0), session
        )

        cached_unread_count, cached_unread_groups = self.env.cache.get_total_unread_count(BaseTest.USER_ID)
        self.assertEqual(0, cached_unread_count)
        self.assertEqual(0, cached_unread_groups)

        # when bookmarking, unread should go up if there's 0 actually unread messages
        await self.env.rest.group.update_user_group_stats(
            group_id, BaseTest.USER_ID, UpdateUserGroupStats(bookmark=True), session
        )
        cached_unread_count, cached_unread_groups = self.env.cache.get_total_unread_count(BaseTest.USER_ID)
        self.assertEqual(1, cached_unread_count)
        self.assertEqual(1, cached_unread_groups)

        # mark as read, so should have 0 unread for this group, 1 unread in total
        await self.env.rest.group.update_user_group_stats(
            group_id, BaseTest.USER_ID, UpdateUserGroupStats(last_read_time=arrow.utcnow().timestamp()), session
        )
        cached_unread_count, cached_unread_groups = self.env.cache.get_total_unread_count(BaseTest.USER_ID)
        self.assertEqual(0, cached_unread_count)
        self.assertEqual(0, cached_unread_groups)

    @async_test
    async def test_cached_unread_count_decrease_when_leaving_group(self):
        session = self.env.session_maker()

        cached_unread_count, cached_unread_groups = self.env.cache.get_total_unread_count(BaseTest.USER_ID)
        self.assertIsNone(cached_unread_count)
        self.assertIsNone(cached_unread_groups)

        message = await self.env.rest.message.send_message_to_user(
            BaseTest.OTHER_USER_ID,
            SendMessageQuery(
                receiver_id=BaseTest.USER_ID,
                message_type=MessageTypes.MESSAGE,
                message_payload="some message"
            ),
            session
        )
        group_id = message.group_id

        # we don't increase cached about if it's already None in redis
        cached_unread_count, cached_unread_groups = self.env.cache.get_total_unread_count(BaseTest.USER_ID)
        self.assertIsNone(cached_unread_count)
        self.assertIsNone(cached_unread_groups)

        # force a count to cache the real values
        await self.env.rest.user.get_user_stats(BaseTest.USER_ID, UserStatsQuery(count_unread=True), session)

        cached_unread_count, cached_unread_groups = self.env.cache.get_total_unread_count(BaseTest.USER_ID)
        self.assertEqual(1, cached_unread_count)
        self.assertEqual(1, cached_unread_groups)

        # leave the group, so should have 0 unread for this group, 1 unread in total
        group_id_to_type = {group_id: GroupTypes.ONE_TO_ONE}
        self.env.rest.group.leave_groups(group_id_to_type, BaseTest.USER_ID, CreateActionLogQuery(), session)

        cached_unread_count, cached_unread_groups = self.env.cache.get_total_unread_count(BaseTest.USER_ID)
        self.assertEqual(0, cached_unread_count)
        self.assertEqual(0, cached_unread_groups)

    @async_test
    async def test_cached_unread_count_changes_on_hiding_or_unhide(self):
        session = self.env.session_maker()

        cached_unread_count, cached_unread_groups = self.env.cache.get_total_unread_count(BaseTest.USER_ID)
        self.assertIsNone(cached_unread_count)
        self.assertIsNone(cached_unread_groups)

        message = await self.env.rest.message.send_message_to_user(
            BaseTest.OTHER_USER_ID,
            SendMessageQuery(
                receiver_id=BaseTest.USER_ID,
                message_type=MessageTypes.MESSAGE,
                message_payload="some message"
            ),
            session
        )
        group_id = message.group_id

        # we don't increase cached about if it's already None in redis
        cached_unread_count, cached_unread_groups = self.env.cache.get_total_unread_count(BaseTest.USER_ID)
        self.assertIsNone(cached_unread_count)
        self.assertIsNone(cached_unread_groups)

        # force a count to cache the real values
        await self.env.rest.user.get_user_stats(BaseTest.USER_ID, UserStatsQuery(count_unread=True), session)

        cached_unread_count, cached_unread_groups = self.env.cache.get_total_unread_count(BaseTest.USER_ID)
        self.assertEqual(1, cached_unread_count)
        self.assertEqual(1, cached_unread_groups)

        # hide the group, so should have 0 unread for this group, 1 unread in total
        await self.env.rest.group.update_user_group_stats(
            group_id, BaseTest.USER_ID, UpdateUserGroupStats(hide=True), session
        )

        cached_unread_count, cached_unread_groups = self.env.cache.get_total_unread_count(BaseTest.USER_ID)
        self.assertEqual(0, cached_unread_count)
        self.assertEqual(0, cached_unread_groups)

        # un-hide the group, so should have 0 unread for this group, 1 unread in total
        await self.env.rest.group.update_user_group_stats(
            group_id, BaseTest.USER_ID, UpdateUserGroupStats(hide=False), session
        )

        cached_unread_count, cached_unread_groups = self.env.cache.get_total_unread_count(BaseTest.USER_ID)
        self.assertEqual(1, cached_unread_count)
        self.assertEqual(1, cached_unread_groups)

    @async_test
    async def test_cached_unread_count_changes_on_hiding_or_unhide(self):
        session = self.env.session_maker()

        cached_unread_count, cached_unread_groups = self.env.cache.get_total_unread_count(BaseTest.USER_ID)
        self.assertIsNone(cached_unread_count)
        self.assertIsNone(cached_unread_groups)

        message = await self.env.rest.message.send_message_to_user(
            BaseTest.OTHER_USER_ID,
            SendMessageQuery(
                receiver_id=BaseTest.USER_ID,
                message_type=MessageTypes.MESSAGE,
                message_payload="some message"
            ),
            session
        )
        group_id = message.group_id

        # we don't increase cached about if it's already None in redis
        cached_unread_count, cached_unread_groups = self.env.cache.get_total_unread_count(BaseTest.USER_ID)
        self.assertIsNone(cached_unread_count)
        self.assertIsNone(cached_unread_groups)

        # force a count to cache the real values
        await self.env.rest.user.get_user_stats(BaseTest.USER_ID, UserStatsQuery(count_unread=True), session)

        cached_unread_count, cached_unread_groups = self.env.cache.get_total_unread_count(BaseTest.USER_ID)
        self.assertEqual(1, cached_unread_count)
        self.assertEqual(1, cached_unread_groups)

        # hide the group, so should have 0 unread for this group, 1 unread in total
        await self.env.rest.group.update_user_group_stats(
            group_id, BaseTest.USER_ID, UpdateUserGroupStats(hide=True), session
        )

        cached_unread_count, cached_unread_groups = self.env.cache.get_total_unread_count(BaseTest.USER_ID)
        self.assertEqual(0, cached_unread_count)
        self.assertEqual(0, cached_unread_groups)

        # un-hide the group, so should have 0 unread for this group, 1 unread in total
        await self.env.rest.group.update_user_group_stats(
            group_id, BaseTest.USER_ID, UpdateUserGroupStats(hide=False), session
        )

        cached_unread_count, cached_unread_groups = self.env.cache.get_total_unread_count(BaseTest.USER_ID)
        self.assertEqual(1, cached_unread_count)
        self.assertEqual(1, cached_unread_groups)

    @async_test
    async def test_cached_unread_count_changes_new_message_for_hidden_group(self):
        session = self.env.session_maker()

        cached_unread_count, cached_unread_groups = self.env.cache.get_total_unread_count(BaseTest.USER_ID)
        self.assertIsNone(cached_unread_count)
        self.assertIsNone(cached_unread_groups)

        message = await self.env.rest.message.send_message_to_user(
            BaseTest.OTHER_USER_ID,
            SendMessageQuery(
                receiver_id=BaseTest.USER_ID,
                message_type=MessageTypes.MESSAGE,
                message_payload="some message"
            ),
            session
        )
        group_id = message.group_id

        # we don't increase cached about if it's already None in redis
        cached_unread_count, cached_unread_groups = self.env.cache.get_total_unread_count(BaseTest.USER_ID)
        self.assertIsNone(cached_unread_count)
        self.assertIsNone(cached_unread_groups)

        # force a count to cache the real values
        await self.env.rest.user.get_user_stats(BaseTest.USER_ID, UserStatsQuery(count_unread=True), session)

        cached_unread_count, cached_unread_groups = self.env.cache.get_total_unread_count(BaseTest.USER_ID)
        self.assertEqual(1, cached_unread_count)
        self.assertEqual(1, cached_unread_groups)

        # hide the group, so should have 0 unread for this group, 1 unread in total
        await self.env.rest.group.update_user_group_stats(
            group_id, BaseTest.USER_ID, UpdateUserGroupStats(hide=True), session
        )

        cached_unread_count, cached_unread_groups = self.env.cache.get_total_unread_count(BaseTest.USER_ID)
        self.assertEqual(0, cached_unread_count)
        self.assertEqual(0, cached_unread_groups)

        await self.env.rest.message.send_message_to_user(
            BaseTest.OTHER_USER_ID,
            SendMessageQuery(
                receiver_id=BaseTest.USER_ID,
                message_type=MessageTypes.MESSAGE,
                message_payload="some message"
            ),
            session
        )

        # should have 2 unread now, but still only 1 group
        cached_unread_count, cached_unread_groups = self.env.cache.get_total_unread_count(BaseTest.USER_ID)
        self.assertEqual(2, cached_unread_count)
        self.assertEqual(1, cached_unread_groups)

    @async_test
    async def test_cached_unread_count_does_not_change_when_group_is_undeleted(self):
        session = self.env.session_maker()

        cached_unread_count, cached_unread_groups = self.env.cache.get_total_unread_count(BaseTest.USER_ID)
        self.assertIsNone(cached_unread_count)
        self.assertIsNone(cached_unread_groups)

        message = await self.env.rest.message.send_message_to_user(
            BaseTest.OTHER_USER_ID,
            SendMessageQuery(
                receiver_id=BaseTest.USER_ID,
                message_type=MessageTypes.MESSAGE,
                message_payload="some message"
            ),
            session
        )
        group_id = message.group_id

        # we don't increase cached about if it's already None in redis
        cached_unread_count, cached_unread_groups = self.env.cache.get_total_unread_count(BaseTest.USER_ID)
        self.assertIsNone(cached_unread_count)
        self.assertIsNone(cached_unread_groups)

        # force a count to cache the real values
        await self.env.rest.user.get_user_stats(BaseTest.USER_ID, UserStatsQuery(count_unread=True), session)

        cached_unread_count, cached_unread_groups = self.env.cache.get_total_unread_count(BaseTest.USER_ID)
        self.assertEqual(1, cached_unread_count)
        self.assertEqual(1, cached_unread_groups)

        # delete the group, so should have 0 unread (1 still when it comes back)
        await self.env.rest.group.update_user_group_stats(
            group_id, BaseTest.USER_ID, UpdateUserGroupStats(delete_before=arrow.utcnow().timestamp()), session
        )

        cached_unread_count, cached_unread_groups = self.env.cache.get_total_unread_count(BaseTest.USER_ID)
        self.assertEqual(0, cached_unread_count)
        self.assertEqual(0, cached_unread_groups)

        await self.env.rest.message.send_message_to_user(
            BaseTest.OTHER_USER_ID,
            SendMessageQuery(
                receiver_id=BaseTest.USER_ID,
                message_type=MessageTypes.MESSAGE,
                message_payload="some message"
            ),
            session
        )

        # should have 1 unread now still, can't get unread back after deleting
        cached_unread_count, cached_unread_groups = self.env.cache.get_total_unread_count(BaseTest.USER_ID)
        self.assertEqual(1, cached_unread_count)
        self.assertEqual(1, cached_unread_groups)
