import time

from dinofw.rest.groups import GroupResource
from dinofw.rest.message import MessageResource
from dinofw.rest.models import Group
from dinofw.rest.models import GroupUsers
from dinofw.rest.queries import CreateActionLogQuery
from dinofw.rest.queries import CreateGroupQuery
from dinofw.rest.queries import GroupInfoQuery
from dinofw.rest.queries import JoinGroupQuery
from dinofw.rest.queries import MessageQuery
from dinofw.rest.queries import SendMessageQuery
from dinofw.utils.config import MessageTypes, GroupTypes
from dinofw.utils.exceptions import NoSuchGroupException
from test.base import async_test, BaseTest


class TestGroupResource(BaseTest):
    def setUp(self) -> None:
        super().setUp()
        self.group = GroupResource(self.fake_env)
        self.message = MessageResource(self.fake_env)

    @async_test
    async def test_create_new_group(self):
        group_name = "new group name"
        query = CreateGroupQuery(
            group_name=group_name, group_type=0, users=[BaseTest.USER_ID],
        )

        self.assertEqual(0, len(self.group.env.storage.action_log))

        group = await self.group.create_new_group(
            user_id=BaseTest.USER_ID, query=query, db=None  # noqa
        )

        self.assertIsNotNone(group)
        self.assertEqual(type(group), Group)
        self.assertEqual(group_name, group.name)
        self.assertEqual(0, len(self.group.env.storage.action_log))

    @async_test
    async def test_get_users_in_group(self):
        query = CreateGroupQuery(
            group_name="a group name", group_type=0, users=[BaseTest.USER_ID],
        )

        group = await self.group.create_new_group(
            user_id=BaseTest.USER_ID, query=query, db=None
        )
        group_users = await self.group.get_users_in_group(
            group_id=group.group_id, db=None
        )

        self.assertIsNotNone(group_users)
        self.assertEqual(type(group_users), GroupUsers)
        self.assertEqual(1, group_users.user_count)
        self.assertEqual(BaseTest.USER_ID, group_users.users[0].user_id)
        self.assertEqual(BaseTest.USER_ID, group_users.owner_id)

    @async_test
    async def test_get_group(self):
        group_info_query = GroupInfoQuery(count_messages=False)

        with self.assertRaises(NoSuchGroupException):
            await self.group.get_group(
                group_id=BaseTest.GROUP_ID,
                query=group_info_query,
                db=None  # noqa
            )

        query = CreateGroupQuery(
            group_name="some group name", group_type=0, users=[BaseTest.USER_ID],
        )
        group = await self.group.create_new_group(
            user_id=BaseTest.USER_ID, query=query, db=None  # noqa
        )

        self.assertIsNotNone(group)
        self.assertEqual(type(group), Group)

        group = await self.group.get_group(
            group_id=group.group_id,
            query=group_info_query,
            db=None  # noqa
        )

        self.assertIsNotNone(group)
        self.assertEqual(type(group), Group)

    @async_test
    async def test_histories(self):
        send_query = SendMessageQuery(message_payload="some text", message_type=MessageTypes.MESSAGE)
        message_query = MessageQuery(per_page=10, since=0)
        create_query = CreateGroupQuery(group_name="some group name", group_type=0, users=[BaseTest.USER_ID])

        with self.assertRaises(NoSuchGroupException):
            await self.group.histories(BaseTest.GROUP_ID, BaseTest.USER_ID, message_query, db=None)  # noqa

        # create a new group
        group = await self.group.create_new_group(BaseTest.USER_ID, create_query, None)  # noqa
        time.sleep(0.01)

        # send message and get histories
        await self.message.send_message_to_group(group.group_id, BaseTest.USER_ID, send_query, None)  # noqa
        time.sleep(0.01)
        histories = await self.group.histories(group.group_id, BaseTest.USER_ID, message_query, db=None)  # noqa

        # one join event and one message
        self.assertEqual(1, len(histories.messages))

        # send another message
        await self.message.send_message_to_group(group.group_id, BaseTest.USER_ID, send_query, None)  # noqa
        time.sleep(0.01)
        histories = await self.group.histories(group.group_id, BaseTest.USER_ID, message_query, db=None)  # noqa

        # now we should have two messages but still only one join event
        self.assertEqual(2, len(histories.messages))

    @async_test
    async def test_get_user_group_stats(self):
        create_query = CreateGroupQuery(
            group_name="some group name", group_type=0, users=[BaseTest.USER_ID],
        )
        send_query = SendMessageQuery(
            message_payload="some text", message_type=MessageTypes.MESSAGE
        )

        # create a new group
        group = await self.group.create_new_group(
            BaseTest.USER_ID, create_query, None
        )
        stats = await self.group.get_user_group_stats(
            group.group_id, BaseTest.USER_ID, None
        )
        self.assertEqual(0, stats.unread)

        # send a message, should have 0 unread since we sent it
        await self.message.send_message_to_group(
            group.group_id, BaseTest.USER_ID, send_query, None
        )
        time.sleep(0.01)
        stats = await self.group.get_user_group_stats(
            group.group_id, BaseTest.USER_ID, None
        )
        self.assertEqual(0, stats.unread)

        # another user sends a message, should have 1 unread now
        await self.message.send_message_to_group(
            group.group_id, BaseTest.OTHER_USER_ID, send_query, None
        )
        time.sleep(0.01)
        stats = await self.group.get_user_group_stats(
            group.group_id, BaseTest.USER_ID, None
        )
        self.assertEqual(1, stats.unread)

    @async_test
    async def test_join_group(self):
        create_query = CreateGroupQuery(
            group_name="some group name", group_type=0, users=[BaseTest.USER_ID],
        )

        # create a new group
        group = await self.group.create_new_group(
            BaseTest.USER_ID, create_query, None
        )

        # check we only have one user in the group, the creator
        group_users = await self.group.get_users_in_group(group.group_id, None)  # noqa
        self.assertIsNotNone(group_users)
        self.assertEqual(1, group_users.user_count)
        self.assertEqual(1, len(group_users.users))
        self.assertTrue(any((g.user_id == BaseTest.USER_ID for g in group_users.users)))

        join_query = JoinGroupQuery(users=[BaseTest.OTHER_USER_ID])

        # other user joins it
        await self.group.join_group(
            group.group_id, join_query, None
        )

        # check the other user is now in the group as well
        group_users = await self.group.get_users_in_group(group.group_id, None)  # noqa
        self.assertIsNotNone(group_users)
        self.assertEqual(2, group_users.user_count)
        self.assertEqual(2, len(group_users.users))
        self.assertTrue(any((g.user_id == BaseTest.USER_ID for g in group_users.users)))
        self.assertTrue(
            any((g.user_id == BaseTest.OTHER_USER_ID for g in group_users.users))
        )

    @async_test
    async def test_leave_group(self):
        create_query = CreateGroupQuery(
            group_name="some group name", group_type=0, users=[BaseTest.USER_ID],
        )

        # group doesn't exist yet
        self.group.leave_groups({BaseTest.GROUP_ID: GroupTypes.PRIVATE_GROUP}, BaseTest.USER_ID, CreateActionLogQuery(), None)  # noqa

        # create a new group
        group = await self.group.create_new_group(
            BaseTest.USER_ID, create_query, None
        )

        # check we only have one user in the group, the creator
        group_users = await self.group.get_users_in_group(group.group_id, None)  # noqa
        self.assertIsNotNone(group_users)
        self.assertEqual(1, group_users.user_count)

        # leave the group
        self.group.leave_groups({group.group_id: GroupTypes.PRIVATE_GROUP}, BaseTest.USER_ID, CreateActionLogQuery(), None)  # noqa

        # check there's no users left in the group after leaving
        group_users = await self.group.get_users_in_group(group.group_id, None)  # noqa
        self.assertIsNotNone(group_users)
        self.assertEqual(0, group_users.user_count)
