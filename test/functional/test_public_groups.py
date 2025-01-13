import time

from dinofw.utils.config import GroupTypes, GroupStatus
from test.base import BaseTest
from test.functional.base_functional import BaseServerRestApi


class TestPublicGroups(BaseServerRestApi):
    async def test_leave_group_creates_deleted_copy(self):
        await self.assert_deleted_groups_for_user(0)
        await self.assert_groups_for_user(0)

        group_id = await self.create_and_join_group(
            user_id=BaseTest.USER_ID,
            users=[
                BaseTest.OTHER_USER_ID,
                BaseTest.THIRD_USER_ID
            ],
            group_type=GroupTypes.PUBLIC_ROOM
        )

        await self.assert_deleted_groups_for_user(0)
        await self.assert_groups_for_user(0)
        await self.assert_public_groups_for_user(1)
        self.assertEqual(0, len(self.env.storage.action_log))

        await self.user_leaves_group(group_id)

        # we don't create deletion logs for public groups
        await self.assert_deleted_groups_for_user(0)
        await self.assert_groups_for_user(0)
        await self.assert_public_groups_for_user(0)

        # should have an action log for leaving a public group
        self.assertEqual(1, len(self.env.storage.action_log))

    async def test_count_groups_does_not_includes_public_groups(self):
        await self.assert_groups_for_user(0)

        group_id_private = await self.create_and_join_group(
            user_id=BaseTest.USER_ID,
            users=[
                BaseTest.OTHER_USER_ID,
                BaseTest.THIRD_USER_ID
            ],
            group_type=GroupTypes.PRIVATE_GROUP
        )
        group_id_public = await self.create_and_join_group(
            user_id=BaseTest.USER_ID,
            users=[
                BaseTest.OTHER_USER_ID,
                BaseTest.THIRD_USER_ID
            ],
            group_type=GroupTypes.PUBLIC_ROOM
        )

        await self.assert_groups_for_user(1)

        await self.user_leaves_group(group_id_private)
        await self.assert_groups_for_user(0)

        await self.user_leaves_group(group_id_public)
        await self.assert_groups_for_user(0)

    @BaseServerRestApi.init_db_session
    async def test_unread_count_includes_public_groups(self):
        session = self.env.db_session

        await self.assert_groups_for_user(0)
        await self.assert_unread_amount_and_groups(BaseTest.USER_ID, 0, 0, session)

        group_id_private = await self.create_and_join_group(
            user_id=BaseTest.USER_ID,
            users=[
                BaseTest.OTHER_USER_ID,
                BaseTest.THIRD_USER_ID
            ],
            group_type=GroupTypes.PRIVATE_GROUP
        )
        group_id_public = await self.create_and_join_group(
            user_id=BaseTest.USER_ID,
            users=[
                BaseTest.OTHER_USER_ID,
                BaseTest.THIRD_USER_ID
            ],
            group_type=GroupTypes.PUBLIC_ROOM
        )

        await self.assert_groups_for_user(1)
        await self.assert_public_groups_for_user(1)
        await self.assert_unread_amount_and_groups(BaseTest.USER_ID, 0, 0, session)

        await self.send_message_to_group_from(group_id_private, BaseTest.OTHER_USER_ID)
        await self.send_message_to_group_from(group_id_private, BaseTest.OTHER_USER_ID)
        await self.assert_unread_amount_and_groups(BaseTest.USER_ID, 2, 1, session)

        await self.send_message_to_group_from(group_id_public, BaseTest.OTHER_USER_ID)
        await self.send_message_to_group_from(group_id_public, BaseTest.OTHER_USER_ID)
        await self.assert_unread_amount_and_groups(BaseTest.USER_ID, 4, 2, session)

        await self.user_leaves_group(group_id_private)
        await self.assert_unread_amount_and_groups(BaseTest.USER_ID, 2, 1, session)

        await self.user_leaves_group(group_id_public)
        await self.assert_unread_amount_and_groups(BaseTest.USER_ID, 0, 0, session)

    async def test_get_public_groups(self):
        await self.create_and_join_group(
            user_id=BaseTest.USER_ID,
            users=[
                BaseTest.OTHER_USER_ID,
                BaseTest.THIRD_USER_ID
            ],
            group_type=GroupTypes.PRIVATE_GROUP
        )
        group_id_public = await self.create_and_join_group(
            user_id=BaseTest.USER_ID,
            users=[
                BaseTest.OTHER_USER_ID,
                BaseTest.THIRD_USER_ID
            ],
            group_type=GroupTypes.PUBLIC_ROOM
        )

        groups = await self.get_public_groups()
        self.assertEqual(1, len(groups))

        for group in groups:
            self.assertEqual(GroupTypes.PUBLIC_ROOM, group["group_type"])
            self.assertEqual(group_id_public, group["group_id"])

    async def test_get_public_groups_for_friends(self):
        await self.create_and_join_group(
            user_id=BaseTest.USER_ID,
            users=[
                BaseTest.OTHER_USER_ID,
                BaseTest.THIRD_USER_ID
            ],
            group_type=GroupTypes.PUBLIC_ROOM
        )

        groups = await self.get_public_groups(users=[BaseTest.THIRD_USER_ID])
        self.assertEqual(1, len(groups))

    async def test_get_public_groups_for_friends_with_lang(self):
        await self.create_and_join_group(
            user_id=BaseTest.USER_ID,
            users=[
                BaseTest.OTHER_USER_ID,
                BaseTest.THIRD_USER_ID
            ],
            group_type=GroupTypes.PUBLIC_ROOM,
            language="de"
        )

        groups = await self.get_public_groups(
            users=[BaseTest.THIRD_USER_ID],
            spoken_languages=["de"]
        )
        self.assertEqual(1, len(groups))

    async def test_can_archive_groups(self):
        group_id = await self.create_and_join_group(
            user_id=BaseTest.USER_ID,
            users=[
                BaseTest.OTHER_USER_ID,
                BaseTest.THIRD_USER_ID
            ],
            group_type=GroupTypes.PUBLIC_ROOM
        )

        group = await self.get_group_info(group_id, count_messages=False)
        self.assertNotEqual(GroupStatus.ARCHIVED, group["status"])
        self.assertIsNone(group["status_changed_at"])

        await self.update_group_archived(group_id, archived=True)

        group = await self.get_group_info(group_id, count_messages=False)
        self.assertEqual(GroupStatus.ARCHIVED, group["status"])
        self.assertIsNotNone(group["status_changed_at"])

    async def test_can_un_archive_groups(self):
        group_id = await self.create_and_join_group(
            user_id=BaseTest.USER_ID,
            users=[
                BaseTest.OTHER_USER_ID,
                BaseTest.THIRD_USER_ID
            ],
            group_type=GroupTypes.PUBLIC_ROOM
        )

        await self.update_group_archived(group_id, archived=True)

        group = await self.get_group_info(group_id, count_messages=False)
        self.assertEqual(GroupStatus.ARCHIVED, group["status"])
        self.assertIsNotNone(group["status_changed_at"])
        first_changed_at = group["status_changed_at"]

        # so we can test the time difference
        time.sleep(0.01)
        await self.update_group_archived(group_id, archived=False)

        group = await self.get_group_info(group_id, count_messages=False)
        self.assertNotEqual(GroupStatus.ARCHIVED, group["status"])
        self.assertIsNotNone(group["status_changed_at"])
        self.assertNotEqual(first_changed_at, group["status_changed_at"])

    async def test_can_not_send_to_archived_groups(self):
        group_id = await self.create_and_join_group(
            user_id=BaseTest.USER_ID,
            users=[
                BaseTest.OTHER_USER_ID,
                BaseTest.THIRD_USER_ID
            ],
            group_type=GroupTypes.PUBLIC_ROOM
        )

        await self.update_group_archived(group_id, archived=True)

        group = await self.get_group_info(group_id, count_messages=False)
        self.assertEqual(GroupStatus.ARCHIVED, group["status"])
        self.assertIsNotNone(group["status_changed_at"])

        await self.send_message_to_group_from(group_id, BaseTest.OTHER_USER_ID, expected_error_code=607)

    async def test_only_admins_can_list_archived_groups(self):
        group_id = await self.create_and_join_group(
            user_id=BaseTest.USER_ID,
            users=[
                BaseTest.OTHER_USER_ID,
                BaseTest.THIRD_USER_ID
            ],
            group_type=GroupTypes.PUBLIC_ROOM
        )

        groups = await self.get_public_groups()
        self.assertEqual(1, len(groups))

        await self.update_group_archived(group_id, archived=True)

        groups = await self.get_public_groups()
        self.assertEqual(0, len(groups))

        groups = await self.get_public_groups(include_archived=True, admin_id=1971)
        self.assertEqual(1, len(groups))

    async def test_no_archived_groups_listed_without_admin_id(self):
        group_id = await self.create_and_join_group(
            user_id=BaseTest.USER_ID,
            users=[
                BaseTest.OTHER_USER_ID,
                BaseTest.THIRD_USER_ID
            ],
            group_type=GroupTypes.PUBLIC_ROOM
        )

        groups = await self.get_public_groups()
        self.assertEqual(1, len(groups))

        await self.update_group_archived(group_id, archived=True)

        groups = await self.get_public_groups(include_archived=True, admin_id=None)
        self.assertEqual(0, len(groups))

        groups = await self.get_public_groups(include_archived=False, admin_id=1971)
        self.assertEqual(0, len(groups))

    async def test_public_groups_can_have_language(self):
        await self.create_and_join_group(
            user_id=BaseTest.USER_ID,
            users=[
                BaseTest.OTHER_USER_ID,
                BaseTest.THIRD_USER_ID
            ],
            group_type=GroupTypes.PUBLIC_ROOM,
            language='de'
        )

        groups = await self.get_public_groups()
        self.assertEqual(1, len(groups))
        self.assertEqual('de', groups[0]['language'])

        groups = await self.groups_for_user()
        self.assertEqual(0, len(groups))

    async def test_private_groups_can_not_have_language(self):
        await self.create_and_join_group(
            user_id=BaseTest.USER_ID,
            users=[
                BaseTest.OTHER_USER_ID,
                BaseTest.THIRD_USER_ID
            ],
            group_type=GroupTypes.PRIVATE_GROUP,
            language='de'
        )

        groups = await self.groups_for_user(BaseTest.USER_ID)
        self.assertEqual(1, len(groups))
        self.assertIsNone(groups[0]['group']['language'])

    async def test_get_public_rooms_my_friends_are_in(self):
        await self.create_and_join_group(
            user_id=BaseTest.USER_ID,
            users=[
                BaseTest.OTHER_USER_ID
            ],
            group_type=GroupTypes.PUBLIC_ROOM,
            language='de'
        )
        await self.create_and_join_group(
            user_id=BaseTest.USER_ID,
            users=[
                BaseTest.USER_ID
            ],
            group_type=GroupTypes.PUBLIC_ROOM,
            language='de'
        )

        groups = await self.get_public_groups()
        self.assertEqual(2, len(groups))

        groups = await self.get_public_groups(users=[BaseTest.OTHER_USER_ID])
        self.assertEqual(1, len(groups))

        groups = await self.get_public_groups(users=[BaseTest.THIRD_USER_ID])
        self.assertEqual(0, len(groups))

        groups = await self.get_public_groups(users=[BaseTest.USER_ID])
        self.assertEqual(2, len(groups))

        groups = await self.get_public_groups(users=[BaseTest.OTHER_USER_ID, BaseTest.THIRD_USER_ID])
        self.assertEqual(1, len(groups))

        groups = await self.get_public_groups(users=[BaseTest.OTHER_USER_ID, BaseTest.USER_ID])
        self.assertEqual(2, len(groups))

    async def test_get_private_groups_for_spoken_languages(self):
        await self.create_and_join_group(
            user_id=BaseTest.USER_ID,
            users=[
                BaseTest.OTHER_USER_ID,
                BaseTest.THIRD_USER_ID
            ],
            group_type=GroupTypes.PUBLIC_ROOM,
            language='de'
        )
        await self.create_and_join_group(
            user_id=BaseTest.USER_ID,
            users=[
                BaseTest.OTHER_USER_ID,
                BaseTest.THIRD_USER_ID
            ],
            group_type=GroupTypes.PUBLIC_ROOM,
            language='jp'
        )

        groups = await self.get_public_groups(spoken_languages=None)
        self.assertEqual(2, len(groups))

        groups = await self.get_public_groups(spoken_languages=['sv'])
        self.assertEqual(0, len(groups))

        groups = await self.get_public_groups(spoken_languages=['de'])
        self.assertEqual(1, len(groups))

        groups = await self.get_public_groups(spoken_languages=['de', 'jp'])
        self.assertEqual(2, len(groups))

        groups = await self.get_public_groups(spoken_languages=['sv', 'jp'])
        self.assertEqual(1, len(groups))
