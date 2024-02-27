from dinofw.utils.config import GroupTypes
from test.base import BaseTest
from test.functional.base_functional import BaseServerRestApi


class TestPublicGroups(BaseServerRestApi):
    def test_leave_group_creates_deleted_copy(self):
        self.assert_deleted_groups_for_user(0)
        self.assert_groups_for_user(0)

        group_id = self.create_and_join_group(
            user_id=BaseTest.USER_ID,
            users=[
                BaseTest.OTHER_USER_ID,
                BaseTest.THIRD_USER_ID
            ],
            group_type=GroupTypes.PUBLIC_GROUP
        )

        self.assert_deleted_groups_for_user(0)
        self.assert_groups_for_user(1)
        self.assertEqual(0, len(self.env.storage.action_log))

        self.user_leaves_group(group_id)

        # we don't create deletion logs for public groups
        self.assert_deleted_groups_for_user(0)
        self.assert_groups_for_user(0)

        # should have an action log for leaving a public group
        self.assertEqual(1, len(self.env.storage.action_log))

    def test_count_groups_includes_public_groups(self):
        self.assert_groups_for_user(0)

        group_id_private = self.create_and_join_group(
            user_id=BaseTest.USER_ID,
            users=[
                BaseTest.OTHER_USER_ID,
                BaseTest.THIRD_USER_ID
            ],
            group_type=GroupTypes.PRIVATE_GROUP
        )
        group_id_public = self.create_and_join_group(
            user_id=BaseTest.USER_ID,
            users=[
                BaseTest.OTHER_USER_ID,
                BaseTest.THIRD_USER_ID
            ],
            group_type=GroupTypes.PUBLIC_GROUP
        )

        self.assert_groups_for_user(2)

        self.user_leaves_group(group_id_private)
        self.assert_groups_for_user(1)

        self.user_leaves_group(group_id_public)
        self.assert_groups_for_user(0)

    def test_unread_count_includes_public_groups(self):
        session = self.env.session_maker()

        self.assert_groups_for_user(0)
        self.assert_unread_amount_and_groups(BaseTest.USER_ID, 0, 0, session)

        group_id_private = self.create_and_join_group(
            user_id=BaseTest.USER_ID,
            users=[
                BaseTest.OTHER_USER_ID,
                BaseTest.THIRD_USER_ID
            ],
            group_type=GroupTypes.PRIVATE_GROUP
        )
        group_id_public = self.create_and_join_group(
            user_id=BaseTest.USER_ID,
            users=[
                BaseTest.OTHER_USER_ID,
                BaseTest.THIRD_USER_ID
            ],
            group_type=GroupTypes.PUBLIC_GROUP
        )

        self.assert_groups_for_user(2)
        self.assert_unread_amount_and_groups(BaseTest.USER_ID, 0, 0, session)

        self.send_message_to_group_from(group_id_private, BaseTest.OTHER_USER_ID)
        self.send_message_to_group_from(group_id_private, BaseTest.OTHER_USER_ID)
        self.assert_unread_amount_and_groups(BaseTest.USER_ID, 2, 1, session)

        self.send_message_to_group_from(group_id_public, BaseTest.OTHER_USER_ID)
        self.send_message_to_group_from(group_id_public, BaseTest.OTHER_USER_ID)
        self.assert_unread_amount_and_groups(BaseTest.USER_ID, 4, 2, session)

        self.user_leaves_group(group_id_private)
        self.assert_unread_amount_and_groups(BaseTest.USER_ID, 2, 1, session)

        self.user_leaves_group(group_id_public)
        self.assert_unread_amount_and_groups(BaseTest.USER_ID, 0, 0, session)

    def test_get_public_groups(self):
        self.create_and_join_group(
            user_id=BaseTest.USER_ID,
            users=[
                BaseTest.OTHER_USER_ID,
                BaseTest.THIRD_USER_ID
            ],
            group_type=GroupTypes.PRIVATE_GROUP
        )
        group_id_public = self.create_and_join_group(
            user_id=BaseTest.USER_ID,
            users=[
                BaseTest.OTHER_USER_ID,
                BaseTest.THIRD_USER_ID
            ],
            group_type=GroupTypes.PUBLIC_GROUP
        )

        groups = self.get_public_groups()
        self.assertEqual(1, len(groups))

        for group in groups:
            self.assertEqual(GroupTypes.PUBLIC_GROUP, group["group_type"])
            self.assertEqual(group_id_public, group["group_id"])

    def test_can_archive_groups(self):
        group_id = self.create_and_join_group(
            user_id=BaseTest.USER_ID,
            users=[
                BaseTest.OTHER_USER_ID,
                BaseTest.THIRD_USER_ID
            ],
            group_type=GroupTypes.PUBLIC_GROUP
        )

        group = self.get_group_info(group_id, count_messages=False)
        self.assertEqual(False, group["archived"])
        self.assertIsNone(group["archived_at"])

        self.update_group_archived(group_id, archived=True)

        group = self.get_group_info(group_id, count_messages=False)
        self.assertEqual(True, group["archived"])
        self.assertIsNotNone(group["archived_at"])

    def test_can_un_archive_groups(self):
        group_id = self.create_and_join_group(
            user_id=BaseTest.USER_ID,
            users=[
                BaseTest.OTHER_USER_ID,
                BaseTest.THIRD_USER_ID
            ],
            group_type=GroupTypes.PUBLIC_GROUP
        )

        self.update_group_archived(group_id, archived=True)

        group = self.get_group_info(group_id, count_messages=False)
        self.assertEqual(True, group["archived"])
        self.assertIsNotNone(group["archived_at"])

        self.update_group_archived(group_id, archived=False)

        group = self.get_group_info(group_id, count_messages=False)
        self.assertEqual(False, group["archived"])
        self.assertIsNone(group["archived_at"])

    def test_can_not_send_to_archived_groups(self):
        group_id = self.create_and_join_group(
            user_id=BaseTest.USER_ID,
            users=[
                BaseTest.OTHER_USER_ID,
                BaseTest.THIRD_USER_ID
            ],
            group_type=GroupTypes.PUBLIC_GROUP
        )

        self.update_group_archived(group_id, archived=True)

        group = self.get_group_info(group_id, count_messages=False)
        self.assertEqual(True, group["archived"])
        self.assertIsNotNone(group["archived_at"])

        self.send_message_to_group_from(group_id, BaseTest.OTHER_USER_ID, expected_error_code=607)

    def test_only_admins_can_list_archived_groups(self):
        group_id = self.create_and_join_group(
            user_id=BaseTest.USER_ID,
            users=[
                BaseTest.OTHER_USER_ID,
                BaseTest.THIRD_USER_ID
            ],
            group_type=GroupTypes.PUBLIC_GROUP
        )

        groups = self.get_public_groups()
        self.assertEqual(1, len(groups))

        self.update_group_archived(group_id, archived=True)

        groups = self.get_public_groups()
        self.assertEqual(0, len(groups))

        groups = self.get_public_groups(include_archived=True, admin_id=1971)
        self.assertEqual(1, len(groups))

    def test_no_archived_groups_listed_without_admin_id(self):
        group_id = self.create_and_join_group(
            user_id=BaseTest.USER_ID,
            users=[
                BaseTest.OTHER_USER_ID,
                BaseTest.THIRD_USER_ID
            ],
            group_type=GroupTypes.PUBLIC_GROUP
        )

        groups = self.get_public_groups()
        self.assertEqual(1, len(groups))

        self.update_group_archived(group_id, archived=True)

        groups = self.get_public_groups(include_archived=True, admin_id=None)
        self.assertEqual(0, len(groups))

        groups = self.get_public_groups(include_archived=False, admin_id=1971)
        self.assertEqual(0, len(groups))

    def test_public_groups_can_have_language(self):
        self.create_and_join_group(
            user_id=BaseTest.USER_ID,
            users=[
                BaseTest.OTHER_USER_ID,
                BaseTest.THIRD_USER_ID
            ],
            group_type=GroupTypes.PUBLIC_GROUP,
            language='de'
        )

        groups = self.get_public_groups()
        self.assertEqual(1, len(groups))
        self.assertEqual('de', groups[0]['language'])

        groups = self.groups_for_user()
        self.assertEqual(1, len(groups))
        self.assertEqual('de', groups[0]['group']['language'])

    def test_private_groups_can_not_have_language(self):
        self.create_and_join_group(
            user_id=BaseTest.USER_ID,
            users=[
                BaseTest.OTHER_USER_ID,
                BaseTest.THIRD_USER_ID
            ],
            group_type=GroupTypes.PRIVATE_GROUP,
            language='de'
        )

        groups = self.groups_for_user(BaseTest.USER_ID)
        self.assertEqual(1, len(groups))
        self.assertIsNone(groups[0]['group']['language'])

    def test_get_private_groups_for_spoken_languages(self):
        self.create_and_join_group(
            user_id=BaseTest.USER_ID,
            users=[
                BaseTest.OTHER_USER_ID,
                BaseTest.THIRD_USER_ID
            ],
            group_type=GroupTypes.PUBLIC_GROUP,
            language='de'
        )
        self.create_and_join_group(
            user_id=BaseTest.USER_ID,
            users=[
                BaseTest.OTHER_USER_ID,
                BaseTest.THIRD_USER_ID
            ],
            group_type=GroupTypes.PUBLIC_GROUP,
            language='jp'
        )

        groups = self.get_public_groups(spoken_languages=None)
        self.assertEqual(2, len(groups))

        groups = self.get_public_groups(spoken_languages=['sv'])
        self.assertEqual(0, len(groups))

        groups = self.get_public_groups(spoken_languages=['de'])
        self.assertEqual(1, len(groups))

        groups = self.get_public_groups(spoken_languages=['de', 'jp'])
        self.assertEqual(2, len(groups))

        groups = self.get_public_groups(spoken_languages=['sv', 'jp'])
        self.assertEqual(1, len(groups))
