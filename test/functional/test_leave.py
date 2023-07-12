from dinofw.utils.config import ErrorCodes
from test.base import BaseTest
from test.functional.base_functional import BaseServerRestApi


class TestLeaveGroup(BaseServerRestApi):
    def test_leave_removes_user_from_user_list(self):
        self.assert_groups_for_user(0, user_id=BaseTest.USER_ID)

        group_id = self.create_and_join_group(
            user_id=BaseTest.USER_ID,
            users=[
                BaseTest.OTHER_USER_ID,
                BaseTest.THIRD_USER_ID
            ]
        )
        self.assert_groups_for_user(1, user_id=BaseTest.OTHER_USER_ID)

        group_info = self.get_group_info(group_id, count_messages=False)
        users_in_group = {u["user_id"] for u in group_info["users"]}
        self.assertIn(BaseTest.OTHER_USER_ID, users_in_group)

        self.user_leaves_group(group_id, user_id=BaseTest.OTHER_USER_ID)

        group_info = self.get_group_info(group_id, count_messages=False)
        users_in_group = {u["user_id"] for u in group_info["users"]}
        self.assertNotIn(BaseTest.OTHER_USER_ID, users_in_group)

    def test_leave_removes_user_stats_entry(self):
        self.assert_groups_for_user(0, user_id=BaseTest.USER_ID)

        group_id = self.create_and_join_group(
            user_id=BaseTest.USER_ID,
            users=[
                BaseTest.OTHER_USER_ID,
                BaseTest.THIRD_USER_ID
            ]
        )
        self.assert_groups_for_user(1, user_id=BaseTest.USER_ID)

        result = self.get_user_stats(group_id, user_id=BaseTest.OTHER_USER_ID, status_code=200)
        self.assertEqual(group_id, result["group"]["group_id"])

        self.user_leaves_group(group_id, user_id=BaseTest.OTHER_USER_ID)

        result = self.get_user_stats(group_id, user_id=BaseTest.OTHER_USER_ID, status_code=400)
        self.assertEqual(ErrorCodes.USER_NOT_IN_GROUP, result["code"])

    def test_owner_leaves_resets_owner_id(self):
        self.assert_groups_for_user(0, user_id=BaseTest.USER_ID)

        group_id = self.create_and_join_group(
            user_id=BaseTest.USER_ID,
            users=[
                BaseTest.OTHER_USER_ID,
                BaseTest.THIRD_USER_ID
            ]
        )
        self.assert_groups_for_user(1, user_id=BaseTest.USER_ID)
        group_info = self.get_group_info(group_id, count_messages=False)
        self.assertEqual(BaseTest.USER_ID, group_info["owner_id"])

        self.user_leaves_group(group_id, user_id=BaseTest.USER_ID)

        group_info = self.get_group_info(group_id, count_messages=False)
        self.assertIsNone(group_info["owner_id"])

    def test_leave_group_creates_deleted_copy(self):
        self.assert_deleted_groups_for_user(0)
        self.assert_groups_for_user(0)

        group_id = self.create_and_join_group(
            user_id=BaseTest.USER_ID,
            users=[
                BaseTest.OTHER_USER_ID,
                BaseTest.THIRD_USER_ID
            ]
        )

        self.assert_deleted_groups_for_user(0)
        self.assert_groups_for_user(1)

        self.user_leaves_group(group_id)

        self.assert_deleted_groups_for_user(1)
        self.assert_groups_for_user(0)
