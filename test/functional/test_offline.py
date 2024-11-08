from dinofw.utils.config import GroupTypes
from test.base import BaseTest
from test.functional.base_functional import BaseServerRestApi


class TestOfflineUsers(BaseServerRestApi):
    def test_offline_users_removed_from_rooms(self):
        group_id = self.create_and_join_group(
            user_id=BaseTest.USER_ID,
            group_type=GroupTypes.PUBLIC_ROOM
        )
        self.user_joins_group(group_id, user_id=BaseTest.OTHER_USER_ID)

        self.assert_user_in_group(group_id, user_id=BaseTest.USER_ID)
        self.assert_user_in_group(group_id, user_id=BaseTest.OTHER_USER_ID)

        self.env.db.remove_user_stats_for_offline_users(
            user_ids=[BaseTest.USER_ID, BaseTest.OTHER_USER_ID],
            db=self.env.session_maker()
        )

        # removed when going offline
        self.assert_user_not_in_group(group_id, user_id=BaseTest.USER_ID)
        self.assert_user_not_in_group(group_id, user_id=BaseTest.OTHER_USER_ID)

    def test_offline_users_not_affecting_private_groups(self):
        group_id = self.create_and_join_group(
            user_id=BaseTest.USER_ID,
            group_type=GroupTypes.PRIVATE_GROUP
        )
        self.user_joins_group(group_id, user_id=BaseTest.OTHER_USER_ID)

        self.assert_user_in_group(group_id, user_id=BaseTest.USER_ID)
        self.assert_user_in_group(group_id, user_id=BaseTest.OTHER_USER_ID)

        self.env.db.remove_user_stats_for_offline_users(
            user_ids=[BaseTest.USER_ID, BaseTest.OTHER_USER_ID],
            db=self.env.session_maker()
        )

        # still in the group after going offline
        self.assert_user_in_group(group_id, user_id=BaseTest.USER_ID)
        self.assert_user_in_group(group_id, user_id=BaseTest.OTHER_USER_ID)

    def assert_user_in_group(self, group_id: str, user_id: int):
        self._assert_user_in_group(group_id, user_id, should_exist=True)

    def assert_user_not_in_group(self, group_id: str, user_id: int):
        self._assert_user_in_group(group_id, user_id, should_exist=False)

    def _assert_user_in_group(self, group_id: str, user_id: int, should_exist: bool):
        group = self.get_group_info(group_id, count_messages=False)

        found = False
        for user in group["users"]:
            if user["user_id"] == user_id:
                found = True
                break

        self.assertEqual(found, should_exist)
