from dinofw.utils.config import ErrorCodes
from test.functional.base_functional import BaseServerRestApi


class TestDeleteProfile(BaseServerRestApi):
    def test_delete_profile(self):
        self.send_1v1_message(receiver_id=1111)
        self.send_1v1_message(receiver_id=2222)
        self.send_1v1_message(receiver_id=3333)
        msg = self.send_1v1_message(receiver_id=4444)
        group_id = msg["group_id"]

        self.assert_groups_for_user(4)
        self.delete_all_groups(create_action_log=False)
        self.assert_groups_for_user(0)

        response = self.histories_for(group_id, assert_response=False)
        self.assert_error(response, error_code=ErrorCodes.USER_NOT_IN_GROUP)

    def test_delete_profile_with_action_log(self):
        self.send_1v1_message(receiver_id=1111)
        self.send_1v1_message(receiver_id=2222)
        self.send_1v1_message(receiver_id=3333)
        msg = self.send_1v1_message(receiver_id=4444)
        group_id = msg["group_id"]

        self.assert_groups_for_user(4)
        self.delete_all_groups(create_action_log=True)
        self.assert_groups_for_user(0)

        response = self.histories_for(group_id, assert_response=False)
        self.assert_error(response, error_code=ErrorCodes.USER_NOT_IN_GROUP)

    def test_delete_profile_keeps_copy_in_deleted_table(self):
        self.send_1v1_message(receiver_id=1111)
        self.send_1v1_message(receiver_id=2222)
        self.send_1v1_message(receiver_id=3333)
        self.send_1v1_message(receiver_id=4444)

        self.assert_deleted_groups_for_user(0)
        self.assert_groups_for_user(4)

        self.delete_all_groups(create_action_log=False)

        self.assert_deleted_groups_for_user(4)
        self.assert_groups_for_user(0)
