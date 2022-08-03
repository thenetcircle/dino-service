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
        self.delete_all_groups()
        self.assert_groups_for_user(0)

        response = self.histories_for(group_id, assert_response=False)
        self.assert_error(response, error_code=ErrorCodes.USER_NOT_IN_GROUP)
