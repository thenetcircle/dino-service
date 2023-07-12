from test.functional.base_functional import BaseServerRestApi


class TestHistory(BaseServerRestApi):
    def test_get_all_message_history_in_group(self):
        self.assert_groups_for_user(0)

        group_id = self.send_1v1_message()["group_id"]
        self.assert_groups_for_user(1)
        self.assert_all_history(group_id, 1)

        self.send_1v1_message()
        self.assert_groups_for_user(1)
        self.assert_all_history(group_id, 2)
