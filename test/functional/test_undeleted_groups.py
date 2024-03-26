from test.functional.base_functional import BaseServerRestApi


class TestUndeletedGroups(BaseServerRestApi):
    def test_get_undeleted_groups(self):
        # session = self.env.session_maker()

        group_ids = [
            self.send_1v1_message(user_id=1234, receiver_id=4444)["group_id"],
            self.send_1v1_message(user_id=2345, receiver_id=4444)["group_id"],
            self.send_1v1_message(user_id=3456, receiver_id=4444)["group_id"],
            self.send_1v1_message(user_id=4567, receiver_id=4444)["group_id"]
        ]

        groups = self.get_undeleted_groups_for_user(user_id=4444)["stats"]
        self.assertEqual(len(group_ids), len(groups))

        for group in groups:
            self.assertIn(group["group_id"], group_ids)
            self.assertEqual(1, group["group_type"])
            self.assertIsNotNone(group["join_time"])
