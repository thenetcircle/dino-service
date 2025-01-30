from test.functional.base_functional import BaseServerRestApi


class TestUndeletedGroups(BaseServerRestApi):
    async def test_get_undeleted_groups(self):
        # session = self.env.db_session

        group_ids = [
            (await self.send_1v1_message(user_id=1234, receiver_id=4444))["group_id"],
            (await self.send_1v1_message(user_id=2345, receiver_id=4444))["group_id"],
            (await self.send_1v1_message(user_id=3456, receiver_id=4444))["group_id"],
            (await self.send_1v1_message(user_id=4567, receiver_id=4444))["group_id"]
        ]

        groups = (await self.get_undeleted_groups_for_user(user_id=4444))["stats"]
        self.assertEqual(len(group_ids), len(groups))

        for group in groups:
            self.assertIn(group["group_id"], group_ids)
            self.assertEqual(1, group["group_type"])
            self.assertIsNotNone(group["join_time"])
