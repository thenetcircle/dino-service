from test.functional.base_functional import BaseServerRestApi


class TestHistory(BaseServerRestApi):
    async def test_get_all_message_history_in_group(self):
        await self.assert_groups_for_user(0)

        group_id = (await self.send_1v1_message())["group_id"]
        await self.assert_groups_for_user(1)
        await self.assert_all_history(group_id, 1)

        await self.send_1v1_message()
        await self.assert_groups_for_user(1)
        await self.assert_all_history(group_id, 2)
