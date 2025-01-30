from test.base import BaseTest
from test.functional.base_functional import BaseServerRestApi


class TestReceiverStats(BaseServerRestApi):
    async def test_receiver_stats(self):
        await self.send_1v1_message()
        info = await self.get_1v1_group_info()
        self.assertIsNotNone(info)

        self.assertLess(1, info["stats"][0]["receiver_delete_before"])
        self.assertLess(1, info["stats"][1]["receiver_delete_before"])

        self.assertLess(1, info["stats"][0]["receiver_highlight_time"])
        self.assertLess(1, info["stats"][1]["receiver_highlight_time"])

        self.assertLess(1, info["stats"][0]["receiver_delete_before"])
        self.assertLess(1, info["stats"][1]["receiver_delete_before"])

        self.assertFalse(info["stats"][0]["receiver_deleted"])
        self.assertFalse(info["stats"][1]["receiver_deleted"])

    async def test_attachment_count_in_1v1_info(self):
        await self.send_1v1_message(user_id=BaseTest.USER_ID)
        info = await self.get_1v1_group_info()
        self.assertIsNotNone(info)

        for stat in info["stats"]:
            # only counted for one user
            if stat["user_id"] == BaseTest.USER_ID:
                self.assertEqual(0, stat["attachment_amount"])
            else:
                self.assertEqual(-1, stat["attachment_amount"])

        self.assertEqual(1, info["group"]["message_amount"])

    async def test_attachment_count_in_1v1_info_with_attachment(self):
        await self.send_1v1_message()
        await self.create_attachment()

        info = await self.get_1v1_group_info()
        self.assertIsNotNone(info)

        for stat in info["stats"]:
            # only counted for one user
            if stat["user_id"] == BaseTest.USER_ID:
                self.assertEqual(1, stat["attachment_amount"])
            else:
                self.assertEqual(-1, stat["attachment_amount"])

        self.assertEqual(2, info["group"]["message_amount"])
