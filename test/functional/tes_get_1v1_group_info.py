from test.functional.base_functional import BaseServerRestApi


class TestReceiverStats(BaseServerRestApi):
    def test_receiver_stats(self):
        self.send_1v1_message()
        info = self.get_1v1_group_info()
        self.assertIsNotNone(info)

        self.assertLess(1, info["stats"][0]["receiver_delete_before"])
        self.assertLess(1, info["stats"][1]["receiver_delete_before"])

        self.assertLess(1, info["stats"][0]["receiver_highlight_time"])
        self.assertLess(1, info["stats"][1]["receiver_highlight_time"])

        self.assertLess(1, info["stats"][0]["receiver_delete_before"])
        self.assertLess(1, info["stats"][1]["receiver_delete_before"])

        self.assertFalse(info["stats"][0]["receiver_deleted"])
        self.assertFalse(info["stats"][1]["receiver_deleted"])
