from unittest import TestCase
import json

from dinofw.utils import truncate_json_message


class TestUserResource(TestCase):
    def test_truncate_none(self):
        self.assertIsNone(truncate_json_message(None, limit=10))

    def test_truncate_no_content(self):
        msg = '{"other_key":"asdf"}'
        self.assertEqual(
            msg,
            truncate_json_message(msg, limit=10)
        )

    def test_truncate_content_is_okay(self):
        msg = '{"content":"asdf"}'
        self.assertEqual(
            msg,
            truncate_json_message(msg, limit=10)
        )

    def test_truncate_content_is_too_long(self):
        limit = 10
        msg = '{"content":"aaaaabbbbbccccc"}'
        truncated = truncate_json_message(msg, limit=limit)

        self.assertLess(len(truncated), len(msg))

        truncated_json = json.loads(truncated)
        msg_json = json.loads(msg)

        self.assertEqual(10, len(truncated_json["content"]))
        self.assertLess(len(truncated_json["content"]), len(msg_json["content"]))
