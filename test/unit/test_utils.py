import json
from unittest import TestCase

import arrow

from dinofw.utils import to_ts
from dinofw.utils import truncate_json_message


class TestUserResource(TestCase):
    def test_to_ts_not_adding_1ms_if_round_adds_1ms(self):
        to_convert = '1675148709.6019'
        expected = '1675148709.602'
        self.assertEqual(expected, str(to_ts(arrow.get(float(to_convert)))))

    def test_to_ts_is_adding_1ms_if_round_does_not_add_1ms(self):
        to_convert = '1675148709.6011'
        expected = '1675148709.602'
        self.assertEqual(expected, str(to_ts(arrow.get(float(to_convert)))))

    def test_to_ts_is_not_adding_1ms_if_no_micros(self):
        to_convert = '1675148709.601'
        expected = '1675148709.601'
        self.assertEqual(expected, str(to_ts(arrow.get(float(to_convert)))))

    def test_to_ts_is_adding_1ms_if_1_micros(self):
        to_convert = '1675148709.601001'
        expected = '1675148709.602'
        self.assertEqual(expected, str(to_ts(arrow.get(float(to_convert)))))

    def test_to_ts_is_adding_1ms_if_999_micros(self):
        to_convert = '1675148709.601999'
        expected = '1675148709.602'
        self.assertEqual(expected, str(to_ts(arrow.get(float(to_convert)))))

    def test_to_ts_is_adding_1ms_if_500_micros(self):
        to_convert = '1675148709.601500'
        expected = '1675148709.602'
        self.assertEqual(expected, str(to_ts(arrow.get(float(to_convert)))))

    def test_to_ts_is_not_adding_1ms_if_no_millis(self):
        to_convert = '1675148709.000000'
        expected = '1675148709.0'
        self.assertEqual(expected, str(to_ts(arrow.get(float(to_convert)))))

    def test_to_ts_is_adding_1ms_if_only_micros(self):
        to_convert = '1675148709.000001'
        expected = '1675148709.001'
        self.assertEqual(expected, str(to_ts(arrow.get(float(to_convert)))))

    def test_to_ts_is_adding_1ms_if_will_round_1s(self):
        to_convert = '1675148709.999999'
        expected = '1675148710.0'
        self.assertEqual(expected, str(to_ts(arrow.get(float(to_convert)))))

    def test_to_ts_is_not_adding_1ms_if_999ms(self):
        to_convert = '1675148709.999'
        expected = '1675148709.999'
        self.assertEqual(expected, str(to_ts(arrow.get(float(to_convert)))))

    def test_to_ts_is_not_adding_1ms_if_999001_micros(self):
        to_convert = '1675148709.999001'
        expected = '1675148710.0'
        self.assertEqual(expected, str(to_ts(arrow.get(float(to_convert)))))

    def test_non_ascii(self):
        s = '{"content": "Ganz gut, selbst? dass kinto-gui.py in (/root/ auflöst ausführen), während die es ... ẞ ß"}'
        self.assertEqual(
            70,
            len(truncate_json_message(s, limit=55))
        )

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

    def test_truncate_content_is_too_long_ignore_other_keys(self):
        limit = 10
        msg = '{"content":"aaaaabbbbbccccc","other_key":"sooome-vaaaalue"}'
        truncated = truncate_json_message(msg, limit=limit, only_content=True)

        self.assertLess(len(truncated), len(msg))

        truncated_json = json.loads(truncated)
        msg_json = json.loads(msg)

        self.assertEqual(10, len(truncated_json["content"]))
        self.assertLess(len(truncated_json["content"]), len(msg_json["content"]))
        self.assertNotIn("other_key", truncated_json.keys())

    def test_truncate_content_is_too_long_do_not_ignore_other_keys(self):
        limit = 10
        msg = '{"content":"aaaaabbbbbccccc","other_key":"sooome-vaaaalue"}'
        truncated = truncate_json_message(msg, limit=limit, only_content=False)

        self.assertLess(len(truncated), len(msg))

        truncated_json = json.loads(truncated)
        msg_json = json.loads(msg)

        self.assertEqual(10, len(truncated_json["content"]))
        self.assertLess(len(truncated_json["content"]), len(msg_json["content"]))
        self.assertIn("other_key", truncated_json.keys())

    def test_truncate_emojis(self):
        payload = "{\"content\":\"\\ud83c\\udf08\\ud83c\\udf08\\ud83c\\udf08\\ud83c\\udf08\\ud83c\\udf08\\ud83c\\udf08\\ud83c\\udf08\\ud83c\\udf08\\ud83c\\udf08\\ud83c\\udf08\\ud83c\\udf08\\ud83c\\udf08\\ud83c\\udf08\\ud83c\\udf08\\ud83c\\udf08\\ud83c\\udf08\\ud83c\\udf08\\ud83c\\udf08\\ud83c\\udf08\\ud83c\\udf08\\ud83c\\udf08\\ud83c\\udf08\\ud83c\\udf08\\ud83c\\udf08\\ud83c\\udf08\\ud83c\\udf08\\ud83c\\udf08\\ud83c\\udf08\\ud83c\\udf08\\ud83c\\udf08\\ud83c\\udf08\\ud83c\\udf08\\ud83c\\udf08\\ud83c\\udf08\\ud83c\\udf08\\ud83e\\uddd8\\u200d\\u2642\\ufe0f\\ud83e\\uddd8\\u200d\\u2642\\ufe0f\\ud83c\\udf08\\ud83c\\udf08\\ud83e\\uddd8\\u200d\\u2642\\ufe0f\\ud83c\\udf08\\ud83c\\udf08\\ud83c\\udf08\\ud83c\\udf08\\ud83c\\udf08\\ud83c\\udf08\\ud83c\\udf08\\ud83c\\udf08\\ud83c\\udf08\\ud83c\\udf08\\ud83c\\udf08\\ud83c\\udf08\\ud83c\\udf08\\ud83c\\udf08\\ud83c\\udf08\\ud83e\\uddd8\\u200d\\u2642\\ufe0f\\ud83e\\uddd8\\u200d\\u2642\\ufe0f\\ud83e\\uddd8\\u200d\\u2642\\ufe0f\\ud83e\\uddd8\\u200d\\u2642\\ufe0f\\ud83e\\uddd8\\u200d\\u2642\\ufe0f\\ud83e\\uddd8\\u200d\\u2642\\ufe0f\\ud83c\\udf08\\ud83e\\uddd8\\u200d\\u2642\\ufe0f\\ud83e\\uddd8\\u200d\\u2642\\ufe0f\\ud83e\\uddd8\\u200d\\u2642\\ufe0f\\ud83e\\uddd8\\u200d\\u2642\\ufe0f\\ud83c\\udf08\\ud83c\\udf08\\ud83e\\uddd8\\u200d\\u2642\\ufe0f\\ud83c\\udf08\\ud83c\\udf08\\ud83c\\udf08\\ud83c\\udf08\\ud83c\\udf08\\ud83c\\udf08\\ud83c\\udf08\\ud83c\\udf08\\ud83c\\udf08\"}"

        length_raw = len(payload)
        truncated = truncate_json_message(payload, limit=500, only_content=True)
        length_truncated = len(truncated)
        loaded = json.loads(payload)
        print(length_truncated, length_raw)
