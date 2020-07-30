import time

import arrow

from dinofw.rest.server.models import AbstractQuery
from test.base import BaseTest
from test.functional.base_db import BaseDatabaseTest


class BaseServerRestApi(BaseDatabaseTest):
    def update_delete_before(self, group_id: str, delete_before: float, user_id: int = None):
        if user_id is None:
            user_id = BaseTest.USER_ID

        raw_response = self.client.put(
            f"/v1/groups/{group_id}/userstats/{user_id}",
            json={
                "delete_before": delete_before,
            },
        )
        self.assertEqual(raw_response.status_code, 200)

    def send_message_to_group_from(self, group_id: str, user_id: int = None, amount: int = 1, delay: int = 0) -> list:
        if user_id is None:
            user_id = BaseTest.USER_ID

        messages = list()

        for _ in range(amount):
            raw_response = self.client.post(
                f"/v1/groups/{group_id}/users/{user_id}/send",
                json={
                    "message_payload": "test message",
                    "message_type": "text",
                },
            )
            self.assertEqual(raw_response.status_code, 200)
            messages.append(raw_response.json())

            if delay > 0:
                time.sleep(delay / 1000)

        return messages

    def create_and_join_group(self, user_id: int = None) -> str:
        if user_id is None:
            user_id = BaseTest.USER_ID

        raw_response = self.client.post(
            f"/v1/users/{user_id}/groups/create",
            json={
                "group_name": "a new group",
                "group_type": 0,
                "users": [user_id],
            },
        )
        self.assertEqual(raw_response.status_code, 200)

        return raw_response.json()["group_id"]

    def user_leaves_group(self, group_id: str, user_id: int = None) -> None:
        if user_id is None:
            user_id = BaseTest.USER_ID

        raw_response = self.client.delete(f"/v1/groups/{group_id}/users/{user_id}/join")
        self.assertEqual(raw_response.status_code, 200)

    def user_joins_group(self, group_id: str, user_id: int = None) -> None:
        if user_id is None:
            user_id = BaseTest.USER_ID

        raw_response = self.client.put(f"/v1/groups/{group_id}/users/{user_id}/join")
        self.assertEqual(raw_response.status_code, 200)

    def update_hide_group_for(self, group_id: str, hide: bool, user_id: int = None):
        if user_id is None:
            user_id = BaseTest.USER_ID

        raw_response = self.client.put(
            f"/v1/groups/{group_id}/userstats/{user_id}",
            json={"hide": hide},
        )
        self.assertEqual(raw_response.status_code, 200)

    def update_user_stats_to_now(self, group_id: str, user_id: int = None):
        if user_id is None:
            user_id = BaseTest.USER_ID

        now = arrow.utcnow().datetime
        now_ts = AbstractQuery.to_ts(now)

        raw_response = self.client.put(
            f"/v1/groups/{group_id}/userstats/{user_id}",
            json={"last_read_time": now_ts},
        )
        self.assertEqual(raw_response.status_code, 200)

        return float(now_ts)

    def get_user_stats(self, group_id: str, user_id: int = None):
        if user_id is None:
            user_id = BaseTest.USER_ID

        raw_response = self.client.get(f"/v1/groups/{group_id}/userstats/{user_id}")
        self.assertEqual(raw_response.status_code, 200)

        return raw_response.json()

    def assert_messages_in_group(self, group_id: str, user_id: int = None, amount: int = 0):
        raw_response = self.client.post(
            f"/v1/groups/{group_id}/user/{user_id}/histories",
            json={
                "per_page": 100,
            },
        )
        self.assertEqual(raw_response.status_code, 200)
        self.assertEqual(amount, len(raw_response.json()["messages"]))

    def assert_hidden_for_user(self, hidden: bool, group_id: str, user_id: int = None) -> None:
        if user_id is None:
            user_id = BaseTest.USER_ID

        raw_response = self.client.get(
            f"/v1/groups/{group_id}/userstats/{user_id}",
        )
        self.assertEqual(raw_response.status_code, 200)
        self.assertEqual(hidden, raw_response.json()["hide"])

    def assert_groups_for_user(self, amount_of_groups, user_id: int = None) -> None:
        if user_id is None:
            user_id = BaseTest.USER_ID

        raw_response = self.client.post(
            f"/v1/users/{user_id}/groups", json={"per_page": "10"},
        )
        self.assertEqual(raw_response.status_code, 200)
        self.assertEqual(amount_of_groups, len(raw_response.json()))
