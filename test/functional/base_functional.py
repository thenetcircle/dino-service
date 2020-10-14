import time
from uuid import uuid4 as uuid

import arrow

from dinofw.rest.models import AbstractQuery
from dinofw.utils.config import MessageTypes
from test.base import BaseTest
from test.functional.base_db import BaseDatabaseTest


class BaseServerRestApi(BaseDatabaseTest):
    def get_group(self, group_id: str, user_id: int = BaseTest.USER_ID) -> dict:
        raw_response = self.client.post(
            f"/v1/users/{user_id}/groups", json={"per_page": "10"},
        )
        self.assertEqual(raw_response.status_code, 200)

        for group in raw_response.json():
            if group["group"]["group_id"] == group_id:
                return group

        return dict()

    def update_delete_before(
        self, group_id: str, delete_before: float, user_id: int = BaseTest.USER_ID
    ):
        raw_response = self.client.put(
            f"/v1/groups/{group_id}/user/{user_id}/update",
            json={"delete_before": delete_before,},
        )
        self.assertEqual(raw_response.status_code, 200)

    def bookmark_group(
        self, group_id: str, bookmark: bool, user_id: int = BaseTest.USER_ID
    ):
        raw_response = self.client.put(
            f"/v1/groups/{group_id}/user/{user_id}/update",
            json={"bookmark": bookmark},
        )
        self.assertEqual(raw_response.status_code, 200)

    def action_log(self, group_id: str, user_id: int = BaseTest.USER_ID):
        raw_response = self.client.post(
            f"/v1/groups/{group_id}/actions",
            json={
                "user_ids": [user_id],
                "payload": f"User {user_id} joined the group.",
                "action_type": 1
            },
        )
        self.assertEqual(raw_response.status_code, 200)

        return raw_response.json()

    def send_message_to_group_from(
        self, group_id: str, user_id: int = BaseTest.USER_ID, amount: int = 1, delay: int = 0
    ) -> list:
        messages = list()

        for _ in range(amount):
            raw_response = self.client.post(
                f"/v1/groups/{group_id}/user/{user_id}/send",
                json={
                    "message_payload": BaseTest.MESSAGE_PAYLOAD,
                    "message_type": MessageTypes.MESSAGE,
                },
            )
            self.assertEqual(raw_response.status_code, 200)
            messages.append(raw_response.json())

            if delay > 0:
                time.sleep(delay / 1000)

        return messages

    def create_and_join_group(self, user_id: int = BaseTest.USER_ID) -> str:
        raw_response = self.client.post(
            f"/v1/users/{user_id}/groups/create",
            json={"group_name": "a new group", "group_type": 0, "users": [user_id],},
        )
        self.assertEqual(raw_response.status_code, 200)

        return raw_response.json()["group_id"]

    def last_read_in_histories_for(self, histories: dict, user_id: int):
        return [
            stat["last_read"]
            for stat in histories["last_reads"]
            if stat["user_id"] == user_id
        ][0]

    def histories_for(self, group_id: str, user_id: int = BaseTest.USER_ID):
        raw_response = self.client.post(
            f"/v1/groups/{group_id}/user/{user_id}/histories", json={"per_page": "10"},
        )
        self.assertEqual(raw_response.status_code, 200)

        return raw_response.json()

    def user_leaves_group(self, group_id: str, user_id: int = BaseTest.USER_ID) -> None:
        raw_response = self.client.delete(f"/v1/groups/{group_id}/user/{user_id}/join")
        self.assertEqual(raw_response.status_code, 200)

    def user_joins_group(self, group_id: str, user_id: int = BaseTest.USER_ID) -> None:
        raw_response = self.client.put(f"/v1/groups/{group_id}/user/{user_id}/join")
        self.assertEqual(raw_response.status_code, 200)

    def update_hide_group_for(self, group_id: str, hide: bool, user_id: int = BaseTest.USER_ID):
        raw_response = self.client.put(
            f"/v1/groups/{group_id}/user/{user_id}/update", json={"hide": hide},
        )
        self.assertEqual(raw_response.status_code, 200)

    def update_user_stats_to_now(self, group_id: str, user_id: int = BaseTest.USER_ID):
        now = arrow.utcnow().datetime
        now_ts = AbstractQuery.to_ts(now)

        raw_response = self.client.put(
            f"/v1/groups/{group_id}/user/{user_id}/update",
            json={"last_read_time": now_ts},
        )
        self.assertEqual(raw_response.status_code, 200)

        return float(now_ts)

    def get_global_user_stats(self, user_id: int = BaseTest.USER_ID):
        raw_response = self.client.get(f"/v1/userstats/{user_id}")
        self.assertEqual(raw_response.status_code, 200)

        return raw_response.json()

    def get_user_stats(self, group_id: str, user_id: int = BaseTest.USER_ID):
        raw_response = self.client.get(f"/v1/groups/{group_id}/user/{user_id}")
        self.assertEqual(raw_response.status_code, 200)

        return raw_response.json()

    def groups_updated_since(self, user_id: int, since: float):
        raw_response = self.client.post(
            f"/v1/users/{user_id}/groups/updates",
            json={"since": since, "count_unread": False, "per_page": 100,},
        )
        self.assertEqual(raw_response.status_code, 200)

        return raw_response.json()

    def groups_for_user(
            self,
            user_id: int = BaseTest.USER_ID,
            count_unread: bool = False,
            only_unread: bool = False,
            hidden: bool = False,
    ):
        raw_response = self.client.post(
            f"/v1/users/{user_id}/groups",
            json={
                "per_page": "10",
                "count_unread": count_unread,
                "only_unread": only_unread,
                "hidden": hidden,
            },
        )
        self.assertEqual(raw_response.status_code, 200)

        return raw_response.json()

    def mark_as_read(self, user_id: int = BaseTest.USER_ID):
        raw_response = self.client.put(
            f"/v1/user/{user_id}/read",
        )
        self.assertEqual(raw_response.status_code, 201)

        # async api
        time.sleep(0.1)

    def leave_all_groups(self, user_id: int = BaseTest.USER_ID):
        raw_response = self.client.delete(
            f"/v1/users/{user_id}/groups"
        )
        self.assertEqual(raw_response.status_code, 201)

        # async api
        time.sleep(0.1)

    def pin_group_for(self, group_id: str, user_id: int = BaseTest.USER_ID) -> None:
        self._set_pin_group_for(group_id, user_id, pinned=True)

    def unpin_group_for(self, group_id: str, user_id: int = BaseTest.USER_ID) -> None:
        self._set_pin_group_for(group_id, user_id, pinned=False)

    def _set_pin_group_for(
        self, group_id: str, user_id: int = BaseTest.USER_ID, pinned: bool = False
    ) -> None:
        raw_response = self.client.put(
            f"/v1/groups/{group_id}/user/{user_id}/update", json={"pin": pinned}
        )
        self.assertEqual(raw_response.status_code, 200)

    def edit_group(
        self,
        group_id: str,
        name: str = "test name",
        weight: int = 1,
        context: str = "",
        owner: int = None,
    ):
        if owner is None:
            owner = BaseTest.USER_ID

        raw_response = self.client.put(
            f"/v1/groups/{group_id}",
            json={"owner": owner, "name": name, "weight": weight, "context": context,},
        )
        self.assertEqual(raw_response.status_code, 200)

    def edit_message(
        self, group_id: str, message_id: str, created_at: float, new_payload: str
    ):
        raw_response = self.client.put(
            f"/v1/groups/{group_id}/message/{message_id}/edit",
            json={"created_at": created_at, "message_payload": new_payload,},
        )
        self.assertEqual(raw_response.status_code, 200)

    def highlight_group_for_user(self, group_id: str, user_id: int) -> None:
        now_plus_2_days = arrow.utcnow().shift(days=2).datetime
        now_plus_2_days = AbstractQuery.to_ts(now_plus_2_days)

        raw_response = self.client.put(
            f"/v1/groups/{group_id}/user/{user_id}/update",
            json={"highlight_time": now_plus_2_days,},
        )
        self.assertEqual(raw_response.status_code, 200)

    def delete_highlight_group_for_user(self, group_id: str, user_id: int) -> None:
        now = arrow.utcnow().datetime

        raw_response = self.client.put(
            f"/v1/groups/{group_id}/user/{user_id}/update",
            json={"last_read_time": now,},
        )
        self.assertEqual(raw_response.status_code, 200)

    def get_1v1_group_info(self, user_id: int = BaseTest.USER_ID, receiver_id: int = None):
        if receiver_id is None:
            receiver_id = BaseTest.OTHER_USER_ID

        raw_response = self.client.post(
            f"/v1/users/{user_id}/group", json={"receiver_id": receiver_id,},
        )
        self.assertEqual(raw_response.status_code, 200)

        return raw_response.json()

    def send_1v1_message(
        self,
        message_type: int = MessageTypes.MESSAGE,
        user_id: int = BaseTest.USER_ID,
        receiver_id: int = BaseTest.OTHER_USER_ID,
    ) -> dict:
        json_data = {
            "receiver_id": receiver_id,
            "message_type": message_type
        }

        raw_response = self.client.post(
            f"/v1/users/{user_id}/send",
            json=json_data,
        )
        self.assertEqual(raw_response.status_code, 200)

        return raw_response.json()

    def attachments_for(self, group_id: str, user_id: int = BaseTest.USER_ID):
        now = arrow.utcnow().float_timestamp

        raw_response = self.client.post(
            f"/v1/groups/{group_id}/user/{user_id}/attachments",
            json={"per_page": 100, "until": now,},
        )
        self.assertEqual(raw_response.status_code, 200)

        return raw_response.json()

    def update_attachment(
        self,
        message_id: str,
        created_at: float,
        user_id: int = BaseTest.USER_ID,
        receiver_id: int = BaseTest.OTHER_USER_ID,
    ):
        raw_response = self.client.post(
            f"/v1/users/{user_id}/message/{message_id}/attachment",
            json={
                "file_id": BaseTest.FILE_ID,
                "status": BaseTest.FILE_STATUS,
                "message_payload": BaseTest.FILE_CONTEXT,
                "created_at": created_at,
                "receiver_id": receiver_id,
            },
        )
        self.assertEqual(raw_response.status_code, 200)

        return raw_response.json()

    def attachment_for_file_id(self, group_id: str, file_id: str, assert_response: bool = True):
        raw_response = self.client.get(
            f"/v1/group/{group_id}/attachment/{file_id}"
        )
        if assert_response:
            self.assertEqual(raw_response.status_code, 200)

        return raw_response.json()

    def assert_error(self, response, error_code):
        self.assertEqual(int(response["detail"].split(":")[0]), error_code)

    def assert_messages_in_group(
        self, group_id: str, user_id: int = BaseTest.USER_ID, amount: int = 0
    ):
        raw_response = self.client.post(
            f"/v1/groups/{group_id}/user/{user_id}/histories", json={"per_page": 100,},
        )
        self.assertEqual(raw_response.status_code, 200)
        self.assertEqual(amount, len(raw_response.json()["messages"]))

    def assert_hidden_for_user(
        self, hidden: bool, group_id: str, user_id: int = BaseTest.USER_ID
    ) -> None:
        raw_response = self.client.get(f"/v1/groups/{group_id}/user/{user_id}",)
        self.assertEqual(raw_response.status_code, 200)
        self.assertEqual(hidden, raw_response.json()["hide"])

    def assert_groups_for_user(self, amount_of_groups, user_id: int = BaseTest.USER_ID) -> None:
        response = self.groups_for_user(user_id)
        self.assertEqual(amount_of_groups, len(response))

    def assert_total_unread_count(self, user_id: int, unread_count: int):
        raw_response = self.client.get(f"/v1/userstats/{user_id}")
        self.assertEqual(raw_response.status_code, 200)
        self.assertEqual(unread_count, raw_response.json()["unread_amount"])

    def assert_order_of_groups(self, user_id: int, *group_ids):
        groups = self.groups_for_user(user_id)
        for i, group_id in enumerate(group_ids):
            self.assertEqual(group_id, groups[i]["group"]["group_id"])

    def assert_in_histories(self, user_id: int, histories, is_in: bool):
        if is_in:
            self.assertTrue(
                any((user_id == user["user_id"] for user in histories["last_reads"]))
            )
        else:
            self.assertFalse(
                any((user_id == user["user_id"] for user in histories["last_reads"]))
            )

    def assert_payload(self, group_id: str, message_id: str, new_payload: str):
        histories = self.histories_for(group_id)

        message_payload = ""

        for message in histories["messages"]:
            if message["message_id"] == message_id:
                message_payload = message["message_payload"]
                break

        self.assertEqual(new_payload, message_payload)
