import json
import time

import arrow

from dinofw.utils import to_ts
from dinofw.utils import utcnow_ts
from dinofw.utils.config import MessageTypes, PayloadStatus
from test.base import BaseTest
from test.functional.base_db import BaseDatabaseTest


class BaseServerRestApi(BaseDatabaseTest):
    def get_group(self, group_id: str, user_id: int = BaseTest.USER_ID) -> dict:
        raw_response = self.client.post(
            f"/v1/users/{user_id}/groups", json={
                "per_page": "10",
                "only_unread": False,
            },
        )
        self.assertEqual(raw_response.status_code, 200)

        for group in raw_response.json():
            if group["group"]["group_id"] == group_id:
                return group

        return dict()

    def create_attachment(self):
        group_message = self.send_1v1_message(
            message_type=MessageTypes.IMAGE,
            user_id=BaseTest.USER_ID,
            receiver_id=BaseTest.OTHER_USER_ID,
            payload=json.dumps({
                "content": "some payload",
                "status": PayloadStatus.PENDING
            })
        )

        message_id = group_message["message_id"]
        created_at = group_message["created_at"]
        group_id = group_message["group_id"]
        user_id = group_message["user_id"]

        self.update_attachment(
            message_id=message_id,
            created_at=created_at,
            file_id=BaseTest.FILE_ID,
            payload=json.dumps({
                "content": "some payload",
                "status": PayloadStatus.RESIZED
            })
        )

        return group_id, user_id

    def update_delete_before(
            self,
            group_id: str,
            delete_before: float,
            user_id: int = BaseTest.USER_ID,
            create_action_log: bool = False
    ):
        the_json = {
            "delete_before": delete_before
        }

        if create_action_log:
            the_json["action_log"] = {
                "payload": "user updated delete_before"
            }

        raw_response = self.client.put(
            f"/v1/groups/{group_id}/user/{user_id}/update",
            json=the_json,
        )
        self.assertEqual(raw_response.status_code, 200)

    def update_last_read(self, group_id: str, user_id: int):
        the_json = {
            "last_read_time": arrow.utcnow().timestamp()
        }

        raw_response = self.client.put(
            f"/v1/groups/{group_id}/user/{user_id}/update",
            json=the_json,
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

    def get_undeleted_groups_for_user(self, user_id: int = BaseTest.USER_ID):
        raw_response = self.client.get(f"/v1/undeleted/{user_id}/groups")
        self.assertEqual(raw_response.status_code, 200)

        return raw_response.json()

    def get_message_info(
        self, user_id: int, message_id: str, group_id: str, created_at: float, expected_response_code: int = 200
    ):

        raw_response = self.client.post(
            f"/v1/users/{user_id}/message/{message_id}/info",
            json={
                "group_id": group_id,
                "created_at": created_at
            },
        )

        self.assertEqual(raw_response.status_code, expected_response_code)
        return raw_response.json()

    def send_notification(self, group_id: str) -> None:
        raw_response = self.client.post(
            f"/v1/notification/send",
            json={"group_id": group_id, "event_type": "message", "notification": [{
                "data": {"test": "data"}
            }]},
        )
        self.assertEqual(raw_response.status_code, 200)

    def send_message_to_group_from(
        self, group_id: str, user_id: int = BaseTest.USER_ID, amount: int = 1, delay: int = 10
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

    def create_and_join_group(self, user_id: int = BaseTest.USER_ID, users: list = None) -> str:
        if users is None:
            users = [user_id]

        raw_response = self.client.post(
            f"/v1/users/{user_id}/groups/create",
            json={"group_name": "a new group", "group_type": 0, "users": users},
        )
        self.assertEqual(raw_response.status_code, 200)
        time.sleep(0.01)

        return raw_response.json()["group_id"]

    def delete_all_groups(self, user_id: int = BaseTest.USER_ID, create_action_log: bool = True):
        if create_action_log:
            data = {
                "action_log": {
                    "payload": "some payload for action log"
                }
            }
        else:
            data = dict()

        raw_response = self.client.delete(
            f"/v1/users/{user_id}/groups", json=data
        )

        self.assertEqual(raw_response.status_code, 201)

        # async api
        time.sleep(0.5)

    def histories_for(
            self,
            group_id: str,
            user_id: int = BaseTest.USER_ID,
            admin: bool = False,
            include_deleted: bool = False,
            assert_response: bool = True
    ):
        json_data = {"per_page": "10", "since": 0}

        if admin:
            json_data["admin_id"] = 1971
        if include_deleted:
            json_data["include_deleted"] = True

        raw_response = self.client.post(
            f"/v1/groups/{group_id}/user/{user_id}/histories", json=json_data,
        )

        if assert_response:
            self.assertEqual(raw_response.status_code, 200)

        return raw_response.json()

    def user_leaves_group(self, group_id: str, user_id: int = BaseTest.USER_ID) -> None:
        raw_response = self.client.delete(
            f"/v1/groups/{group_id}/user/{user_id}/join",
            json={
                "action_log": {
                    "payload": "some payload for action log"
                }
            }
        )
        self.assertEqual(raw_response.status_code, 200)

    def user_joins_group(self, group_id: str, user_id: int = BaseTest.USER_ID) -> None:
        raw_response = self.client.put(
            f"/v1/groups/{group_id}/join",
            json={
                "users": [user_id],
                "action_log": {
                    "payload": "some users joined the group",
                    "user_id": user_id
                }
            }
        )
        self.assertEqual(raw_response.status_code, 200)
        time.sleep(0.01)

    def create_action_log(
            self,
            user_id: int = BaseTest.USER_ID,
            receiver_id: int = BaseTest.OTHER_USER_ID
    ) -> dict:
        raw_response = self.client.post(
            f"/v1/users/{user_id}/actions",
            json={
                "payload": "some users joined the group",
                "receiver_id": receiver_id
            }
        )

        self.assertEqual(raw_response.status_code, 200)
        return raw_response.json()

    def update_hide_group_for(self, group_id: str, hide: bool, user_id: int = BaseTest.USER_ID):
        raw_response = self.client.put(
            f"/v1/groups/{group_id}/user/{user_id}/update", json={"hide": hide},
        )
        self.assertEqual(raw_response.status_code, 200)

    def update_kick_for_user(self, group_id: str, kicked: bool, user_id: int = BaseTest.USER_ID):
        raw_response = self.client.put(
            f"/v1/groups/{group_id}/user/{user_id}/update", json={"kicked": kicked},
        )
        self.assertEqual(raw_response.status_code, 200)

    def update_user_stats_to_now(self, group_id: str, user_id: int = BaseTest.USER_ID):
        now = arrow.utcnow().datetime
        now_ts = to_ts(now)

        raw_response = self.client.put(
            f"/v1/groups/{group_id}/user/{user_id}/update",
            json={"last_read_time": now_ts},
        )
        self.assertEqual(raw_response.status_code, 200)

        return float(now_ts)

    def get_global_user_stats(self, user_id: int = BaseTest.USER_ID, hidden: bool = None, count_unread: bool = None):
        json_data = {}
        if hidden is not None:
            json_data["hidden"] = hidden
        if count_unread is not None:
            json_data["count_unread"] = count_unread

        raw_response = self.client.post(
            f"/v1/userstats/{user_id}",
            json=json_data
        )
        self.assertEqual(raw_response.status_code, 200)

        return raw_response.json()

    def get_user_stats(self, group_id: str, user_id: int = BaseTest.USER_ID, status_code: int = 200):
        raw_response = self.client.get(f"/v1/groups/{group_id}/user/{user_id}")
        self.assertEqual(raw_response.status_code, status_code)

        return raw_response.json()

    def groups_updated_since(self, user_id: int, since: float):
        raw_response = self.client.post(
            f"/v1/users/{user_id}/groups/updates",
            json={"since": since, "count_unread": False, "per_page": 100},
        )
        self.assertEqual(raw_response.status_code, 200)

        return raw_response.json()

    def get_all_history(
            self,
            group_id: int
    ):
        raw_response = self.client.get(
            f"/v1/history/{group_id}"
        )
        self.assertEqual(raw_response.status_code, 200)

        return raw_response.json()

    def get_deleted_groups_for_user(
            self,
            user_id: int = BaseTest.USER_ID
    ):
        raw_response = self.client.get(
            f"/v1/deleted/{user_id}/groups"
        )
        self.assertEqual(raw_response.status_code, 200)

        return raw_response.json()

    def groups_for_user(
            self,
            user_id: int = BaseTest.USER_ID,
            count_unread: bool = False,
            only_unread: bool = False,
            hidden: bool = False,
            until: float = None,
            receiver_stats: bool = True
    ):
        json_data = {
            "per_page": "10",
            "count_unread": count_unread,
            "only_unread": only_unread,
            "hidden": hidden,
            "receiver_stats": receiver_stats
        }
        if until is not None:
            json_data["until"] = until

        raw_response = self.client.post(
            f"/v1/users/{user_id}/groups",
            json=json_data,
        )
        self.assertEqual(raw_response.status_code, 200)

        return raw_response.json()

    def mark_as_read(self, user_id: int = BaseTest.USER_ID):
        raw_response = self.client.put(
            f"/v1/user/{user_id}/read",
        )
        self.assertEqual(raw_response.status_code, 201)

        # async api
        time.sleep(0.01)

    def leave_all_groups(self, user_id: int = BaseTest.USER_ID):
        raw_response = self.client.delete(
            f"/v1/users/{user_id}/groups",
            json={
                "action_log": {
                    "payload": "some payload for action log"
                }
            }
        )
        self.assertEqual(raw_response.status_code, 201)

        # async api
        time.sleep(0.01)

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
        description: str = "",
        owner: int = None,
    ):
        if owner is None:
            owner = BaseTest.USER_ID

        raw_response = self.client.put(
            f"/v1/groups/{group_id}",
            json={"owner": owner, "group_name": name, "description": description},
        )
        self.assertEqual(raw_response.status_code, 200)

    def edit_message(
        self, user_id: int, group_id: str, message_id: str, created_at: float, new_payload: str
    ):
        raw_response = self.client.put(
            f"/v1/users/{user_id}/message/{message_id}/edit",
            json={
                "created_at": created_at,
                "message_payload": new_payload,
                "group_id": group_id,
            },
        )
        self.assertEqual(raw_response.status_code, 200)

    def highlight_group_for_user(self, group_id: str, user_id: int, highlight_time: float = None) -> None:
        if highlight_time is None:
            now_plus_2_days = arrow.utcnow().shift(days=2).datetime
            now_plus_2_days = to_ts(now_plus_2_days)
            highlight_time = now_plus_2_days

        raw_response = self.client.put(
            f"/v1/groups/{group_id}/user/{user_id}/update",
            json={"highlight_time": highlight_time},
        )
        self.assertEqual(raw_response.status_code, 200)

    def delete_highlight_group_for_user(self, group_id: str, user_id: int) -> None:
        now = arrow.utcnow().datetime

        raw_response = self.client.put(
            f"/v1/groups/{group_id}/user/{user_id}/update",
            json={"last_read_time": now},
        )
        self.assertEqual(raw_response.status_code, 200)

    def get_1v1_group_info(self, user_id: int = BaseTest.USER_ID, receiver_id: int = None):
        if receiver_id is None:
            receiver_id = BaseTest.OTHER_USER_ID

        raw_response = self.client.post(
            f"/v1/users/{user_id}/group", json={"receiver_id": receiver_id},
        )
        self.assertEqual(raw_response.status_code, 200)

        return raw_response.json()

    def get_group_info(self, group_id: str, count_messages: bool):
        raw_response = self.client.post(
            f"/v1/groups/{group_id}", json={"count_messages": count_messages},
        )
        self.assertEqual(raw_response.status_code, 200)

        return raw_response.json()

    def create_action_log_in_all_groups_for_user(
            self,
            user_id: int = BaseTest.USER_ID,
            delay: int = 20
    ):
        json_data = {
            "payload": "some action log payload"
        }

        raw_response = self.client.post(
            f"/v1/users/{user_id}/groups/actions",
            json=json_data,
        )

        # async api, returns 201 instead of 200
        self.assertEqual(raw_response.status_code, 201)

        if delay > 0:
            time.sleep(delay / 1000)

    def get_last_read_for_all_user(self, group_id: str) -> dict:
        raw_response = self.client.post(
            f"/v1/groups/{group_id}/lastread",
            json=dict(),  # empty body
        )
        self.assertEqual(raw_response.status_code, 200)

        return raw_response.json()

    def get_last_read_for_one_user(self, group_id: str, user_id: int) -> dict:
        json_data = {
            "user_id": user_id
        }

        raw_response = self.client.post(
            f"/v1/groups/{group_id}/lastread",
            json=json_data,
        )
        self.assertEqual(raw_response.status_code, 200)

        return raw_response.json()

    def send_1v1_message(
        self,
        message_type: int = MessageTypes.MESSAGE,
        user_id: int = BaseTest.USER_ID,
        receiver_id: int = BaseTest.OTHER_USER_ID,
        delay: int = 10,
        payload: str = "some payload"
    ) -> dict:
        json_data = {
            "receiver_id": receiver_id,
            "message_type": message_type,
            "message_payload": payload
        }

        raw_response = self.client.post(
            f"/v1/users/{user_id}/send",
            json=json_data,
        )
        self.assertEqual(raw_response.status_code, 200)

        if delay > 0:
            time.sleep(delay / 1000)

        return raw_response.json()

    def attachments_for(self, group_id: str, user_id: int = BaseTest.USER_ID):
        now = utcnow_ts()

        raw_response = self.client.post(
            f"/v1/groups/{group_id}/user/{user_id}/attachments",
            json={"per_page": 100, "until": now},
        )
        self.assertEqual(raw_response.status_code, 200)

        return raw_response.json()

    def update_attachment(
        self,
        message_id: str,
        created_at: float,
        user_id: int = BaseTest.USER_ID,
        receiver_id: int = BaseTest.OTHER_USER_ID,
        file_id: str = BaseTest.FILE_ID,
        payload: str = BaseTest.FILE_CONTEXT
    ):
        raw_response = self.client.post(
            f"/v1/users/{user_id}/message/{message_id}/attachment",
            json={
                "file_id": file_id,
                "message_payload": payload,
                "created_at": created_at,
                "receiver_id": receiver_id,
            },
        )
        self.assertEqual(raw_response.status_code, 200)

        return raw_response.json()

    def attachment_for_file_id(self, group_id: str, file_id: str, assert_response: bool = True):
        raw_response = self.client.post(
            f"/v1/groups/{group_id}/attachment",
            json={
                "file_id": file_id
            }
        )
        if assert_response:
            self.assertEqual(raw_response.status_code, 200)

        return raw_response.json()

    def delete_attachment(self, group_id: str, file_id: str = BaseTest.FILE_ID):
        raw_response = self.client.delete(
            f"/v1/groups/{group_id}/attachment",
            json={"file_id": file_id}
        )
        self.assertEqual(raw_response.status_code, 201)

        # async api
        time.sleep(0.01)

    def delete_attachments_in_group(self, group_id: str, user_id: str = BaseTest.USER_ID):
        raw_response = self.client.delete(
            f"/v1/groups/{group_id}/user/{user_id}/attachments",
            json={
                "action_log": {
                    "payload": "some deletion payload"
                }
            }
        )
        self.assertEqual(raw_response.status_code, 200)

    def delete_attachments_in_all_groups(self, user_id: str = BaseTest.USER_ID, send_action_log_query: bool = True):
        json_value = {}
        if send_action_log_query:
            json_value = {
                "action_log": {
                    "payload": "some deletion payload"
                }
            }

        raw_response = self.client.delete(
            f"/v1/user/{user_id}/attachments",
            json=json_value
        )
        self.assertEqual(raw_response.status_code, 201)

        # async api
        time.sleep(0.01)

    def assert_error(self, response, error_code):
        self.assertEqual(int(response["detail"].split(":")[0]), error_code)

    def assert_attachment_count(self, group_id: str, user_id: int, expected_amount: int):
        raw_response = self.client.post(
            f"/v1/groups/{group_id}/user/{user_id}/count", json={
                "only_attachments": True
            },
        )
        self.assertEqual(raw_response.status_code, 200)

        group = raw_response.json()
        self.assertEqual(expected_amount, group["message_count"])

    def assert_messages_in_group(
        self, group_id: str, user_id: int = BaseTest.USER_ID, amount: int = 0
    ):
        raw_response = self.client.post(
            f"/v1/groups/{group_id}/user/{user_id}/histories", json={"per_page": 100, "since": 0},
        )
        self.assertEqual(raw_response.status_code, 200)
        self.assertEqual(amount, len(raw_response.json()["messages"]))

    def assert_kicked_for_user(
        self, kicked: bool, group_id: str, user_id: int = BaseTest.USER_ID
    ) -> None:
        raw_response = self.client.get(f"/v1/groups/{group_id}/user/{user_id}",)
        self.assertEqual(raw_response.status_code, 200)
        self.assertEqual(kicked, raw_response.json()["stats"]["kicked"])

    def assert_hidden_for_user(
        self, hidden: bool, group_id: str, user_id: int = BaseTest.USER_ID
    ) -> None:
        raw_response = self.client.get(f"/v1/groups/{group_id}/user/{user_id}",)
        self.assertEqual(raw_response.status_code, 200)
        self.assertEqual(hidden, raw_response.json()["stats"]["hide"])

    def assert_bookmarked_for_user(self, bookmark: bool, group_id: str, user_id: int = BaseTest.USER_ID) -> None:
        raw_response = self.client.get(f"/v1/groups/{group_id}/user/{user_id}",)
        self.assertEqual(raw_response.status_code, 200)
        self.assertEqual(bookmark, raw_response.json()["stats"]["bookmark"])

    def assert_all_history(self, group_id: str, amount: int) -> None:
        response = self.get_all_history(group_id)
        self.assertEqual(amount, len(response["messages"]))

    def assert_groups_for_user(self, amount_of_groups, user_id: int = BaseTest.USER_ID, until: float = None) -> None:
        response = self.groups_for_user(user_id, until=until)
        self.assertEqual(amount_of_groups, len(response))

    def assert_deleted_groups_for_user(self, amount_of_groups, user_id: int = BaseTest.USER_ID) -> None:
        response = self.get_deleted_groups_for_user(user_id)
        self.assertEqual(amount_of_groups, len(response["stats"]))

    def assert_total_mqtt_sent_to(self, user_id: int, n_messages: int):
        total_sent = 0
        if user_id in self.env.client_publisher.sent_per_user:
            total_sent = len(self.env.client_publisher.sent_per_user[user_id])

        self.assertEqual(n_messages, total_sent)

    def assert_total_unread_count(self, user_id: int, unread_count: int):
        raw_response = self.client.post(f"/v1/userstats/{user_id}", json={})
        self.assertEqual(raw_response.status_code, 200)
        self.assertEqual(unread_count, raw_response.json()["unread_amount"])

    def assert_order_of_groups(self, user_id: int, *group_ids):
        groups = self.groups_for_user(user_id)
        for i, group_id in enumerate(group_ids):
            self.assertEqual(group_id, groups[i]["group"]["group_id"])

    def assert_mqtt_read_events(self, user_id: int, amount: int):
        if amount == 0:
            self.assertNotIn(user_id, self.env.client_publisher.sent_reads)
        else:
            self.assertEqual(amount, len(self.env.client_publisher.sent_reads[user_id]))

    def assert_payload(self, group_id: str, message_id: str, new_payload: str):
        histories = self.histories_for(group_id)

        message_payload = ""

        for message in histories["messages"]:
            if message["message_id"] == message_id:
                message_payload = message["message_payload"]
                break

        self.assertEqual(new_payload, message_payload)

    def assert_unread_amount_and_groups(self, user_id, unread_amount, unread_groups, session):
        n_unread_amount, n_unread_groups = self.env.rest.user.count_unread(
            user_id, session
        )
        self.assertEqual(n_unread_amount, unread_amount)
        self.assertEqual(n_unread_groups, unread_groups)

    def assert_cached_unread_for_group(self, user_id, group_id, amount):
        cache_unread = self.env.cache.get_unread_in_group(group_id, user_id)
        self.assertEqual(cache_unread, amount)
