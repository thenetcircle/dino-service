import time
import random

from locust import FastHttpUser, task, between

n_users = 20
user_ids = list(set([
    random.randint(80000000, 90000000)
    for _ in range(n_users)
]))


class QuickstartUser(FastHttpUser):
    wait_time = between(0, 1)
    sender_id, receiver_id = random.sample(user_ids, 2)

    @task(10)
    def count_users_online(self):
        self.client.get("/v1/online")

    @task(3)
    def online_status_for_1971(self):
        for _ in range(10):
            self.client.get("/v1/online/1971")
            time.sleep(0.01)

    @task(1)
    def send_message(self):
       self.sender_id, self.receiver_id = random.sample(user_ids, 2)

       for _ in range(1):
           self.client.post(
               f"/v1/users/{self.sender_id}/send",
               json={
                   "receiver_id": self.receiver_id,
                   "message_payload": "load test",
                   "message_type": 0
               }
           )
           time.sleep(0.01)

    @task(1)
    def check_group_info(self):
        response = self.client.post(
            f"/v1/users/{self.sender_id}/groups",
            json={
                "per_page": 5,
                "only_unread": False,
            },
        )

        for group in response.json():
            group_id = group["group"]["group_id"]
            self.client.post(
                f"/v1/groups/{group_id}",
                json={
                    "count_messages": True
                }
            )
            time.sleep(0.01)
            self.client.post(
                f"/v1/groups/{group_id}/user/{self.sender_id}/histories",
                json={
                    "per_page": 10,
                    "since": 0
                }
            )
