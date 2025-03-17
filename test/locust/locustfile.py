import time
import random

from locust import HttpUser, task, between

n_users = 20
user_ids = list(set([
    random.randint(80000000, 90000000)
    for _ in range(n_users)
]))


class QuickstartUser(HttpUser):
    wait_time = between(0, 1)

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
       sender, receiver = random.sample(user_ids, 2)

       for _ in range(1):
           self.client.post(
               f"/v1/users/{sender}/send",
               json={
                   "receiver_id": receiver,
                   "message_payload": "load test",
                   "message_type": 0
               }
           )
           time.sleep(0.01)
