"""
from locust import HttpUser, task, between


class AdUser(HttpUser):
    wait_time = between(0.0, 0.05)

    @task
    def serve(self):
        self.client.post(
            "/v1/ads/serve",
            json={"query": "running shoes", "geo": "IN", "cohort_id": "cohort_runners_in"},
        )
