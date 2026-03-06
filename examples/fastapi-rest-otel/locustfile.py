import random

from locust import HttpUser, between, task


class FastAPITelemetryUser(HttpUser):
    wait_time = between(0.1, 0.7)

    def on_start(self):
        self.item_ids = []

    @task(4)
    def health(self):
        self.client.get("/health", name="GET /health")

    @task(3)
    def create_item(self):
        item_name = f"load-item-{random.randint(1, 1_000_000)}"
        response = self.client.post(
            f"/items?name={item_name}",
            name="POST /items",
        )
        if response.status_code == 200:
            payload = response.json()
            item_id = payload.get("id")
            if isinstance(item_id, int):
                self.item_ids.append(item_id)

    @task(2)
    def list_items(self):
        self.client.get("/items", name="GET /items")

    @task(1)
    def get_or_delete_item(self):
        if not self.item_ids:
            self.client.get("/items/999999", name="GET /items/{item_id} 404")
            return

        item_id = random.choice(self.item_ids)
        if random.random() < 0.7:
            self.client.get(f"/items/{item_id}", name="GET /items/{item_id}")
        else:
            response = self.client.delete(
                f"/items/{item_id}",
                name="DELETE /items/{item_id}",
            )
            if response.status_code == 200 and item_id in self.item_ids:
                self.item_ids.remove(item_id)
