import requests


class RalfClient:
    def __init__(self) -> None:
        self.base_url = "http://localhost:8000/table"

    def point_query(self, *, table_name, key):
        url = f"{self.base_url}/{table_name}/{key}"
        print(f"querying {url}...")
        return requests.get(url).json()

    def retract(self, *, table_name, key):
        url = f"{self.base_url}/retract/{table_name}/{key}"
        print(f"retracting {url}...")
        return requests.get(url).json()


    def bulk_query(self, *, table_name):
        url = f"{self.base_url}/{table_name}"
        print(f"querying {url}...")
        return requests.get(url).json()


if __name__ == "__main__":
    client = RalfClient()
    print(client.point_query(table_name="not exist", key=1))
    print(client.point_query(table_name="window", key=1), "...")
    print(client.point_query(table_name="joined", key=1))
    print(client.bulk_query(table_name="joined")[:10], "...")
