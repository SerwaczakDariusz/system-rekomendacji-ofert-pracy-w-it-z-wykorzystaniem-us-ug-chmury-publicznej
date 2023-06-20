from typing import Any, Dict, List

import pymongo
from connectors import helpers


class MongoDBConnector:
    """MongoDB connector for scrappers."""

    def __init__(self):
        db_config = helpers.load_config("./configs/mongodb.yaml")["DATABASE"]
        conn_str: str = db_config["CONNECTION_STRING"]
        parameters: str = db_config["ENCODED_PARAMETERS"]

        self.client = pymongo.MongoClient(
            f"{conn_str}/?{parameters}", server_api=pymongo.server_api.ServerApi("1")
        )
        self.current_database = None
        self.current_collection = None

    def get_database_names(self) -> List[str]:
        return self.client.list_database_names()

    def get_collection_names(self, database_name: str) -> List[str]:
        db = self.client.get_database(database_name)
        return db.list_collection_names()

    def set_database(self, db_name: str) -> None:
        self.current_database = db_name
        self.current_collection = None

    def set_collection(self, db_collection: str) -> None:
        self.current_collection = db_collection

    def write_batch(self, item_batch: List[Dict[str, Any]]) -> None:
        db = self.client[self.current_database]
        collection = db[self.current_collection]

        try:
            collection.insert_many(item_batch, ordered=False)
        except pymongo.errors.BulkWriteError as e:
            write_errors = e.details.get("writeErrors")

            duplication_error_code = 11000
            existing_ids = [
                item["keyValue"]["_id"]
                for item in write_errors
                if item["code"] == duplication_error_code
            ]

            if len(existing_ids) != len(write_errors):
                raise

    def read_all(self) -> pymongo.cursor.Cursor:
        db = self.client.get_database(self.current_database)
        collection = db.get_collection(self.current_collection)
        return collection.find()

    def aggregate(self, steps: dict):
        db = self.client.get_database(self.current_database)
        collection = db.get_collection(self.current_collection)

        return collection.aggregate(steps)
