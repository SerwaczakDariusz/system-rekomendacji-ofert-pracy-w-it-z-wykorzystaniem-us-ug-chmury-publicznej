import logging

from scrapers.helpers import base_logging


@base_logging
def run(connector, db_name):
    connector.set_database(db_name)
    all_collections = connector.get_collection_names(db_name)

    for collection in all_collections:
        logging.info(f"Aggregating {collection}...")
        connector.set_database(db_name)
        connector.set_collection(collection)

        report = connector.aggregate(
            [
                {"$unwind": {"path": "$skills"}},
                {"$group": {"_id": "$skills.name", "sum": {"$sum": 1}}},
                {"$sort": {"sum": -1}},
            ]
        )

        # Writing
        connector.set_database("analyticsDB")
        connector.set_collection(f"count_by_tags_{collection}")
        logging.info("Writing data using the connector...")
        connector.write_batch(report)
