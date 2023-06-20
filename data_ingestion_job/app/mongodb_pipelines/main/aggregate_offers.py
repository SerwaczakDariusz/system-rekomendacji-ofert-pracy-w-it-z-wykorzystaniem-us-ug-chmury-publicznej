import logging

from scrapers.helpers import base_logging


@base_logging
def run(connector, db_name):
    connector.set_database(db_name)
    all_collections = connector.get_collection_names(db_name)

    for collection in all_collections:
        logging.info(f"Aggregating data from {collection} into a single collection...")
        connector.set_database(db_name)
        connector.set_collection(collection)

        base_aggregations = [
            {
                "$project": {
                    "_id": 1,
                    "title": 1,
                    "experience_level": 1,
                    "skills": 1,
                    "url": 1,
                    "ingested_at": 1,
                }
            },
            {"$addFields": {"source": collection}},
        ]
        payload = connector.aggregate(base_aggregations)

        # Writing
        connector.set_database(db_name)
        connector.set_collection("aggregated")
        logging.info("Writing data using the connector...")
        connector.write_batch(payload)
