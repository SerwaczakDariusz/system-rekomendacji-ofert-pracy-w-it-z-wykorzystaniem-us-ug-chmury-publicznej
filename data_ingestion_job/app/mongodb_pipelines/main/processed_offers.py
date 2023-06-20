import logging

from scrapers.helpers import base_logging


@base_logging
def run(connector, db_name):
    source_collection = "aggregated"

    logging.info(f"Processing and normalizing data from {source_collection}...")
    connector.set_database(db_name)
    connector.set_collection(source_collection)

    base_aggregations = [
        {"$match": {"skills": {"$exists": True, "$not": {"$size": 0}}}},
        {
            "$addFields": {
                "normalized_skills": {
                    "$map": {
                        "input": "$skills",
                        "in": {
                            "name": "$$this.name",
                            "level": {
                                "$switch": {
                                    "branches": [
                                        # pracuj.pl
                                        {
                                            "case": {
                                                "$eq": ["$$this.level", "expected"]
                                            },
                                            "then": 3,
                                        },
                                        {
                                            "case": {
                                                "$eq": ["$$this.level", "optional"]
                                            },
                                            "then": 1,
                                        },
                                        # bulldogjob.pl
                                        {
                                            "case": {
                                                "$eq": ["$$this.level", "excellent"]
                                            },
                                            "then": 5,
                                        },
                                        {
                                            "case": {
                                                "$eq": ["$$this.level", "very_well"]
                                            },
                                            "then": 4,
                                        },
                                        {
                                            "case": {
                                                "$eq": ["$$this.level", "unspecified"]
                                            },
                                            "then": 3,
                                        },
                                        {
                                            "case": {
                                                "$eq": ["$$this.level", "beginner"]
                                            },
                                            "then": 2,
                                        },
                                        # nofluffjobs.pl
                                        {
                                            "case": {
                                                "$and": [
                                                    {"$eq": ["$$this.type", "main"]},
                                                    {"$eq": ["$$this.level", "musts"]},
                                                ]
                                            },
                                            "then": 4,
                                        },
                                        {
                                            "case": {
                                                "$and": [
                                                    {"$eq": ["$$this.type", "other"]},
                                                    {"$eq": ["$$this.level", "musts"]},
                                                ]
                                            },
                                            "then": 3,
                                        },
                                        {
                                            "case": {
                                                "$and": [
                                                    {"$eq": ["$$this.type", "main"]},
                                                    {"$eq": ["$$this.level", "nices"]},
                                                ]
                                            },
                                            "then": 2,
                                        },
                                        {
                                            "case": {
                                                "$and": [
                                                    {"$eq": ["$$this.type", "other"]},
                                                    {"$eq": ["$$this.level", "nices"]},
                                                ]
                                            },
                                            "then": 1,
                                        },
                                    ],
                                    "default": "$$this.level",
                                }
                            },
                        },
                    }
                }
            }
        },
    ]

    payload = connector.aggregate(base_aggregations)

    # Writing
    connector.set_database(db_name)
    connector.set_collection("processed")
    logging.info("Writing data using the connector...")
    connector.write_batch(payload)
