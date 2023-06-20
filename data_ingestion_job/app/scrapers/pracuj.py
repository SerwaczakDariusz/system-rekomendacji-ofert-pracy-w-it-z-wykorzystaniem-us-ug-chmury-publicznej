import datetime
import json
import logging

from scrapers.helpers import base_logging
from scrapers.pracuj_pl_helper import scrape_from_html


@base_logging
def run(connector, db_name, ingested_at):
    collection_name = "pracuj"

    connector.set_database(db_name)
    connector.set_collection(collection_name)

    payload = scrape_from_html()

    expected_technologies_key = "technologiesExpected"
    optional_technologies_key = "technologiesOptional"
    group_id_key = "groupId"
    for item in payload:
        item["_id"] = item[group_id_key]
        del item[group_id_key]

        keys_to_rename = [
            ["title", "jobTitle"],
            ["url", "offerUrl"],
            ["experience_level", "employmentLevel"],
        ]
        for new_name, key in keys_to_rename:
            item[new_name] = item[key]
            del item[key]

        skills = [
            {"name": skill, "level": "expected"}
            for skill in item[expected_technologies_key]
        ] + [
            {"name": skill, "level": "optional"}
            for skill in item[optional_technologies_key]
        ]
        item["skills"] = skills
        del item[expected_technologies_key]
        del item[optional_technologies_key]

        item["ingested_at"] = ingested_at

    # Writing
    logging.info("Writing data using the connector...")
    connector.write_batch(payload)
    logging.info(f"Saved records: {len(payload)}")
