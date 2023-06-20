import asyncio
import logging
from typing import List

import aiohttp
import requests
from scrapers.helpers import base_logging


@base_logging
def run(connector, db_name, ingested_at):
    collection_name = "justjoin"

    connector.set_database(db_name)
    connector.set_collection(collection_name)

    # Scrapping
    main_domain = "https://justjoin.it"
    slug_url = f"{main_domain}/api/offers"
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/39.0.2171.95 Safari/537.36"
    }

    response = requests.get(slug_url, headers=headers)
    json_content = response.json()

    # Defining payload structure
    payload = []
    other_key = "others"
    url_key = "url"
    keys_to_extract = [("_id", "id"), "title", "experience_level", "skills"]

    # Moving keys without content modifications
    for item in json_content:
        payload_item = {}
        for key in keys_to_extract:
            if isinstance(key, tuple):
                payload_item[key[0]] = item[key[1]]
                del item[key[1]]
            else:
                payload_item[key] = item[key]
                del item[key]

        payload_item[other_key] = item
        payload.append(payload_item)

    # Creating the main URL based on offer slugs
    multilocation_key = "multilocation"
    slug_key = "slug"
    for item in payload:
        locations = item[other_key][multilocation_key]
        for location in locations:
            if location[slug_key] == item["_id"]:
                main_location = location
                break
        item[url_key] = f"{main_domain}/offers/{main_location[slug_key]}"
        del item[other_key][multilocation_key]

    # Extending the skill list and validating the process
    logging.info("Extending the skills...")
    loop = asyncio.get_event_loop()
    loop.run_until_complete(extend_skills(payload, headers))

    # Adding ingestion time
    for item in payload:
        item["ingested_at"] = ingested_at

    # Writing
    logging.info("Writing data using the connector...")
    connector.write_batch(payload)
    logging.info(f"Scrapped records: {len(payload)}")


async def extend_skills(payload: List[dict], headers: dict):
    """Extend required skills stored under the 'skills' key."""

    async def __extend_skills_for_single_item(item_url, item_to_update):
        """Helper function to make a single fetch from the API."""
        skill_key = "skills"
        async with session.get(item_url, headers=headers) as response:
            item_to_update[skill_key] = (await response.json())[skill_key]

    url_key = "url"
    params = []
    for item in payload:
        url = item[url_key].replace("offers", "api/offers")
        params.append(dict(item_url=url, item_to_update=item))

    async with aiohttp.ClientSession() as session:
        await asyncio.gather(
            *[__extend_skills_for_single_item(**kwargs) for kwargs in params],
            return_exceptions=True,
        )
