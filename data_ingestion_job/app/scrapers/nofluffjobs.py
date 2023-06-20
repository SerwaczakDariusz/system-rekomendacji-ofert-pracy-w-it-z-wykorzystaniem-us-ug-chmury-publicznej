import asyncio
import json
import logging

import aiohttp
import requests
from scrapers.helpers import base_logging


@base_logging
def run(connector, db_name, ingested_at):
    collection_name = "nofluffjobs"

    connector.set_database(db_name)
    connector.set_collection(collection_name)

    # Scrapping
    postings_url = "https://nofluffjobs.com/api/posting"
    regions = ["pl", "hu", "cz", "ua", "sk", "nl"]
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/39.0.2171.95 Safari/537.36"
    }

    raw_offers = {}
    session = requests.Session()
    for region in regions:
        params = {"region": region}
        result = session.get(postings_url, params=params, headers=headers)
        logging.info(f"Result received from the {result.url}")

        parsed_result = result.json()
        raw_offers[region] = parsed_result["postings"]
        logging.info(f"Fetched offers count: {len(raw_offers[region])}")
        logging.info(f"Declared offers count: {parsed_result['totalCount']}")

    offer_ids = set()
    for region in regions:
        for offer in raw_offers[region]:
            offer_ids.add(offer["id"])

    # Fetching unique offers
    logging.info(f"Unique offers count across all regions: {len(offer_ids)}")
    loop = asyncio.get_event_loop()

    unique_offer_urls = [f"{postings_url}/{offer_id}" for offer_id in offer_ids]
    raw_payload = loop.run_until_complete(
        fetch_detailed_offers(unique_offer_urls, headers)
    )

    # Defining payload structure
    payload = []
    other_key = "others"
    url_key = "url"
    skills_key = "skills"
    basics_key = "basics"
    keys_to_extract = [("_id", "id"), "title", (skills_key, "requirements"), basics_key]
    keys_to_delete = [
        "details",
        "analytics",
        "seo",
        "meta",
        "metadata",
        "benefits",
        "consents",
        "company",
        "apply",
        "specs",
        "recruitment",
    ]

    for item in raw_payload:
        payload_item = dict()

        # Creating URL
        payload_item[
            url_key
        ] = f"https://nofluffjobs.com/{item['regions'][0]}/job/{item['postingUrl']}"

        # Moving keys without content modifications
        for key in keys_to_extract:
            if isinstance(key, tuple):
                payload_item[key[0]] = item[key[1]]
                del item[key[1]]
            else:
                payload_item[key] = item[key]
                del item[key]

        payload_item[other_key] = item

        # Deleting keys
        for key in keys_to_delete:
            if key in item:
                del item[key]

        payload.append(payload_item)

    # Parsing seniority level and marking non-IT offers to remove
    seniority_key = "seniority"
    new_seniority_key = "experience_level"
    category_key = "category"
    non_it_categories = {"hr"}
    offers_to_remove = set()
    for item in payload:
        item[new_seniority_key] = item[basics_key][seniority_key]
        if item[basics_key][category_key] in non_it_categories:
            offers_to_remove.add(item["_id"])
        del item[basics_key]

    cleaned_payload = []
    for item in payload:
        if item["_id"] not in offers_to_remove:
            cleaned_payload.append(item)
    payload = cleaned_payload

    # Skills parsing and flattening
    description_key = "description"
    language_key = "languages"
    level_key = "level"
    for item in payload:
        if description_key in item[skills_key]:
            del item[skills_key][description_key]

        if language_key in item[skills_key]:
            item[other_key][language_key] = item[skills_key][language_key]
            del item[skills_key][language_key]

        # Flattening the skills
        for skill_level in item[skills_key]:
            for skill in item[skills_key][skill_level]:
                skill[level_key] = skill_level

        flattened_skills = []
        skill_levels = list(item[skills_key].keys())
        for skill_level in skill_levels:
            flattened_skills.extend(item[skills_key][skill_level])
            del item[skills_key][skill_level]
        item[skills_key] = flattened_skills

    # Renaming key in skills' object
    skill_name_key = "name"
    value_key = "value"
    for item in payload:
        for skill in item[skills_key]:
            skill[skill_name_key] = skill[value_key]
            del skill[value_key]

    # Adding ingestion time
    for item in payload:
        item["ingested_at"] = ingested_at

    # Writing
    logging.info("Writing data using the connector...")
    connector.write_batch(payload)
    logging.info(f"Saved records: {len(payload)}")


async def fetch_detailed_offers(offer_urls: list, headers: dict):
    """Fetch offers' details."""

    async def __fetch_single_offer(item_url):
        """Helper function to make a single fetch from the API."""
        async with session.get(item_url, headers=headers) as response:
            detailed_offers.append(await response.json())

    detailed_offers = []
    params = [{"item_url": url} for url in offer_urls]
    async with aiohttp.ClientSession() as session:
        await asyncio.gather(
            *[__fetch_single_offer(**kwargs) for kwargs in params],
            return_exceptions=True,
        )
    return detailed_offers
