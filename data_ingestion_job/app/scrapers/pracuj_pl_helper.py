import concurrent.futures
import json
import logging
import re

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter, Retry

logging.basicConfig(level=logging.INFO)


def scrape_from_html():
    def get_offer_details_from_html(offer_html_content):
        soup = BeautifulSoup(
            offer_html_content, "html.parser", from_encoding="utf-8"
        ).find(
            string=re.compile("kansas-offerview")
        )  # TODO popraw na window['kansas-offerview']
        tag = soup.find_parent("script")
        text = tag.text.replace("undefined", "null").replace(
            "window['kansas-offerview'] =", ""
        )

        try:
            details = json.loads(text)
        except:
            logging.info(text)
            raise

        return details["offerReducer"]["offer"]

    def get_optional_skills(details_json):
        optional_skills = []
        for section in details_json["sections"]:
            if section["sectionType"] == "technologies":
                for subsections in section["subSections"]:
                    if subsections["sectionType"] == "technologies-optional":
                        model = subsections["model"]
                        raw_optional_skills = model.get("customItems", []) + model.get(
                            "items", []
                        )
                        optional_skills = [
                            technology["name"] for technology in raw_optional_skills
                        ]
        return optional_skills

    offer_page_paginator = "https://massachusetts.pracuj.pl/api/offers?jobBoardVersion=2&rop=50&pn="  # LIMIT Maxymalny
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36"
    }

    # TODO weź walnij dwie pętle tymczasowo bez asynchroniczności, potem można ją dodać
    index = 1
    offers = {}

    common_offers_count = requests.get(
        f"{offer_page_paginator}1", headers=headers
    ).json()["commonOffersCount"]
    print("common_offers_count = ", common_offers_count)
    pages = (common_offers_count // 50) + 1

    urls = []
    for index in range(1, pages + 1):
        urls.append(f"{offer_page_paginator}{index}")

    MAX_THREADS = 10
    threads = min(MAX_THREADS, len(urls))

    def get_offers(url):
        return requests.get(url, headers=headers).json()["offers"]

    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        scrapped_pages = executor.map(get_offers, urls)

    group_id_key = "groupId"
    offer_id_key = "offerId"
    location_key = "location"
    for page in scrapped_pages:
        for item in page:
            group_id = item[group_id_key]
            del item[offer_id_key]
            if group_id in offers:
                offers[group_id][location_key].append(item[location_key])
            else:
                offers[group_id] = item
                offers[group_id][location_key] = [offers[group_id][location_key]]

    # Flatten payload
    payload = list(offers.values())

    # Extends skills
    logging.info("Extending skills...")
    s = requests.Session()
    retries = Retry(total=10, backoff_factor=120, status_forcelist=[429])
    s.mount("https://", HTTPAdapter(max_retries=retries))

    url_key = "offerUrl"
    optional_tech_key = "technologiesOptional"

    for index, item in enumerate(payload):
        logging.info(f"{index} - {item[url_key]}")
        r = s.get(item[url_key])
        r.raise_for_status()
        html_offer = r.content
        offer_details = get_offer_details_from_html(html_offer)
        item[optional_tech_key] = get_optional_skills(offer_details)

    return payload
