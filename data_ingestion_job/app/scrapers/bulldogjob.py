import logging

import requests
from scrapers.helpers import base_logging


@base_logging
def run(connector, db_name, ingested_at):
    collection_name = "bulldogjob"

    connector.set_database(db_name)
    connector.set_collection(collection_name)

    # Offers counting
    graphql_endpoint = "https://bulldogjob.pl/graphql"
    count_query = {
        "operationName": "searchJobs",
        "query": """
            query searchJobs($exclude: [ID!]) {
                searchJobs(exclude: $exclude) {
                    totalCount
                } 
            }
        """,
    }

    r = requests.post(graphql_endpoint, json=count_query)
    offers_count = r.json()["data"]["searchJobs"]["totalCount"]

    # Scrapping
    offers_query = {
        "operationName": "searchJobs",
        "variables": {"page": 1, "perPage": offers_count},
        "query": """
        query searchJobs(
                $page: Int,
                $perPage: Int,
                $exclude: [ID!]
            ) {
            searchJobs(page: $page perPage: $perPage exclude: $exclude) {
                totalCount
                nodes {
                    _id: id
                    title: position
                    experience_level: experienceLevel
                    skills: technologies {
                        name
                        level
                    }
                }
            }
        }
    """,
    }

    r = requests.post(graphql_endpoint, json=offers_query)
    payload = r.json()["data"]["searchJobs"]["nodes"]

    # Adding offer URL
    url_key = "url"
    for item in payload:
        item[url_key] = f"https://bulldogjob.pl/companies/jobs/{item['_id']}"

    # Adding ingestion time
    for item in payload:
        item["ingested_at"] = ingested_at

    # Writing
    logging.info("Writing data using the connector...")
    connector.write_batch(payload)
    logging.info(f"Scrapped records: {len(payload)}")
