import argparse
import datetime
import logging
import sys
import time

from connectors.mongodb import MongoDBConnector
from mongodb_pipelines.analytics import count_by_tags, count_by_title
from mongodb_pipelines.main import aggregate_offers, processed_offers
from scrapers import bulldogjob, justjoin, nofluffjobs, pracuj


def postpone_step_execution(seconds: int):
    for counter in range(seconds):
        sys.stdout.write(f"\rAction will be started in {seconds - counter} seconds")
        sys.stdout.flush()
        time.sleep(1)
    sys.stdout.write(f"\rAction will be started in 0 seconds\n")
    sys.stdout.flush()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="PROG")
    subparsers = parser.add_subparsers(dest="command")

    subparsers.add_parser("scraping")
    subparsers.add_parser("aggregate")
    subparsers.add_parser("process")
    subparsers.add_parser("analysis")
    subparsers.add_parser("all")

    cli_args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    logging.info("Starting...")

    # Pre-conditions
    connector = MongoDBConnector()
    db_name = "jobOfferDB"
    ingested_at = datetime.datetime.now()

    # Web scrapper's runs
    if cli_args.command == "scraping" or cli_args.command == "all":
        logging.info("Executing scrapers...")
        postpone_step_execution(10)

        scrapper_kwargs = dict(
            connector=connector, db_name=db_name, ingested_at=ingested_at
        )
        bulldogjob.run(**scrapper_kwargs)
        nofluffjobs.run(**scrapper_kwargs)
        justjoin.run(**scrapper_kwargs)
        pracuj.run(**scrapper_kwargs)

    # Pipeline's runs
    if cli_args.command == "aggregate" or cli_args.command == "all":
        logging.info("Aggregating scrapped data...")
        postpone_step_execution(10)
        aggregation_kwargs = dict(connector=connector, db_name=db_name)
        aggregate_offers.run(**aggregation_kwargs)

    if cli_args.command == "process" or cli_args.command == "all":
        logging.info("Processing aggregated data...")
        postpone_step_execution(10)
        processing_kwargs = dict(connector=connector, db_name=db_name)
        processed_offers.run(**processing_kwargs)

    # Analytics runs
    if cli_args.command == "analysis" or cli_args.command == "all":
        logging.info("Collecting analytic metrics...")
        postpone_step_execution(5)

        analytics_kwargs = dict(connector=connector, db_name=db_name)
        count_by_title.run(**analytics_kwargs)
        count_by_tags.run(**analytics_kwargs)

    logging.info("Stopping...")
