import logging
import time
from functools import wraps


def base_logging(func):
    @wraps(func)
    def wrapped(*args, **kwargs):
        job_name = func.__module__.split(".")[-1]

        start = time.time()
        logging.info(f"Starting the {job_name} job...")

        func(*args, **kwargs)

        end = time.time()
        logging.info(f"Stopping the {job_name} job after {end - start} seconds...")

    return wrapped
