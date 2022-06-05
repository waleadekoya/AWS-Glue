from pathlib import Path
from pprint import pprint
import time
import timeit
import logging

import awswrangler as wr
import boto3
import botocore.exceptions
import pendulum
from airflow.decorators import dag, task

etl_script = Path(Path(__file__).parent, "glue_job_etl_script.py")
s3_script_location = f"s3://aws-glue-data-source-az/scripts/{etl_script.name}"
log = logging.getLogger(__name__)

glue = boto3.client(
    service_name="glue",
    region_name="eu-west-1",
    endpoint_url="https://glue.eu-west-1.amazonaws.com"
)

pprint(glue.get_crawler(Name="customer_csv_crawler")['Crawler']['LastCrawl']['Status'])
# status = 'SUCCEEDED' | 'CANCELLED' | 'FAILED'


def __start_crawler(crawler_name: str, *, timeout_minutes: int = 120, retry_seconds: int = 5):
    timeout_seconds = timeout_minutes * 60
    start_time = timeit.default_timer()
    abort_time = start_time + timeout_seconds

    def wait_until_ready() -> None:
        state_previous = None
        while True:
            response_get = glue.get_crawler(Name=crawler_name)
            state = response_get["Crawler"]["State"]
            if state != state_previous:
                log.info(f"Crawler {crawler_name} is {state.lower()}.")
                state_previous = state
            if state == "READY":  # Other known states: RUNNING, STOPPING
                return
            if timeit.default_timer() > abort_time:
                raise TimeoutError(
                    f"Failed to crawl {crawler_name}. The allocated time of {timeout_minutes:,} minutes has elapsed.")
            time.sleep(retry_seconds)

    if glue.get_crawler(Name=crawler_name):
        wait_until_ready()
        response = glue.start_crawler(Name=crawler_name)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        log.info(f"Crawling {crawler_name}.")
        wait_until_ready()
        log.info(f"Crawled {crawler_name}.")
        pprint(response)


def start_crawler():
    crawlers = ["stocks_database_crawler", "customer_csv_crawler"]
    for crawler in crawlers:
        __start_crawler(crawler)


start_crawler()
