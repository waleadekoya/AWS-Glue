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


@task()
def upload_script_to_s3():
    # Upload Python Script to S3
    wr.s3.upload(str(etl_script), path=s3_script_location)


# https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-python-calling.html


# 1. Create an instance of the AWS Glue client
glue = boto3.client(
    service_name="glue",
    region_name="eu-west-1",
    endpoint_url="https://glue.eu-west-1.amazonaws.com"
)


def __create_database(database: str, location_uri: str, description: str = ""):
    # delete database if exists
    # glue.delete_database(Name=database)
    try:
        # Create Database, if not existing,
        glue.get_database(Name=database)['Database']['Name']
    except botocore.exceptions.ClientError as error:
        pprint(error)
        response = glue.create_database(
            DatabaseInput={
                "Name": database,
                "Description": description,
                "LocationUri": location_uri,
            }
        )
        pprint(response)


@task()
def create_databases():
    __create_database("stocks_database",
                      "s3://aws-glue-data-source-az/data/aws_wrangler_database/",
                      "sample database created via Python API call"
                      )
    __create_database("customers_database",
                      "s3://aws-glue-data-source-az/data/customers_database/customers_csv/",
                      "customers database metadata"
                      )


def __create_crawler(crawler_name: str, database: str, s3_targets: str, description: str = "", ):
    try:
        glue.get_crawler(Name=crawler_name)['Crawler']['Name']
    except botocore.exceptions.ClientError as error:
        pprint(error)
        response = glue.create_crawler(
            Name=crawler_name,
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue.html#Glue.Client.create_crawler
            Role="arn:aws:iam::578120814996:role/service-role/AWSGlueServiceRole-Developer",
            DatabaseName=database,
            Description=description,
            Targets={
                "S3Targets": [
                    {
                        "Path": s3_targets,
                    },
                ],
            }
        )
        pprint(response)


@task()
def create_crawler():
    # delete_response = glue.delete_crawler(Name="stocks_database_crawler")
    # pprint(delete_response)
    __create_crawler(crawler_name="stocks_database_crawler",
                     database="stocks_database",
                     s3_targets="s3://aws-glue-data-source-az/data/aws_wrangler_database",
                     description="a crawler to create metadata for stocks_database tables",
                     )
    __create_crawler(crawler_name="customer_csv_crawler",
                     database="customers_database",
                     s3_targets="s3://aws-glue-data-source-az/data/customers_database/customers_csv",
                     description="crawler to get metadata for customer csv data table",
                     )


def __start_crawler(crawler_name: str, *, timeout_minutes: int = 120, retry_seconds: int = 5):
    timeout_seconds = timeout_minutes * 60
    start_time = timeit.default_timer()
    abort_time = start_time + timeout_seconds

    def wait_until_ready() -> None:
        response_get = None
        state_previous = None
        while True:
            try:
                response_get = glue.get_crawler(Name=crawler_name)
            except botocore.exceptions.ClientError as error:
                log.error(error)
                time.sleep(retry_seconds)
            if response_get:
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


@task()
def start_crawler():
    crawlers = ["stocks_database_crawler", "customer_csv_crawler"]
    for crawler in crawlers:
        __start_crawler(crawler)
    return "completed"


@task(multiple_outputs=True)
def create_job(crawler_status: str):
    # 2. Create a job. You must use glueetl as the name for the ETL command
    # Delete existing job with the same job name
    pprint(
        glue.delete_job(
            JobName="glue-api-job"
        )
    )
    if crawler_status:
        my_job = glue.create_job(
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue.html#Glue.Client.create_job
            Name="glue-api-job",
            Description="ETL job using translate API to convert non-English texts to English",
            Role="AWSGlueServiceRole-Developer",
            Command={
                "Name": "glueetl",
                "ScriptLocation": s3_script_location,
                "PythonVersion": "3"
            },
            DefaultArguments={
                # You can specify arguments here that:
                # 1. the job-execution script consumes, as well as arguments that Glue itself consumes.
                # "--extra-py-files": "s3://bucket/prefix/lib_A.zip,s3://bucket_B/prefix/lib_X.zip",
                "--additional-python-modules": "deep-translator"

            },
            NumberOfWorkers=10,
            WorkerType="G.1X",
            GlueVersion="3.0"

        )
        pprint(my_job)
        return my_job


@task()
def start_job(my_job):
    # 3. Start a new run of the job that you created in the previous step
    my_new_job_run = glue.start_job_run(JobName=my_job['Name'])

    # 4 Get Job Status
    status = glue.get_job_run(JobName=my_job['Name'], RunId=my_new_job_run['JobRunId'])

    # Print current state of the job
    pprint(status['JobRun']['JobRunState'])


@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=['example-wale'],
)
def glue_taskflow_api_etl():
    upload_script_to_s3()
    create_databases()
    create_crawler()
    status = start_crawler()
    job_name = create_job(status)
    start_job(job_name)


glue_taskflow_api_dag = glue_taskflow_api_etl()
