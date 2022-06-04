from pprint import pprint
import json

import pendulum
import boto3
import awswrangler as wr
from pathlib import Path

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator

etl_script = Path(Path(__file__).parent, "glue_job_etl_script.py")
s3_script_location = f"s3://aws-glue-data-source-az/scripts/{etl_script.name}"


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


@task()
def create_database():
    # Create Database, if not existing
    # Delete database if exists
    glue.delete_database(Name="stocks_database")
    response = glue.create_database(
        DatabaseInput={
            "Name": "stocks_database",
            "Description": "sample database created via Python API call",
            "LocationUri": "s3://aws-glue-data-source-az/data/aws_wrangler_database/",
        }
    )
    pprint(response)


@task()
def create_crawler():
    # Create Crawler
    delete_response = glue.delete_crawler(Name="stocks_database_crawler")
    pprint(delete_response)
    response = glue.create_crawler(
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue.html#Glue.Client.create_crawler
        Name="stocks_database_crawler",
        Role="arn:aws:iam::578120814996:role/service-role/AWSGlueServiceRole-Developer",
        DatabaseName="stocks_database",
        Description="a crawler to create metadata for stocks_database tables",
        Targets={
            "S3Targets": [
                {
                    "Path": "s3://aws-glue-data-source-az/data/aws_wrangler_database",
                },
            ],
        }
    )
    pprint(response)


@task()
def start_crawler():
    # Start Crawler
    if glue.get_crawler(Name="stocks_database_crawler"):
        response = glue.start_crawler(Name="stocks_database_crawler")
        pprint(response)


@task(multiple_outputs=True)
def create_job():
    # 2. Create a job. You must use glueetl as the name for the ETL command
    # Delete existing job with the same job name
    pprint(
        glue.delete_job(
            JobName="glue-api-job"
        )
    )

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
    create_database()
    create_crawler()
    start_crawler()
    job_name = create_job()
    start_job(job_name)


glue_taskflow_api_dag = glue_taskflow_api_etl()
