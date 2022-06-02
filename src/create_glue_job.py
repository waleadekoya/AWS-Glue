from pprint import pprint

import boto3
import awswrangler as wr
from pathlib import Path

etl_script = Path(Path(__file__).parent, "glue_job_etl_script.py")
s3_location = f"s3://aws-glue-data-source-az/scripts/{etl_script.name}"

# Upload Python Script to S3
wr.s3.upload(str(etl_script), path=s3_location)

# https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-python-calling.html


# 1. Create an instance of the AWS Glue client
glue = boto3.client(
    service_name="glue",
    region_name="eu-west-1",
    endpoint_url="https://glue.eu-west-1.amazonaws.com"
)

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

# Create Crawler
crawler = glue.create_crawler(
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

# Start Crawler
glue.start_crawler(Name="stocks_database_crawler")

# 2. Create a job. You must use glueetl as the name for the ETL command
# Delete existing job with the same job name
pprint(
    glue.delete_job(
        JobName="glue-api-job"
    )
)

myJob = glue.create_job(
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue.html#Glue.Client.create_job
    Name="glue-api-job",
    Description="ETL job using translate API to convert non-English texts to English",
    Role="AWSGlueServiceRole-Developer",
    Command={
        "Name": "glueetl",
        "ScriptLocation": s3_location,
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
pprint(myJob)
# https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-python-libraries.html
# https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api.html
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue.html

# 3. Start a new run of the job that you created in the previous step
# myNewJobRun = glue.start_job_run(JobName=myJob['Name'])

# 4 Get Job Status
# status = glue.get_job_run(JobName=myJob['Name'], RunId=myNewJobRun['JobRunId'])

# Print current state of the job
# pprint(status['JobRun']['JobRunState'])
