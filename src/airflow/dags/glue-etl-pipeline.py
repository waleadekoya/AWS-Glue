# https://stackoverflow.com/a/66248028
from datetime import datetime, timedelta

from pathlib import Path

from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

# Quick Start: https://airflow.apache.org/docs/apache-airflow/stable/start/index.html

# Local Set Up: https://airflow.apache.org/docs/apache-airflow/stable/start/local.html

from src.utils import get_remote_file

"""
# 1. https://linuxize.com/post/how-to-install-python-3-7-on-ubuntu-18-04/
# 2. sudo apt-get install python3-pip

export AIRFLOW_HOME=~/airflow
AIRFLOW_VERSION=2.3.1
PYTHON_VERSION="$(python3.7 --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

if you get:  WARNING: The script airflow is installed in '/home/wale/.local/bin' which is not on PATH.
DO THIS:  export PATH="$PATH:/home/wale/.local/bin"


to start development server: airflow standalone
http://localhost:8080/login/
Login with username: admin  password: uYXqufgHAxPpmwFc
https://www.ntweekly.com/2021/05/18/open-wsl-path-on-windows-explorer/
"""

# Docker Set Up: https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html

glue_job_name = "glue-api-job"
glue_iam_role = "arn:aws:iam::578120814996:role/service-role/AWSGlueServiceRole-Developer"
region_name = "eu-west-1"
email_recipient = "me@gmail.com"
etl_script = Path(Path(__file__).parent, "glue_job_etl_script.py")
script_location = f"s3://aws-glue-data-source-az/scripts/{etl_script.name}"

default_args = {
    'owner': 'me',
    'start_date': datetime(2020, 1, 1),
    'retry_delay': timedelta(minutes=5),
    'email': email_recipient,
    'email_on_failure': True
}

with DAG(dag_id="glue-etl-pipeline", default_args=default_args, schedule_interval=None) as dag:
    # https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/_api/airflow/providers/amazon/aws/operators/glue/index.html
    # https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/operators/glue.html#howto-operator-gluejoboperator
    pass

job_name = 'example_glue_job_with_airflow'
submit_glue_job = GlueJobOperator(
    task_id='submit_glue_job',
    job_name=job_name,
    wait_for_completion=False,
    script_location=script_location,
    s3_bucket="s3://aws-glue-data-source-az/data/aws_wrangler_database",
    iam_role_name=glue_iam_role,
    create_job_kwargs={'GlueVersion': '3.0', 'NumberOfWorkers': 3, 'WorkerType': 'G.1X'},
)

# get_remote_file('https://airflow.apache.org/docs/apache-airflow/2.3.1/docker-compose.yaml')

# Deployment Options for Airflow on AWS https://thinkport.digital/how-to-deploy-airflow-on-aws/
# 1. Deploy on AWS EKS # https://aws.amazon.com/eks/
# 2. Deploy on AWS EC2
# 3. Use Amazon Managed Workflow for Apache Airflow (AMWAA) Service
# Deploy locally on Kubernetes
