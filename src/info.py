import sys

from pyspark import SparkContext
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

from awsglue.context import GlueContext
from deep_translator import GoogleTranslator

# https://aws.amazon.com/blogs/big-data/building-an-aws-glue-etl-pipeline-locally-without-an-aws-account/
# https://github.com/awslabs/aws-glue-libs
# https://aws.amazon.com/blogs/big-data/developing-aws-glue-etl-jobs-locally-using-a-container/
# https://docs.microsoft.com/en-gb/windows/wsl/install-manual#step-4---download-the-linux-kernel-update-package # step 4 & 5
# docker image rm [Image ID] [Image ID] [Image ID]

# Docker SetUp
# https://hub.docker.com/r/amazon/aws-glue-libs/tags
# https://phoenixnap.com/kb/remove-docker-images-containers-networks-volumes
# https://github.com/cevoaustralia/glue-vscode/tree/v1.0-and-v2.0

# docker pull amazon/aws-glue-libs:glue_libs_2.0.0_image_01
# docker pull amazon/aws-glue-libs:glue_libs_1.0.0_image_01  [this has python 3.6]

# AWS Toolkit for JetBrains
# https://docs.aws.amazon.com/toolkit-for-jetbrains/latest/userguide/welcome.html
# tcp://localhost:2375

# Convert Docker run command to Docker Compose commands
# https://www.composerize.com/

# local set up/deployment
# https://aws.plainenglish.io/aws-glue-development-using-docker-c6b1ccfab12b
# https://cevo.com.au/post/aws-glue-local-development/
# https://www.jetbrains.com/help/pycharm/using-docker-compose-as-a-remote-interpreter.html#example

# UDF - functions
# https://sqlandhadoop.com/pyspark-apply-function-to-column/

# Docker set up steps:
# 1. docker pull amazon/aws-glue-libs:glue_libs_1.0.0_image_01
# 2. docker run -itd -p 8888:8888 -p 4040:4040 -v %UserProfile%\.aws:/root/.aws:rw --name glue_jupyter amazon/aws-glue-libs:glue_libs_1.0.0_image_01 /home/jupyter/jupyter_start.sh
# 3. docker ps
# 4. http://localhost:8888 for Jupyter Notebook
# 5

print("this runs")
print(sys.executable)

bucket = "s3://awsglue-datasets/examples/us-legislators/all/memberships.json"
glueContext = GlueContext(SparkContext.getOrCreate())
inputDF = glueContext.create_dynamic_frame.from_options(connection_type="s3", connection_options={"paths": [bucket]},
                                                        format="json")


def translate(col):
    return GoogleTranslator(source="auto", target="en").translate(col)


udf_translate = udf(lambda x: translate(x), StringType())

sparkDF = inputDF.toDF().withColumn("roleTranslated", udf_translate("role"))
sparkDF.show()

# sc = SparkContext()
# glueContext = GlueContext(sc)
# spark = glueContext.spark_session
# spark.sql("show databases").show()
