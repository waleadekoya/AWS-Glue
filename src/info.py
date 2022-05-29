import sys

from pyspark import SparkContext
from awsglue.context import GlueContext

# https://aws.amazon.com/blogs/big-data/building-an-aws-glue-etl-pipeline-locally-without-an-aws-account/
# https://github.com/awslabs/aws-glue-libs
# https://aws.amazon.com/blogs/big-data/developing-aws-glue-etl-jobs-locally-using-a-container/
# https://docs.microsoft.com/en-gb/windows/wsl/install-manual#step-4---download-the-linux-kernel-update-package # step 4 & 5


# https://hub.docker.com/r/amazon/aws-glue-libs/tags
# https://phoenixnap.com/kb/remove-docker-images-containers-networks-volumes

# UDF - functions
# https://sqlandhadoop.com/pyspark-apply-function-to-column/

# docker pull amazon/aws-glue-libs:glue_libs_2.0.0_image_01
# docker pull amazon/aws-glue-libs:glue_libs_1.0.0_image_01  [this has python 3.6]

# AWS Toolkit for JetBrains
# https://docs.aws.amazon.com/toolkit-for-jetbrains/latest/userguide/welcome.html

# Convert Docker run command to Docker Compose commands
# https://www.composerize.com/

# local set up/deployment
# https://aws.plainenglish.io/aws-glue-development-using-docker-c6b1ccfab12b
# https://cevo.com.au/post/aws-glue-local-development/

# docker run -itd -p 8888:8888 -p 4040:4040 -v %UserProfile%\.aws:/root/.aws:rw --name glue_jupyter amazon/aws-glue-libs:glue_libs_1.0.0_image_01 /home/jupyter/jupyter_start.sh

print("this runs")
print(sys.executable)

bucket = "s3://awsglue-datasets/examples/us-legislators/all/memberships.json"
glueContext = GlueContext(SparkContext.getOrCreate())
inputDF = glueContext.create_dynamic_frame.from_options(connection_type="s3", connection_options={"paths": [bucket]},
                                                        format="json")

inputDF.toDF().show()
