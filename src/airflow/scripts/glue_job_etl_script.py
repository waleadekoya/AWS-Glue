import sys

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from deep_translator import GoogleTranslator
from pyspark import SparkContext
from pyspark.sql.functions import udf, lit
from pyspark.sql.types import StringType

# https://aws.amazon.com/blogs/big-data/building-python-modules-from-a-wheel-for-spark-etl-workloads-using-aws-glue-2-0/
# https://aws.amazon.com/premiumsupport/knowledge-center/glue-version2-external-python-libraries/
# https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-python-calling.html

## @params: [JOB_NAME]

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

job = Job(glueContext)

job.init(args['JOB_NAME'], args)

# =####################### Custom Code Starts Here ####################################=

# Read the data
datasource0 = glueContext.create_dynamic_frame.from_catalog(database="customers_database", table_name="customers_csv",

                                                            transformation_ctx="datasource0")
# Convert the DynamicFrame to Spark DataFrame
spark_df = datasource0.toDF()


# Make Transformations
def translate(col):
    return GoogleTranslator(source="auto", target="en").translate(col)


udf_translate = udf(lambda x: translate(x), StringType())
spark_df = spark_df.filter((spark_df.title == "Ms.")).limit(5).withColumn("companyname", lit("spital"))
spark_df = spark_df.withColumn("companyname_en", udf_translate("companyname"))
spark_df.show()

# Convert backed to a Dynamic DataFrame
dy_df = DynamicFrame.fromDF(spark_df, glueContext, "dy_df")

# Result is re-ingested into S3 bucket
# https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format.html
dest_bucket = "s3://aws-glue-data-source-az/data/customers_database/outputs/"
dataset = glueContext.write_dynamic_frame.from_options(frame=dy_df, connection_type="s3",
                                                       connection_options=dict(path=dest_bucket),
                                                       format="csv",
                                                       format_options={
                                                           "quoteChar": -1,
                                                           "separator": "|"
                                                       },
                                                       transformation_ctx="dataset")

# =####################### Custom Code Ends Here ####################################=

job.commit()