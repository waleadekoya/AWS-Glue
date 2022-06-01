from awsglue.dynamicframe import DynamicFrame
import sys

from pyspark import SparkContext
from pyspark.sql.functions import udf, lit
from pyspark.sql.types import StringType
from deep_translator import GoogleTranslator
from awsglue.context import GlueContext

glueContext = GlueContext(SparkContext.getOrCreate())


def filter_dynamic_frame(dyf: DynamicFrame, column_name: str, value: int):
    return dyf.filter(f=lambda x: x[column_name] > value)


bucket = "s3://awsglue-datasets/examples/us-legislators/all/memberships.json"
inputDF = glueContext.create_dynamic_frame.from_options(connection_type="s3", connection_options={"paths": [bucket]},
                                                        format="json")


def translate(col):
    return GoogleTranslator(source="auto", target="en").translate(col)


udf_translate = udf(lambda x: translate(x), StringType())

sparkDF = inputDF.toDF()  # .withColumn("roleTranslated", udf_translate("role"))
sparkDF = sparkDF.filter(
    (sparkDF.legislative_period_id == "term/103") & sparkDF.on_behalf_of_id.endswith("republican")).limit(5)
sparkDF.select("legislative_period_id", "on_behalf_of_id", "role") \
    .withColumn("role", lit("spital")) \
    .withColumn("roleTranslated", udf_translate("role")).show()
