
# def filter_dynamic_frame(dyf: DynamicFrame, column_name: str, value: int):
#     return dyf.filter(f=lambda x: x[column_name] > value)
#
#
# bucket = "s3://awsglue-datasets/examples/us-legislators/all/memberships.json"
# inputDF = glueContext.create_dynamic_frame.from_options(connection_type="s3", connection_options={"paths": [bucket]},
#                                                         format="json")


# def translate(col):
#     return GoogleTranslator(source="auto", target="en").translate(col)
#
#
# udf_translate = udf(lambda x: translate(x), StringType())
#
# sparkDF = inputDF.toDF()  # .withColumn("roleTranslated", udf_translate("role"))
# sparkDF = sparkDF.filter(
#     (sparkDF.legislative_period_id == "term/103") & sparkDF.on_behalf_of_id.endswith("republican")).limit(5)
# sparkDF.select("legislative_period_id", "on_behalf_of_id", "role") \
#     .withColumn("role", lit("spital")) \
#     .withColumn("roleTranslated", udf_translate("role")).show()
from pathlib import Path


def get_remote_file(url, local_dir=None):
    import requests
    data = requests.get(url)
    destination = Path(local_dir) if local_dir else Path(__file__).parent
    target_file = Path(destination, Path(url).name)
    if not target_file.exists():
        print(f"downloading \"{url}\"")
        with open(target_file, "wb") as file:
            file.write(data.content)
            print(f"successfully downloaded \"{url}\" to \"{str(target_file)}\"")
