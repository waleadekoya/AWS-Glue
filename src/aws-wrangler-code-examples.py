import awswrangler as wr
import pandas as pd
import boto3
from datetime import datetime

# boto3.Session()

df = pd.DataFrame({
    "id": [1, 2, 3],
    "name": ["shoes", "tshirt", "ball"],
    "price": [50.3, 10.5, 20.0],
    "in_stock": [True, True, False]
})
print(df)

# Storing data on Data Lake
wr.s3.to_csv(
    df=df,
    path="s3://aws-glue-data-source-az/data/aws_wrangler_database/sample3.csv",
    sep="|",
    # mode="overwrite",
    # dataset=True,
    # database="aws_wrangler_database",
    # table="my_table"
    index=False
)

databases = wr.catalog.databases()
print(databases)

# https://aws-data-wrangler.readthedocs.io/en/stable/tutorials/005%20-%20Glue%20Catalog.html
if "awswrangler_test" not in databases.values:
    wr.catalog.create_database("awswrangler_test")
    print(wr.catalog.databases())
else:
    print("Database awswrangler_test already exists")

print(wr.catalog.tables(database="customers_database"))

desc = "This is my product table."

param = {
    "source": "Product Web Service",
    "class": "e-commerce"
}

comments = {
    "id": "Unique product ID.",
    "name": "Product name",
    "price": "Product price (dollar)",
    "in_stock": "Is this product available in the stock?"
}

# res = wr.s3.to_parquet(
#     df=df,
#     path="s3://aws-glue-data-source-az/data/aws_wrangler_database/",
#     dataset=True,
#     database="awswrangler_test",
#     table="products",
#     mode="overwrite",
#     description=desc,
#     parameters=param,
#     columns_comments=comments
# )
#
# print(databases.values)
for table in wr.catalog.get_tables(database="awswrangler_test"):
    wr.catalog.delete_table_if_exists(database="awswrangler_test", table=table["Name"])
print(wr.catalog.delete_database('awswrangler_test'))

df = wr.s3.select_query(
    sql="SELECT * FROM s3object",
    path="s3://aws-glue-data-source-az/data/aws_wrangler_database/sample3.csv",  # 58 MB
    input_serialization="CSV",
    input_serialization_params={
        "FileHeaderInfo": "Use",
        "RecordDelimiter": "\r\n",
    },
    use_threads=True,
)
print(df.head())
# data = wr.s3.read_csv(path="s3://aws-glue-data-source-az/data/aws_wrangler_database/sample3.csv", sep="|")
# print(data.head())
#
# data = wr.s3.read_parquet(path="s3://aws-glue-data-source-az/data/aws_wrangler_database/0217c4ac1cb74d8fb79cf281014264fa.snappy.parquet")
# print(data.head())
