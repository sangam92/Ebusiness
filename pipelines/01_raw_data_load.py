"""
Author - Sangam Choubey
Date - 31/07/2025
Version - 0.1
Jira Ticket Id - NA
Design Document - Confluence Link
Description - This module checks for the raw file in the Filestore and create the RAW table from it
"""


from pyspark.sql import SparkSession
from config.spark_config import get_spark
from config.logger import get_logger

logger = get_logger("raw data loading started")
spark = get_spark("EcommercePipeline")


logger = get_logger("raw paths for all the files")
raw_paths = {
    "orders": "/FileStore/tables/orders.csv",
    "customers": "/FileStore/tables/customers.csv",
    "products": "/FileStore/tables/products.csv",
    "transactions": "/FileStore/tables/transactions.csv"
}

logger = get_logger("reading all the raw path and creating table from it")

def load_raw_table(name, path):
    df = spark.read.option("header", True).csv(path)
    df.write.mode("overwrite").saveAsTable(f"raw.{name}")

for name, path in raw_paths.items():
    load_raw_table(name, path)

logger = get_logger("Raw tables created.")
