"""
Author - Sangam Choubey
Date - 31/07/2025
Version - 0.1
Jira Ticket Id - NA
Design Document - Confluence Link
Description - This module improves the data quality issues and improves the data"
"""
from config.spark_config import get_spark
from config.logger import get_logger
from pyspark.sql.functions import col, round

spark = get_spark("DataEnrichment")
logger = get_logger("data_enrichment")

logger.info("Reading raw tables for enrichment...")
orders = spark.table("raw.orders")
customers = spark.table("raw.customers")
products = spark.table("raw.products")
transactions = spark.table("raw.transactions")

# Data Quality Checks
def check_required_columns(df, required_cols, table_name):
    for col_name in required_cols:
        if col_name not in df.columns:
            logger.error(f"Missing column {col_name} in {table_name}")
            raise Exception(f"Missing column {col_name} in {table_name}")
    logger.info(f"All required columns present in {table_name}")

check_required_columns(customers, ["CustomerID"], "customers")
check_required_columns(products, ["ProductID"], "products")
check_required_columns(orders, ["OrderID", "CustomerID", "ProductID"], "orders")
check_required_columns(transactions, ["OrderID", "Sales", "Cost"], "transactions")

# Enriched customers
customers_clean = customers.dropna(subset=["CustomerID"]).dropDuplicates(["CustomerID"])
customers_clean.write.mode("overwrite").saveAsTable("enriched.customers")
logger.info("Enriched customers table created.")

# Enriched products
products_clean = products.dropna(subset=["ProductID"]).dropDuplicates(["ProductID"])
products_clean.write.mode("overwrite").saveAsTable("enriched.products")
logger.info("Enriched products table created.")

# Enriched sales_orders
sales_orders = orders.join(transactions, "OrderID") \
    .join(customers.select("CustomerID", "CustomerName", "Country"), "CustomerID") \
    .join(products.select("ProductID", "Category", "SubCategory"), "ProductID") \
    .withColumn("Profit", round(col("Sales") - col("Cost"), 2))

sales_orders.write.mode("overwrite").saveAsTable("enriched.sales_orders")
logger.info("Enriched sales_orders table created.")
