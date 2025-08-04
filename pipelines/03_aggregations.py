"""
Author - Sangam Choubey
Date - 04/08/2025
Version - 0.1
Jira Ticket Id - NA
Design Document - Confluence Link
Description - This module contains all the aggregation of the data "
"""

from config.spark_config import get_spark
from config.logger import get_logger
from pyspark.sql.functions import year, sum as _sum, round

spark = get_spark("Aggregations")
logger = get_logger("aggregations")

sales_orders = spark.table("enriched.sales_orders")
logger.info("Performing aggregations on sales_orders...")

if sales_orders.rdd.isEmpty():
    logger.error("No data found in enriched.sales_orders. Aborting aggregation.")
    raise Exception("Empty enriched.sales_orders table")

sales_orders = sales_orders.withColumn("Year", year("OrderDate"))

# Profit by Year
agg_year = sales_orders.groupBy("Year").agg(round(_sum("Profit"), 2).alias("Profit"))
agg_year.show()

# Profit by Year and Category
agg_year_cat = sales_orders.groupBy("Year", "Category").agg(round(_sum("Profit"), 2).alias("Profit"))
agg_year_cat.show()

# Profit by Customer
agg_cust = sales_orders.groupBy("CustomerID").agg(round(_sum("Profit"), 2).alias("Profit"))
agg_cust.show()

# Profit by Customer and Year
agg_cust_year = sales_orders.groupBy("CustomerID", "Year").agg(round(_sum("Profit"), 2).alias("Profit"))
agg_cust_year.show()

# Full aggregation table
df_agg = sales_orders.groupBy("Year", "Category", "SubCategory", "CustomerID") \
    .agg(round(_sum("Profit"), 2).alias("TotalProfit"))

df_agg.write.mode("overwrite").saveAsTable("agg.sales_summary")
logger.info("Aggregated table written to agg.sales_summary")



