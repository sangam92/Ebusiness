import pytest
from pyspark.sql.functions import year, col, round, sum as _sum
from chispa.dataframe_comparer import assert_df_equality
from config.logger import get_logger

logger = get_logger("test_aggregations")

@pytest.fixture(scope="module")
def enriched_sales_orders(spark):
    return spark.createDataFrame([
        ("O1", "2023-01-01", 100.0, 80.0, "C1", "Electronics", "Phones"),
        ("O2", "2023-06-01", 200.0, 150.0, "C1", "Electronics", "Laptops"),
        ("O3", "2024-03-15", 300.0, 200.0, "C2", "Clothing", "Shirts")
    ], ["OrderID", "OrderDate", "Sales", "Cost", "CustomerID", "Category", "SubCategory"])

def test_aggregation_profit_by_year(spark, enriched_sales_orders):
    df = enriched_sales_orders.withColumn("Profit", round(col("Sales") - col("Cost"), 2))
    df = df.withColumn("Year", year("OrderDate"))

    result = df.groupBy("Year").agg(round(_sum("Profit"), 2).alias("Profit"))

    expected = spark.createDataFrame([
        (2023, 70.0),
        (2024, 100.0)
    ], ["Year", "Profit"])

    assert_df_equality(result.orderBy("Year"), expected.orderBy("Year"))
    logger.info(" test_aggregation_profit_by_year passed.")


def test_aggregation_by_year_category(spark, enriched_sales_orders):
    df = enriched_sales_orders.withColumn("Profit", round(col("Sales") - col("Cost"), 2))
    df = df.withColumn("Year", year("OrderDate"))

    result = df.groupBy("Year", "Category").agg(round(_sum("Profit"), 2).alias("Profit"))

    expected = spark.createDataFrame([
        (2023, "Electronics", 70.0),
        (2024, "Clothing", 100.0)
    ], ["Year", "Category", "Profit"])

    assert_df_equality(result.orderBy("Year"), expected.orderBy("Year"))
    logger.info(" test_aggregation_by_year_category passed.")


def test_aggregation_by_customer(spark, enriched_sales_orders):
    df = enriched_sales_orders.withColumn("Profit", round(col("Sales") - col("Cost"), 2))

    result = df.groupBy("CustomerID").agg(round(_sum("Profit"), 2).alias("Profit"))

    expected = spark.createDataFrame([
        ("C1", 70.0),
        ("C2", 100.0)
    ], ["CustomerID", "Profit"])

    assert_df_equality(result.orderBy("CustomerID"), expected.orderBy("CustomerID"))
    logger.info(" test_aggregation_by_customer passed.")


def test_aggregation_by_customer_and_year(spark, enriched_sales_orders):
    df = enriched_sales_orders.withColumn("Profit", round(col("Sales") - col("Cost"), 2))
    df = df.withColumn("Year", year("OrderDate"))

    result = df.groupBy("CustomerID", "Year").agg(round(_sum("Profit"), 2).alias("Profit"))

    expected = spark.createDataFrame([
        ("C1", 2023, 70.0),
        ("C2", 2024, 100.0)
    ], ["CustomerID", "Year", "Profit"])

    assert_df_equality(result.orderBy("CustomerID"), expected.orderBy("CustomerID"))
    logger.info(" test_aggregation_by_customer_and_year passed.")
