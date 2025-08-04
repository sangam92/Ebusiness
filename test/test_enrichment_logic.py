import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql.functions import col, round
from config.spark_config import get_spark
from config.logger import get_logger

logger = get_logger("test_enrichment")

@pytest.fixture(scope="session")
def spark():
    return get_spark("TestSession")

def test_profit_calculation(spark):
    logger.info("Testing profit calculation logic...")
    input_df = spark.createDataFrame([
        ("O1", 100.0, 80.0),
        ("O2", 200.0, 150.0)
    ], ["OrderID", "Sales", "Cost"])

    expected_df = spark.createDataFrame([
        ("O1", 20.0),
        ("O2", 50.0)
    ], ["OrderID", "Profit"])

    result_df = input_df.withColumn("Profit", round(col("Sales") - col("Cost"), 2)).select("OrderID", "Profit")
    assert_df_equality(result_df, expected_df)
    logger.info("Profit calculation test passed.")


def test_profit_by_year(spark):
    from pyspark.sql.functions import year, sum as _sum

    df = spark.createDataFrame([
        ("2023-01-01", 100.0),
        ("2023-06-01", 200.0),
        ("2024-03-01", 300.0)
    ], ["OrderDate", "Profit"])

    df = df.withColumn("Year", year("OrderDate"))

    expected = spark.createDataFrame([
        (2023, 300.0),
        (2024, 300.0)
    ], ["Year", "Profit"])

    result = df.groupBy("Year").agg(round(_sum("Profit"), 2).alias("Profit"))

    assert_df_equality(result, expected)
    logger.info("Profit by year aggregation test passed.")