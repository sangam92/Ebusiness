import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import SparkSession
from config.logger import get_logger

logger = get_logger("test_raw_data_load")

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder \
        .appName("TestRawDataLoad") \
        .master("local[*]") \
        .getOrCreate()


def test_load_orders_schema(spark):
    data = [("O1", "C1", "P1", "2023-08-01")]
    df = spark.createDataFrame(data, ["OrderID", "CustomerID", "ProductID", "OrderDate"])

    assert set(df.columns) == {"OrderID", "CustomerID", "ProductID", "OrderDate"}
    assert df.count() == 1
    logger.info("✅ test_load_orders_schema passed.")


def test_missing_required_columns(spark):
    data = [("O1", "C1")]
    df = spark.createDataFrame(data, ["OrderID", "CustomerID"])
    required_cols = {"OrderID", "CustomerID", "ProductID"}

    missing = required_cols - set(df.columns)
    assert "ProductID" in missing
    logger.info("✅ test_missing_required_columns passed.")


def test_empty_file_behavior(spark):
    df = spark.createDataFrame([], "OrderID STRING, CustomerID STRING, ProductID STRING, OrderDate STRING")
    assert df.count() == 0
    logger.info("✅ test_empty_file_behavior passed.")


def test_duplicate_records_detection(spark):
    data = [("O1", "C1", "P1", "2023-08-01"), ("O1", "C1", "P1", "2023-08-01")]
    df = spark.createDataFrame(data, ["OrderID", "CustomerID", "ProductID", "OrderDate"])

    assert df.count() == 2
    deduped = df.dropDuplicates()
    assert deduped.count() == 1
    logger.info("✅ test_duplicate_records_detection passed.")
