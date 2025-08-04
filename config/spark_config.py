from pyspark.sql import SparkSession

def get_spark(app_name: str = "EcommercePipeline") -> SparkSession:
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()