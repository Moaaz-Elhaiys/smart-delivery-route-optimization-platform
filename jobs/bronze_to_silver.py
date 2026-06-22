# jobs/bronze_to_silver.py (runs on Windows Spark cluster)
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

def create_spark():
    return SparkSession.builder \
        .appName("BronzeToSilver") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile",
                "/opt/bitnami/spark/conf/gcs-key.json") \
        .config("spark.hadoop.fs.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .getOrCreate()

def clean_orders(spark, run_date):
    raw_path = f"gs://delivery-data-lake/bronze/orders/{run_date}/"
    df = spark.read.option("multiline", "true").json(raw_path)

    cleaned = df \
        .filter(F.col("lat").isNotNull() & F.col("lon").isNotNull()) \
        .filter((F.col("lat").between(29.5, 30.5)) & (F.col("lon").between(31.0, 31.8))) \
        .filter(F.col("order_id").isNotNull()) \
        .withColumn("lat",  F.col("lat").cast(DoubleType())) \
        .withColumn("lon",  F.col("lon").cast(DoubleType())) \
        .withColumn("created_at", F.to_timestamp("created_at")) \
        .withColumn("date_partition", F.lit(run_date)) \
        .dropDuplicates(["order_id"])

    # Data quality metrics
    total = df.count()
    valid = cleaned.count()
    drop_rate = round((total - valid) / total * 100, 2)
    print(f"Orders: {total} raw → {valid} clean ({drop_rate}% dropped)")
    assert drop_rate < 5.0, f"Drop rate {drop_rate}% exceeds 5% threshold — check data quality"

    silver_path = f"gs://delivery-data-lake/silver/orders/{run_date}/"
    cleaned.write.mode("overwrite").parquet(silver_path)
    print(f"Written to {silver_path}")

if __name__ == "__main__":
    import sys
    run_date = sys.argv[1]  # e.g. "2024-01-15"
    spark = create_spark()
    # ADD THIS LINE: Tells Spark to only log Warnings and Errors
    spark.sparkContext.setLogLevel("WARN")
    clean_orders(spark, run_date)
    spark.stop()