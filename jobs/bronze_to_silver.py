# jobs/bronze_to_silver.py (runs on Windows Spark cluster)
import sys
import logging
from spark_utils import create_spark_session
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from config import GCS_BUCKET_NAME

logger = logging.getLogger(__name__)


def clean_orders(spark, run_date: str) -> None:
    raw_path = f"gs://{GCS_BUCKET_NAME}/bronze/orders/{run_date}/"
    df = spark.read.option("multiline", "true").json(raw_path)

    cleaned = (
        df
        .filter(F.col("lat").isNotNull() & F.col("lon").isNotNull())
        .filter((F.col("lat").between(29.5, 30.5)) & (F.col("lon").between(31.0, 31.8)))
        .filter(F.col("order_id").isNotNull())
        .withColumn("lat", F.col("lat").cast(DoubleType()))
        .withColumn("lon", F.col("lon").cast(DoubleType()))
        .withColumn("created_at", F.to_timestamp("created_at"))
        .withColumn("date_partition", F.lit(run_date))
        .dropDuplicates(["order_id"])
    )

    total = df.count()
    valid = cleaned.count()
    drop_rate = round((total - valid) / total * 100, 2) if total > 0 else 0
    logger.info("Orders: %d raw → %d clean (%.2f%% dropped)", total, valid, drop_rate)

    if drop_rate > 5.0:
        raise ValueError(f"Drop rate {drop_rate}% exceeds 5% threshold")

    silver_path = f"gs://{GCS_BUCKET}/silver/orders/{run_date}/"
    cleaned.write.mode("overwrite").parquet(silver_path)
    logger.info("Written to %s", silver_path)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_date = sys.argv[1]
    spark = create_spark_session("BronzeToSilver")
    try:
        clean_orders(spark, run_date)
    finally:
        spark.stop()