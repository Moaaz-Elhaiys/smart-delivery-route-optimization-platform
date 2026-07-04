# jobs/spark_utils.py
from pyspark.sql import SparkSession
from sedona.register import SedonaRegistrator
from sedona.utils import SedonaKryoRegistrator, KryoSerializer
from config import GCS_BUCKET_NAME, GCS_CREDENTIALS_PATH


def create_spark_session(
    app_name: str,
    sedona: bool = False,
    gcs_key_path: str = "/opt/bitnami/spark/conf/gcs-key.json",
) -> SparkSession:
    """Create a SparkSession with optional Sedona and GCS support."""
    builder = SparkSession.builder.appName(app_name)

    # GCS connector
    builder = builder \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", GCS_CREDENTIALS_PATH) \
        .config("spark.hadoop.fs.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")

    # Sedona
    if sedona:
        builder = builder \
            .config("spark.serializer", KryoSerializer.getName()) \
            .config("spark.kryo.registrator", SedonaKryoRegistrator.getName())

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Suppress verbose py4j logs
    import logging
    logging.getLogger("py4j").setLevel(logging.WARNING)

    if sedona:
        SedonaRegistrator.registerAll(spark)

    return spark