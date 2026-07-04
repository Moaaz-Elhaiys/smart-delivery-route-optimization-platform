# jobs/road_network_processing.py (runs on Windows Spark cluster)
import sys
from pyspark.sql import functions as F
from spark_utils import create_spark_session

def process_roads(spark, run_date):
    raw_path = f"gs://delivery-data-lake/bronze/roads/{run_date}/"
    roads_raw = spark.read.option("multiline", "true").json(raw_path)

    # Explode OSM elements array
    elements = roads_raw.select(F.explode("elements").alias("elem"))

    # Extract road attributes
    roads = elements.select(
        F.col("elem.id").alias("road_id"),
        F.col("elem.tags.highway").alias("road_type"),
        F.col("elem.tags.maxspeed").alias("maxspeed_raw"),
        F.col("elem.tags.name").alias("road_name"),
        F.col("elem.tags.oneway").alias("is_oneway"),
        F.col("elem.geometry").alias("geometry_nodes"),
    ).filter(F.col("road_type").isNotNull())

    # Parse speed: OSM maxspeed is a string like "60" or "60 mph"
    roads = roads.withColumn(
        "speed_kmh",
        F.when(
            F.col("maxspeed_raw").rlike("^[0-9]+$"),
            F.col("maxspeed_raw").cast("int")
        ).when(
            F.col("maxspeed_raw").rlike("mph"),
            (F.regexp_extract("maxspeed_raw", r"(\d+)", 1).cast("int") * 1.609).cast("int")
        ).otherwise(
            # Default speeds by road type (Cairo approximations)
            F.when(F.col("road_type") == "motorway", 100)
                .when(F.col("road_type") == "trunk", 80)
                .when(F.col("road_type") == "primary", 60)
                .when(F.col("road_type") == "secondary", 50)
                .otherwise(30)
        )
    )

    silver_path = f"gs://delivery-data-lake/silver/roads/{run_date}/"
    roads.write.mode("overwrite").parquet(silver_path)
    print(f"Roads written: {roads.count()} segments")

if __name__ == "__main__":
    run_date = sys.argv[1]
    spark = create_spark_session(app_name="RoadNetworkProcessing", sedona=True)
    process_roads(spark, run_date)
    spark.stop()