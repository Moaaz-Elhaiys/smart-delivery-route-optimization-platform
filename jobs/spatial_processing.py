# jobs/spatial_processing.py (runs on Windows Spark cluster)
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from sedona.register import SedonaRegistrator
from sedona.utils import SedonaKryoRegistrator, KryoSerializer

def create_sedona_spark():
    return SparkSession.builder \
        .appName("SpatialProcessing") \
        .config("spark.serializer", KryoSerializer.getName) \
        .config("spark.kryo.registrator", SedonaKryoRegistrator.getName) \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile",
                "/opt/bitnami/spark/conf/gcs-key.json") \
        .config("spark.hadoop.fs.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .getOrCreate()

def run_spatial_jobs(spark, run_date):
    SedonaRegistrator.registerAll(spark)
    # --- Load silver data ---
    orders_path = f"gs://delivery-data-lake/silver/orders/{run_date}/"
    orders = spark.read.parquet(orders_path)
    # --- Create geometry column from lat/lon ---
    # IMPORTANT: ST_Point takes (longitude, latitude) — not (lat, lon)
    orders = orders.withColumn(
        "geometry",
        F.expr("ST_Point(CAST(lon AS DECIMAL(24,20)), CAST(lat AS DECIMAL(24,20)))")
    )
    orders.createOrReplaceTempView("orders")
    # --- LEARNING EXERCISE 1: Distance between every order and city center ---
    city_center_lon = 31.2357
    city_center_lat = 30.0444

    orders_with_distance = spark.sql(f"""
        SELECT
            order_id,
            district,
            priority,
            lat, lon,
            ST_Distance(
                ST_Transform(geometry, 'EPSG:4326', 'EPSG:32636'),
                ST_Transform(ST_Point({city_center_lon}, {city_center_lat}), 'EPSG:4326', 'EPSG:32636')
            ) / 1000 AS dist_from_center_km
        FROM orders
    """)
    # Why EPSG:32636? It's UTM Zone 36N — the correct metric projection for Cairo.
    # ST_Distance on EPSG:4326 gives degrees, not meters.

    # --- LEARNING EXERCISE 2: Delivery hotspot detection using ST_Buffer ---
    hotspot_query = """
            SELECT
                district,
                COUNT(*) as order_count,
                ST_AsText(ST_Buffer(
                    ST_Transform(ST_Centroid(ST_Union_Aggr(geometry)), 'EPSG:4326', 'EPSG:32636'),
                    1000  -- 1km buffer around district centroid
                )) as coverage_area_wkt
            FROM orders
            GROUP BY district
        """
    hotspots = spark.sql(hotspot_query)
    hotspots.show()

    # --- LEARNING EXERCISE 3: Spatial join — assign orders to service zones ---
    # (In a real project, zones come from a GeoJSON polygon file)
    # For now, cluster by a grid
    orders_gridded = orders.withColumn(
        "grid_cell",
        F.concat(
            F.round(F.col("lat") * 10).cast("int").cast("string"),
            F.lit("_"),
            F.round(F.col("lon") * 10).cast("int").cast("string")
        )
    )

    # --- Write Gold layer ---
    gold_path = f"gs://delivery-data-lake/gold/orders_spatial/{run_date}/"
    orders_with_distance.write.mode("overwrite").parquet(gold_path)
    hotspots.write.mode("overwrite").parquet(
        f"gs://delivery-data-lake/gold/hotspots/{run_date}/"
    )

    print("Spatial processing complete")

if __name__ == "__main__":
    import sys
    run_date = sys.argv[1]
    spark = create_sedona_spark()
    spark.sparkContext.setLogLevel("WARN")
    run_spatial_jobs(spark, run_date)
    spark.stop()