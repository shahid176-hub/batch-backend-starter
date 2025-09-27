from pyspark.sql import SparkSession, functions as F
import os

def get_spark():
    spark = (SparkSession.builder
             .appName("nyc-batch-clean-aggregate")
             .config("spark.hadoop.fs.s3a.endpoint", os.getenv("S3A_ENDPOINT","http://minio:9000"))
             .config("spark.hadoop.fs.s3a.path.style.access", "true")
             .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID","minio"))
             .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY","minio123"))
             .getOrCreate())
    return spark

def main():
    spark = get_spark()

    raw_path = "s3a://lake/raw/nyc_taxi/*.csv"
    df = (spark.read
                .option("header", True)
                .option("inferSchema", True)
                .csv(raw_path))

    df = (df
          .withColumn("tpep_pickup_datetime", F.to_timestamp("tpep_pickup_datetime"))
          .withColumn("tpep_dropoff_datetime", F.to_timestamp("tpep_dropoff_datetime"))
          .dropna(subset=["tpep_pickup_datetime", "total_amount", "trip_distance"]))

    zone_col = "PULocationID" if "PULocationID" in df.columns else None
    if zone_col is None:
        df = df.withColumn("zone", F.lit("UNKNOWN"))
    else:
        df = df.withColumn("zone", F.col(zone_col).cast("string"))

    df = (df
          .withColumn("year", F.year("tpep_pickup_datetime"))
          .withColumn("quarter", F.quarter("tpep_pickup_datetime")))

    agg = (df.groupBy("year","quarter","zone")
             .agg(F.count("*").alias("trips"),
                  F.sum("total_amount").alias("revenue"),
                  F.avg("trip_distance").alias("avg_trip_distance")))

    out_processed = "s3a://lake/processed/nyc_taxi/"
    (agg.write
        .mode("overwrite")
        .partitionBy("year","quarter")
        .parquet(out_processed))

    pg_url = "jdbc:postgresql://postgres:5432/warehouse"
    props = {"user":"postgres","password":"pgpass","driver":"org.postgresql.Driver"}
    (agg.write
        .mode("overwrite")
        .option("truncate","true")
        .jdbc(pg_url, "features.zone_quarter_agg", properties=props))

    spark.stop()

if __name__ == "__main__":
    main()