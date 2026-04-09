from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from db_config import POSTGRES_URL, POSTGRES_PROPS
from spark.db_config import SILVER_TABLE


def run_gold_aggregations(spark, df_requests_table, df_complaint, df_date, df_location, df_silver):

    df_complaint_by_year = (
        df_requests_table
        .join(df_complaint.select("complaint_key", "complaint_type", "category"),
              on="complaint_key", how="inner")
        .join(df_date.select("date_key", "year"),
              on="date_key", how="inner")
        .groupBy("category", "complaint_type", "year")
        .agg(
            F.count("*").alias("request_count"),
            F.round(
                F.avg(
                    F.when(F.col("resolution_hours") > 0, F.col("resolution_hours"))
                ), 1
            ).alias("avg_resolution_hours")
        )
        .orderBy("year", F.col("request_count").desc())
    )

    df_complaint_by_year.write.mode("append").option("batchsize", "10000").jdbc(
        POSTGRES_URL, "gold.aggregation_complaint_by_year", properties=POSTGRES_PROPS
    )

    df_channel_by_borough = (
        df_requests_table
        .join(df_silver.select("request_id", "channel"),
              on="request_id", how="inner")
        .join(df_location.select("location_key", "borough"),
              on="location_key", how="inner")
        .filter(F.col("channel").isNotNull())
        .filter(F.col("borough").isNotNull())
        .groupBy("borough", "channel")
        .agg(F.count("*").alias("request_count"))
        .withColumn(
            "pct_of_borough_total",
            F.round(
                100.0 * F.col("request_count") /
                F.sum("request_count").over(Window.partitionBy("borough")),
                1
            )
        )
        .orderBy("borough", F.col("request_count").desc())
    )

    df_channel_by_borough.write.mode("append").option("batchsize", "10000").jdbc(
        POSTGRES_URL, "gold.aggregation_channel_by_borough", properties=POSTGRES_PROPS
    )

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("nyc_311_bronze")
        .config("spark.driver.memory", "4g")
        .config("spark.jars", "/opt/postgresql-jdbc.jar")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df_requests_table      = spark.read.jdbc(POSTGRES_URL, "gold.requests",         properties=POSTGRES_PROPS)
    df_complaint = spark.read.jdbc(POSTGRES_URL, "gold.complaint",        properties=POSTGRES_PROPS)
    df_date      = spark.read.jdbc(POSTGRES_URL, "gold.date",             properties=POSTGRES_PROPS)
    df_location  = spark.read.jdbc(POSTGRES_URL, "gold.location",         properties=POSTGRES_PROPS)
    df_silver    = spark.read.jdbc(POSTGRES_URL, SILVER_TABLE,                  properties=POSTGRES_PROPS)

    run_gold_aggregations(spark, df_requests_table, df_complaint, df_date, df_location, df_silver)
    spark.stop()