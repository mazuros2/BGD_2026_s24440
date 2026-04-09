from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from db_config import POSTGRES_URL, POSTGRES_PROPS
from spark.db_config import SILVER_TABLE

def run_gold_requests_table(spark, df_silver, df_agency, df_location, df_complaint, df_status):
    df_requests_table = (
        df_silver

        .withColumn("date_key",
            F.date_format("created_date", "yyyyMMdd").cast("int"))

        .join(df_agency.select("agency_key", "agency_code"),
              on="agency_code", how="left")

        .withColumn("incident_zip_join",
            F.coalesce(F.col("incident_zip"), F.lit("N/A")))
        .join(
            df_location.select("location_key", "borough",
                F.coalesce(F.col("incident_zip"), F.lit("N/A")).alias("incident_zip_join")
            ),
            on=["borough", "incident_zip_join"],
            how="left"
        )

        .join(df_complaint.select("complaint_key", "complaint_type"),
              on="complaint_type", how="left")

        .join(df_status.select("status_key", "status_code"),
              df_silver["status"] == df_status["status_code"],
              how="left")

        .withColumn("resolution_hours",
            F.round(
                (F.unix_timestamp("closed_date") - F.unix_timestamp("created_date"))
                / 3600.0
            ).cast("int"))

        .select(
            "request_id",
            "date_key",
            "agency_key",
            "location_key",
            "complaint_key",
            "status_key",
            "resolution_hours"
        )
    )

    df_requests_table.write.mode("append").option("batchsize", "10000").jdbc(
        POSTGRES_URL, "gold.requests", properties=POSTGRES_PROPS
    )

    return df_requests_table

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("nyc_311_bronze")
        .config("spark.driver.memory", "4g")
        .config("spark.jars", "/opt/postgresql-jdbc.jar")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df_silver    = spark.read.jdbc(POSTGRES_URL, SILVER_TABLE, properties=POSTGRES_PROPS)
    df_agency    = spark.read.jdbc(POSTGRES_URL, "gold.agency",            properties=POSTGRES_PROPS)
    df_location  = spark.read.jdbc(POSTGRES_URL, "gold.location",          properties=POSTGRES_PROPS)
    df_complaint = spark.read.jdbc(POSTGRES_URL, "gold.complaint",         properties=POSTGRES_PROPS)
    df_status    = spark.read.jdbc(POSTGRES_URL, "gold.status",            properties=POSTGRES_PROPS)

    run_gold_requests_table(spark, df_silver, df_agency, df_location, df_complaint, df_status)
    spark.stop()