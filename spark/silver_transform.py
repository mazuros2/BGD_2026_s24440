from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from db_config import POSTGRES_URL, POSTGRES_PROPS, BRONZE_TABLE, SILVER_TABLE

DATE_FORMAT = "MM/dd/yyyy hh:mm:ss a"

def run_silver(spark, df_bronze):

    df = (
        df_bronze

        .filter(F.col("unique_key").isNotNull())
        .filter(F.col("created_date").isNotNull())

        .withColumn("request_id",
            F.col("unique_key").cast("bigint"))

        .withColumn("created_date",
            F.to_timestamp("created_date", DATE_FORMAT))

        .withColumn("closed_date",
            F.to_timestamp("closed_date", DATE_FORMAT))

        .withColumn("agency_code",
            F.upper(F.trim(F.col("agency"))))

        .withColumn("agency_name",
            F.initcap(F.trim(F.col("agency_name"))))

        .withColumn("complaint_type",
            F.initcap(F.trim(F.col("problem"))))

        .withColumn("descriptor",
            F.initcap(F.trim(F.col("problem_detail"))))

        .withColumn("borough",
            F.upper(F.trim(F.col("borough"))))

        .withColumn("city",
            F.initcap(F.trim(F.col("city"))))

        .withColumn("incident_zip",
            F.when(F.trim(F.col("incident_zip")) == "", None)
             .otherwise(F.trim(F.col("incident_zip"))))

        .withColumn("incident_address",
            F.initcap(F.trim(F.col("incident_address"))))

        .withColumn("status",
            F.upper(F.trim(F.col("status"))))

        .withColumn("channel",
            F.upper(F.trim(F.col("open_data_channel_type"))))

        .withColumn("latitude",
            F.when(F.col("latitude").cast("double") == 0, None)
             .otherwise(F.col("latitude").cast("double")))

        .withColumn("longitude",
            F.when(F.col("longitude").cast("double") == 0, None)
             .otherwise(F.col("longitude").cast("double")))

        .filter(
            F.col("closed_date").isNull() |
            (F.col("closed_date") > F.col("created_date"))
        )

        .withColumn(
            "row_num",
            F.row_number().over(
                Window
                .partitionBy("unique_key")
                .orderBy(F.col("created_date").asc())
            )
        )
        .filter(F.col("row_num") == 1)

        .select(
            "request_id",
            "created_date",
            "closed_date",
            "agency_code",
            "agency_name",
            "complaint_type",
            "descriptor",
            "borough",
            "city",
            "incident_zip",
            "incident_address",
            "status",
            "channel",
            "latitude",
            "longitude"
        )
    )

    (
        df.write
        .mode("append")
        .option("batchsize", "10000")
        .jdbc(
            url        = POSTGRES_URL,
            table      = SILVER_TABLE,
            properties = POSTGRES_PROPS
        )
    )

    return df

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("nyc_311_bronze")
        .config("spark.driver.memory", "4g")
        .config("spark.jars", "/opt/postgresql-jdbc.jar")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df_bronze = spark.read.jdbc(
        url        = POSTGRES_URL,
        table      = BRONZE_TABLE,
        properties = {
            "user":     "postgres",
            "password": "postgres",
            "driver":   "org.postgresql.Driver"
        }
    )

    run_silver(spark, df_bronze)
    spark.stop()