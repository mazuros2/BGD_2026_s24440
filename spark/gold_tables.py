from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from db_config import POSTGRES_URL, POSTGRES_PROPS
from spark.db_config import SILVER_TABLE


def run_gold_tables(spark, df_silver):

    df_date = (
        df_silver
        .select("created_date")
        .distinct()
        .withColumn("date_key",
            F.date_format("created_date", "yyyyMMdd").cast("int"))
        .withColumn("full_date",
            F.to_date("created_date"))
        .withColumn("year",
            F.year("created_date").cast("int"))
        .withColumn("month",
            F.month("created_date").cast("int"))
        .withColumn("quarter",
            F.quarter("created_date").cast("int"))
        .withColumn("day_of_week",
            F.date_format("created_date", "EEEE"))
        .withColumn("is_weekend",
            F.dayofweek("created_date").isin([1, 7]))
        .withColumn("season",
            F.when(F.month("created_date").isin([12, 1, 2]), "Winter")
             .when(F.month("created_date").isin([3,  4, 5]), "Spring")
             .when(F.month("created_date").isin([6,  7, 8]), "Summer")
             .otherwise("Fall"))
        .drop("created_date")
    )

    df_date.write.mode("append").option("batchsize", "10000").jdbc(
        POSTGRES_URL, "gold.date", properties=POSTGRES_PROPS
    )

    df_agency = (
        df_silver
        .select("agency_code", "agency_name")
        .distinct()
        .withColumn("agency_key",
            F.row_number().over(
                Window.orderBy("agency_code")
            ))
        .withColumn("agency_type",
            F.when(F.col("agency_code").isin("NYPD", "FIRE", "DOC"), "Public Safety")
             .when(F.col("agency_code").isin("DOT", "DEP", "DSNY"),  "Infrastructure")
             .when(F.col("agency_code").isin("HPD", "DOB"),          "Housing")
             .when(F.col("agency_code") == "DCA",                    "Consumer Affairs")
             .when(F.col("agency_code") == "DOHMH",                  "Health")
             .when(F.col("agency_code") == "DPR",                    "Parks")
             .when(F.col("agency_code") == "TLC",                    "Transportation")
             .otherwise("Other City Agency"))
    )

    df_agency.write.mode("append").option("batchsize", "10000").jdbc(
        POSTGRES_URL, "gold.agency", properties=POSTGRES_PROPS
    )

    df_location = (
        df_silver
        .filter(F.col("borough").isNotNull())
        .filter(F.col("borough") != "UNSPECIFIED")
        .groupBy("borough", "incident_zip", "city")
        .agg(
            F.avg("latitude").alias("latitude"),
            F.avg("longitude").alias("longitude")
        )
        .withColumn("location_key",
            F.row_number().over(
                Window.orderBy("borough", "incident_zip")
            ))
    )

    df_location.write.mode("append").option("batchsize", "10000").jdbc(
        POSTGRES_URL, "gold.location", properties=POSTGRES_PROPS
    )

    df_complaint = (
        df_silver
        .select("complaint_type")
        .distinct()
        .withColumn("complaint_key",
            F.row_number().over(
                Window.orderBy("complaint_type")
            ))
        .withColumn("category",
            F.when(F.col("complaint_type").rlike("(?i)noise"),              "Noise")
             .when(F.col("complaint_type").rlike("(?i)heat|plumbing|paint|mold"), "Housing Maintenance")
             .when(F.col("complaint_type").rlike("(?i)parking|traffic|bike"),     "Transportation")
             .when(F.col("complaint_type").rlike("(?i)rodent|sanit|dirty|trash"), "Sanitation")
             .when(F.col("complaint_type").rlike("(?i)drug|assault|illegal"),     "Public Safety")
             .when(F.col("complaint_type").rlike("(?i)tree|park"),                "Parks")
             .when(F.col("complaint_type").rlike("(?i)water|sewer"),              "Water & Sewer")
             .otherwise("Other"))
    )

    df_complaint.write.mode("append").option("batchsize", "10000").jdbc(
        POSTGRES_URL, "gold.complaint", properties=POSTGRES_PROPS
    )

    status_data = [
        (1, "CLOSED",      "Closed",      True,  False),
        (2, "OPEN",        "Open",        False, True),
        (3, "PENDING",     "Pending",     False, True),
        (4, "IN PROGRESS", "In Progress", False, True),
        (5, "ASSIGNED",    "Assigned",    False, True),
        (6, "UNSPECIFIED", "Unspecified", False, False),
    ]

    df_status = spark.createDataFrame(
        status_data,
        ["status_key", "status_code", "status_label", "is_resolved", "is_pending"]
    )

    return df_date, df_agency, df_location, df_complaint, df_status

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("nyc_311_bronze")
        .config("spark.driver.memory", "4g")
        .config("spark.jars", "/opt/postgresql-jdbc.jar")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df_silver = spark.read.jdbc(
        url        = POSTGRES_URL,
        table      = SILVER_TABLE,
        properties = POSTGRES_PROPS
    )

    run_gold_tables(spark, df_silver)
    spark.stop()