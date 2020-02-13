import logging
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf

import utils
import config

schema = StructType([StructField("crime_id", StringType(), True),
                     StructField("original_crime_type_name", StringType(), True),
                     StructField("report_date", StringType(), True),
                     StructField("call_date", StringType(), True),
                     StructField("offense_date", StringType(), True),
                     StructField("call_time", StringType(), True),
                     StructField("call_date_time", StringType(), True),
                     StructField("disposition", StringType(), True),
                     StructField("address", StringType(), True),
                     StructField("city", StringType(), True),
                     StructField("state", StringType(), True),
                     StructField("agency_id", StringType(), True),
                     StructField("address_type", StringType(), True),
                     StructField("common_location", StringType(), True)
                     ])


def run_spark_job(spark):

    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    topic_name = utils.get_topic_name(config.INPUT_FILE_NAME)
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", config.BOOTSTRAP_SERVERS) \
        .option("subscribe", topic_name) \
        .option("startingOffsets", "earliest") \
        .option("maxRatePerPartition", 100) \
        .option("maxOffsetsPerTrigger", 100) \
        .option("stopGracefullyOnShutdown", "true") \
        .load()

    # Show schema for the incoming resources for checks
    df.printSchema()

    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value AS STRING)")

    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("SERVICE_DF"))\
        .select("SERVICE_DF.*")

    distinct_table = service_table \
        .select(  # psf.col("crime_id"),
            psf.to_timestamp(psf.col("call_date_time")).alias("call_date_time"),
            psf.col("original_crime_type_name"),
            psf.col("disposition")
    )

    # count the number of original crime type
    agg_df = distinct_table \
        .select(distinct_table.call_date_time,
                distinct_table.original_crime_type_name,
                distinct_table.disposition
                ).withWatermark("call_date_time", "60 minutes") \
        .groupBy(psf.window(distinct_table.call_date_time, "10 minutes", "5 minutes"),
                 psf.col("original_crime_type_name")
                ).count()

    query = agg_df \
            .writeStream \
            .outputMode("Complete") \
            .format("console") \
            .option("truncate", "false") \
            .start()

    query.awaitTermination()

    radio_code_json_filepath = f"{Path(__file__).parents[0]}/radio_code.json"
    radio_code_df = spark.read.json(radio_code_json_filepath)

    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    join_query = agg_df \
        .join(radio_code_df, "disposition") \
        .writeStream \
        .format("console") \
        .queryName("join_query") \
        .start()

    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .config("spark.ui.port", 3000) \
        .appName("KafkaSparkStructuredStreaming") \
        .getOrCreate()

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
