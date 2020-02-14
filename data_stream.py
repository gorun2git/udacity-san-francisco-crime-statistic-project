import logging
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf
from dateutil.parser import parse as parse_date

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

radio_code_schema = StructType([
    StructField("disposition_code", StringType(), True),
    StructField("description", StringType(), True)
])

udf_to_timestamp = psf.udf(lambda z: parse_date(z), TimestampType())


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
    
    spark.udf.register("udf_to_timestamp", udf_to_timestamp)
    
    # Show schema for the incoming resources for checks
    df.printSchema()
    
    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value AS STRING)")
    
    service_table = kafka_df \
        .select(psf.from_json(psf.col('value'), schema).alias("SERVICE_DF")) \
        .select("SERVICE_DF.*")
    
    distinct_table = service_table \
        .select(
        udf_to_timestamp(psf.col("call_date_time")).alias("call_date_time"),
        psf.col("original_crime_type_name"),
        psf.col("disposition")
    ).distinct()
    
    # count the number of original crime type
    agg_df = distinct_table \
        .withWatermark("call_date_time", "60 minutes") \
        .groupBy(
        psf.window(psf.col("call_date_time"), "60 minutes", "10 minutes"),
        psf.col("original_crime_type_name"),
        psf.col("disposition")
    ) \
        .count() \
        .orderBy("count", ascending=False)
    
    query = agg_df \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .queryName("Query 1 - Aggregate query") \
        .start()
    
    query.awaitTermination()
    
    radio_code_json_filepath = f"{Path(__file__).parents[0]}/radio_code.json"
    radio_code_df = spark \
        .read \
        .option("multiline", "true") \
        .schema(radio_code_schema) \
        .json(radio_code_json_filepath)
    
    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code
    
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")
    
    join_query = agg_df \
        .join(radio_code_df, "disposition", "left") \
        .writeStream \
        .format("console") \
        .outputMode("complete") \
        .option("truncate", "false") \
        .queryName("Query 2 - Join query") \
        .start()
    
    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .config("spark.ui.port", 3000) \
        .config("spark.default.parallelism", 8) \
        .config("spark.sql.shuffle.partitions", 8) \
        .config("spark.streaming.kafka.maxRatePerPartition", 250) \
        .appName("KafkaSparkStructuredStreaming") \
        .getOrCreate()
    
    logger.info("Spark started")
    
    run_spark_job(spark)
    
    spark.stop()
