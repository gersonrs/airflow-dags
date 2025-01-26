# import libraries
# get file size in python
from __future__ import annotations

from os.path import abspath

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import lit


def get_path_size(df: DataFrame) -> int:
    return (
        spark._jsparkSession.sessionState()
        .executePlan(df._jdf.queryExecution().logical(), df._jdf.queryExecution().mode())
        .optimizedPlan()
        .stats()
        .sizeInBytes()
    )


# set default location for warehouse
warehouse_location = abspath("spark-warehouse")

# main spark program
if __name__ == "__main__":
    # init session
    spark = (
        SparkSession.builder.appName("ingestion-from-local-data-file-to-bronze-tables")
        .config("spark.sql.warehouse.dir", abspath("spark-warehouse"))
        .enableHiveSupport()
        .getOrCreate()
    )

    log4j = spark._jvm.org.apache.log4j
    logger = log4j.LogManager.getLogger("app")

    logger.info(SparkConf().getAll())

    # set log level
    spark.sparkContext.setLogLevel("INFO")

    # set dynamic input file [hard-coded]
    # can be changed for input parameters [spark-submit]
    get_users_file = "s3a://landing/user/"
    get_subscription_file = "s3a://landing/subscription/"
    get_credit_card_file = "s3a://landing/credit_card/"
    get_movies_file = "s3a://landing/movies/"

    # read user data
    df_user = (
        spark.read.format("json")
        .option("inferSchema", "true")
        .option("header", "true")
        .json(get_users_file)
    )

    df_user.printSchema()

    df_user = (
        df_user.withColumn("ingestion_time", lit(current_timestamp()))
        .withColumn("source_system", lit("local"))
        .withColumn("user_name", lit("gersonrs"))
        .withColumn("ingestion_type", lit("spark"))
        .withColumn("base_format", lit("json"))
        .withColumn("file_size", lit(get_path_size(df_user)))
        .withColumn("rows_written", lit(df_user.count()))
        .withColumn("schema", lit(df_user.schema.json()))
    )

    # read subscription data
    df_subscription = (
        spark.read.format("json")
        .option("inferSchema", "true")
        .option("header", "true")
        .json(get_subscription_file)
    )

    df_subscription.printSchema()

    df_subscription = (
        df_subscription.withColumn("ingestion_time", lit(current_timestamp()))
        .withColumn("source_system", lit("local"))
        .withColumn("user_name", lit("gersonrs"))
        .withColumn("ingestion_type", lit("spark"))
        .withColumn("base_format", lit("json"))
        .withColumn("file_size", lit(get_path_size(df_subscription)))
        .withColumn("rows_written", lit(df_subscription.count()))
        .withColumn("schema", lit(df_subscription.schema.json()))
    )

    # read credit card data
    df_credit_card = (
        spark.read.format("json")
        .option("inferSchema", "true")
        .option("header", "true")
        .json(get_credit_card_file)
    )

    df_credit_card.printSchema()

    df_credit_card = (
        df_credit_card.withColumn("ingestion_time", lit(current_timestamp()))
        .withColumn("source_system", lit("local"))
        .withColumn("user_name", lit("gersonrs"))
        .withColumn("ingestion_type", lit("spark"))
        .withColumn("base_format", lit("json"))
        .withColumn("file_size", lit(get_path_size(df_credit_card)))
        .withColumn("rows_written", lit(df_credit_card.count()))
        .withColumn("schema", lit(df_credit_card.schema.json()))
    )

    # read movies data
    df_movies = (
        spark.read.format("json")
        .option("inferSchema", "true")
        .option("header", "true")
        .json(get_movies_file)
    )

    df_movies.printSchema()

    df_movies = (
        df_movies.withColumn("ingestion_time", lit(current_timestamp()))
        .withColumn("source_system", lit("local"))
        .withColumn("user_name", lit("gersonrs"))
        .withColumn("ingestion_type", lit("spark"))
        .withColumn("base_format", lit("json"))
        .withColumn("file_size", lit(get_path_size(df_movies)))
        .withColumn("rows_written", lit(df_movies.count()))
        .withColumn("schema", lit(df_movies.schema.json()))
    )

    # write into parquet file on bronze zone
    # file to be available for virtualization engine
    # using minio as storage inside of [k8s]

    df_user.write.format("delta").mode("overwrite").save("s3a://bronze/users/")
    df_subscription.write.format("delta").mode("overwrite").save("s3a://bronze/subscriptions/")
    df_credit_card.write.format("delta").mode("overwrite").save("s3a://bronze/credit_cards/")
    df_movies.write.format("delta").mode("overwrite").save("s3a://bronze/movies/")

    df_user.printSchema()
    df_subscription.printSchema()
    df_credit_card.printSchema()
    df_movies.printSchema()

    # stop session
    spark.stop()
