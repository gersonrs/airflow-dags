from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
)
from pyspark.sql.functions import (
    col,
    lit,
    round,
    mean,
    stddev,
    min,
    max,
    year,
    month,
    datediff,
    abs,
    count,
    first,
    concat_ws,
    current_timestamp,
)


if __name__ == "__main__":

    spark = (
        SparkSession.builder.config(
            "spark.hive.metastore.uris",
            "thrift://metastore-hive-hive-metastore.metastore.svc.cluster.local:9083",
        )
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hive")
        .getOrCreate()
    )

    log4j = spark._jvm.org.apache.log4j
    logger = log4j.LogManager.getLogger("app")

    logger.info(SparkConf().getAll())

    spark.sparkContext.setLogLevel("INFO")

    csv_path = "s3a://warehouse/cell_tower_sample.csv"

    schema = StructType(
        [
            StructField("radio", StringType(), True),
            StructField("mcc", IntegerType(), True),
            StructField("net", IntegerType(), True),
            StructField("area", IntegerType(), True),
            StructField("cell", IntegerType(), True),
            StructField("unit", IntegerType(), True),
            StructField("lon", DoubleType(), True),
            StructField("lat", DoubleType(), True),
            StructField("range", IntegerType(), True),
            StructField("samples", IntegerType(), True),
            StructField("changeable", IntegerType(), True),
            StructField("created", TimestampType(), True),
            StructField("updated", TimestampType(), True),
            StructField("averageSignal", IntegerType(), True),
        ]
    )

    csv_df = spark.read.csv(csv_path, header=True, schema=schema)

    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.bronze")

    csv_df = (
        csv_df.withColumn("ingestion_time", lit(current_timestamp()))
        .withColumn("source_system", lit("s3"))
        .withColumn("run_instance", lit("airflow"))
        .withColumn("ingestion_type", lit("spark"))
        .withColumn("base_format", lit("csv"))
        .withColumn("rows_written", lit(csv_df.count()))
        .withColumn("schema", lit(csv_df.schema.json()))
    )

    csv_df.write.mode("overwrite").saveAsTable("local.bronze.torres")

    csv_df.show()

    logger.info("Dados salvos com sucesso na tabela Iceberg")

    spark.stop()
