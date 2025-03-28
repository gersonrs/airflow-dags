# import libraries
from __future__ import annotations

from pyspark import SparkConf
from pyspark.sql import SparkSession

# main spark program
if __name__ == "__main__":
    # init session
    spark = SparkSession.builder.appName("BronzeIngestion").enableHiveSupport().getOrCreate()

    log4j = spark._jvm.org.apache.log4j
    logger = log4j.LogManager.getLogger("app")

    logger.info(SparkConf().getAll())

    # set log level
    spark.sparkContext.setLogLevel("INFO")

    spark.sql("DROP DATABASE uber")
    # spark.sql("CREATE DATABASE IF NOT EXISTS uber LOCATION 's3a://warehouse/uber'")

    # spark.sql("SHOW TABLES IN uber").show()

    base_path = "s3a://landing/"

    sources = {
        "users_mssql": "mssql/users/*.json",
        "users_mongo": "mongodb/users/*.json",
        "restaurants": "mysql/restaurants/*.json",
        "drivers": "postgres/drivers/*.json",
        "orders": "kafka/orders/*.json",
        "status": "kafka/status/*.json",
    }

    for entity, relative_path in sources.items():
        path = f"{base_path}{relative_path}"
        print(f"\nIniciando ingestão de {entity} do caminho: {path}")
        df = spark.read.json(path)
        df.write.format("delta").mode("overwrite").save(f"s3a://bronze/uber/{entity}")
        print(f"✓ Ingestão da entidade '{entity}' concluída.")
        df.printSchema()

    # stop session
    spark.stop()
