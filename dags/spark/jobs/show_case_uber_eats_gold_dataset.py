# import libraries
from __future__ import annotations

import random

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql.functions import lit
from pyspark.sql.functions import udf

# main spark program
if __name__ == "__main__":
    # init session
    spark = SparkSession.builder.appName("GoldDataset").enableHiveSupport().getOrCreate()

    # show configured parameters
    print(SparkConf().getAll())

    # set log level
    spark.sparkContext.setLogLevel("INFO")

    spark.sql("SHOW TABLES IN uber").show()

    df = spark.table("uber.silver_deliveries")

    @udf("int")
    def age_from_birth(date_birth: str) -> int | None:
        return 2025 - int(date_birth.split("-")[0]) if date_birth else None

    @udf("double")
    def random_rating() -> float:
        return round(random.uniform(3.5, 5.0), 1)

    @udf("string")
    def random_weather() -> str:
        return random.choice(["Sunny", "Stormy", "Foggy", "Windy", "Cloudy"])

    @udf("string")
    def random_traffic() -> str:
        return random.choice(["Low", "Medium", "High", "Jam"])

    @udf("int")
    def random_condition() -> int:
        return random.randint(0, 2)

    @udf("int")
    def random_multi() -> int:
        return random.randint(0, 3)

    @udf("string")
    def random_type_of_order() -> str:
        return random.choice(["Meal", "Snack", "Drinks", "Buffet"])

    @udf("string")
    def random_city_type() -> str:
        return random.choice(["Urban", "Metropolitian"])

    # Enriquecer com features
    df = (
        df.withColumn("Delivery_person_Age", 2025 - expr("substring(date_birth, 1, 4)").cast("int"))
        .withColumnRenamed("order_id", "ID")
        .withColumnRenamed("driver_key", "Delivery_person_ID")
        .withColumnRenamed("delivery_latitude", "Delivery_location_latitude")
        .withColumnRenamed("delivery_longitude", "Delivery_location_longitude")
        .withColumnRenamed("restaurant_latitude", "Restaurant_latitude")
        .withColumnRenamed("restaurant_longitude", "Restaurant_longitude")
        .withColumnRenamed("order_date", "Order_Date")
        .withColumnRenamed("order_time", "Time_Orderd")
        .withColumnRenamed("pickup_time", "Time_Order_picked")
        .withColumnRenamed("vehicle_type", "Type_of_vehicle")
        .withColumnRenamed("vehicle_condition", "Vehicle_condition")
        .withColumn("Weatherconditions", random_weather())
        .withColumn("Road_traffic_density", random_traffic())
        .withColumn("multiple_deliveries", random_multi())
        .withColumn("Type_of_order", random_type_of_order())
        .withColumn("Festival", lit("No"))
        .withColumn("City", random_city_type())
    )

    # Selecionar e reordenar colunas no formato desejado
    df_gold = df.select(
        "ID",
        "Delivery_person_ID",
        "Delivery_person_Age",
        "Delivery_person_Ratings",
        "Restaurant_latitude",
        "Restaurant_longitude",
        "Delivery_location_latitude",
        "Delivery_location_longitude",
        "Order_Date",
        "Time_Orderd",
        "Time_Order_picked",
        "Weatherconditions",
        "Road_traffic_density",
        "Vehicle_condition",
        "Type_of_order",
        "Type_of_vehicle",
        "multiple_deliveries",
        "Festival",
        "City",
        "Time_taken",
    )

    # Salvar no Hive como tabela Delta
    df_gold.write.format("delta").mode("overwrite").saveAsTable("uber.gold_delivery_ml_ready")

    # Persistindo em Parquet (modo overwrite para sobrescrever se já existir)
    df_gold.write.mode("overwrite").parquet("s3a://gold/delivery_dataset/")

    # Persistindo em CSV (com header, separador padrão, e sobrescrevendo também)
    # df_gold.toPandas().to_csv(
    #     "s3://gold/delivery_dataset_csv/uber_gold_delivery_ml_ready.csv", index=False
    # )

    # stop session
    spark.stop()
