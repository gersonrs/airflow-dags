# import libraries
from __future__ import annotations

from pyspark import SparkConf
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import expr
from pyspark.sql.functions import floor
from pyspark.sql.functions import rand
from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import unix_timestamp

# main spark program
if __name__ == "__main__":
    # init session
    spark = (
        SparkSession.builder.appName("GeocodingUsersRestaurants").enableHiveSupport().getOrCreate()
    )

    # show configured parameters
    print(SparkConf().getAll())

    # set log level
    spark.sparkContext.setLogLevel("INFO")

    spark.sql("SHOW TABLES IN uber").show()

    # Função auxiliar para simular latitude e longitude dentro dos limites de São Paulo
    def generate_coordinates(df: DataFrame, lat_col: str, lon_col: str) -> DataFrame:
        return df.withColumn(
            lat_col, expr("rand() * (23.682160 - 23.501530) + -23.682160")
        ).withColumn(lon_col, expr("rand() * (46.754930 - 46.365570) + -46.754930"))

    # Simular localização dos usuários
    users = spark.table("uber.bronze_users_mssql")
    users = generate_coordinates(users, "delivery_latitude", "delivery_longitude")

    # Simular localização dos restaurantes
    restaurants = spark.table("uber.bronze_restaurants")
    restaurants = generate_coordinates(restaurants, "restaurant_latitude", "restaurant_longitude")

    orders = spark.table("uber.bronze_orders")
    status = spark.table("uber.bronze_status")
    drivers = spark.table("uber.bronze_drivers")

    # Processamento dos status para obter tempos de ordem e coleta
    status_flat = status.select(
        col("order_identifier"),
        col("status.status_name").alias("status_name"),
        col("status.timestamp").alias("timestamp_raw"),
    )

    status_final = status_flat.withColumn(
        "timestamp", (col("timestamp_raw") / 1000).cast("timestamp")
    )

    pivot = (
        status_final.groupBy("order_identifier")
        .pivot("status_name", ["Order Placed", "Picked Up"])
        .agg({"timestamp": "max"})
        .withColumnRenamed("Order Placed", "order_placed")
        .withColumnRenamed("Picked Up", "order_picked")
    )

    drivers = drivers.selectExpr(
        "license_number as driver_id_key", "vehicle_type", "vehicle_year", "date_birth"
    ).withColumn("Delivery_person_Age", (2025 - col("date_birth").substr(1, 4).cast("int")))

    # Simula a nota de cada entregador (como dado ausente na base original)
    drivers = drivers.withColumn("Delivery_person_Ratings", (rand() * 2 + 3).cast("double"))

    # Simula a condicao do veiculo de 0 a 2
    drivers = drivers.withColumn("Vehicle_condition", floor(rand() * 3))

    # Preparando users e restaurantes
    users = users.selectExpr("cpf", "delivery_latitude", "delivery_longitude")

    # Restaurantes
    restaurants = restaurants.selectExpr("cnpj", "restaurant_latitude", "restaurant_longitude")

    # Join dos dados
    orders_filtered = (
        orders.alias("o")
        .join(pivot.alias("s"), col("o.order_id") == col("s.order_identifier"))
        .join(drivers.alias("d"), col("o.driver_key") == col("d.driver_id_key"), "left")
        .join(users.alias("u"), col("o.user_key") == col("u.cpf"), "left")
        .join(restaurants.alias("r"), col("o.restaurant_key") == col("r.cnpj"), "left")
    )

    # Filtro
    df = orders_filtered.filter("order_placed IS NOT NULL AND order_picked IS NOT NULL")

    # Conversoes e calculo de tempo de entrega
    df = (
        df.withColumn("order_time", to_timestamp("order_placed"))
        .withColumn("pickup_time", to_timestamp("order_picked"))
        .withColumn(
            "Time_taken", (unix_timestamp("pickup_time") - unix_timestamp("order_time")) / 60
        )
    )

    # Persistencia
    columns_to_keep = [
        "order_id",
        "driver_key",
        "user_key",
        "restaurant_key",
        "order_date",
        "payment_id",
        "total_amount",
        "order_identifier",
        "order_placed",
        "order_picked",
        "driver_id_key",
        "vehicle_type",
        "vehicle_year",
        "date_birth",
        "delivery_latitude",
        "delivery_longitude",
        "restaurant_latitude",
        "restaurant_longitude",
        "order_time",
        "pickup_time",
        "Time_taken",
        "Delivery_person_Age",
        "Delivery_person_Ratings",
        "Vehicle_condition",
    ]

    df = df.select(*columns_to_keep)

    df.printSchema()
    df.show(5)

    # Salvar como camada silver consolidada
    df.write.format("delta").mode("overwrite").saveAsTable("uber.silver_deliveries")

    # stop session
    spark.stop()
