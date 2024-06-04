from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys
from datetime import date, timedelta

def run_silver ():

    spark = SparkSession \
        .builder \
        .appName("Brewery") \
        .getOrCreate()

    df = spark.read.json("/opt/airflow/bronze/data.json",multiLine=True)

    df = df.withColumnRenamed("address_1", "name_address_1") \
            .withColumnRenamed("address_2", "name_address_2") \
            .withColumnRenamed("address_3", "name_address_3") \
            .withColumnRenamed("street", "name_street") \
            .withColumnRenamed("city", "name_city") \
            .withColumnRenamed("country", "name_country") \
            .withColumnRenamed("name", "name_brewery") \
            .withColumnRenamed("id", "id_brewery") \
            .withColumnRenamed("state", "name_state") \
            .withColumnRenamed("state_province", "name_state_province") \
            .withColumnRenamed("phone", "phone_brewery") \
            .withColumnRenamed("latitude", "latitude_brewery") \
            .withColumnRenamed("longitude", "longitude_brewery") \
            .withColumnRenamed("postal_code", "postal_code_brewery")

    if df.filter(df.name_state.isNull()).count() >= 1 or df.filter(df.brewery_type.isNull()).count() >= 1:
        print("has null values")
        sys.exit()
    else:
        print("has no null values")

    today = date.today()

    df = df.withColumn("dt_load", lit(today))

    df.write.format("parquet").mode("append")\
            .partitionBy(["name_state"])\
            .option("mergeSchema", "true").save(f'/opt/airflow/silver/day{today}')


run_silver()