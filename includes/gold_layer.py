from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import *
import sys
from datetime import date, timedelta

def run_gold():

    spark = SparkSession \
        .builder \
        .appName("Brewery") \
        .getOrCreate()

    df = spark.read.parquet("/opt/airflow/silver/")

    df = df.withColumn('index',row_number().over(Window.partitionBy(col("id_brewery")).orderBy(col("dt_load").desc())))
    df = df.filter(df.index == 1) 
        
    df = df.drop("index")

    df.groupBy(df.brewery_type, df.name_state).count().show()

    df = df.groupBy(df.brewery_type, df.name_state).count()

    df = df.withColumnRenamed("count", "qty_brewery_type_and_location")

    today = date.today()

    df.write.format("parquet").mode("append")\
            .option("mergeSchema", "true").save(f'/opt/airflow/gold/day{today}')

run_gold()
