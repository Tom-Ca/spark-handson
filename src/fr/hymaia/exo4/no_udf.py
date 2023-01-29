import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from tests.fr.hymaia.spark_test_case import spark
from pyspark.sql.window import Window
import time

def add_category_name(df):
    return df.withColumn("category_name", f.when(df.category < 6, "food").otherwise("furniture"))

def price_per_category_per_day(df_category_name):
    windowSpec = Window.partitionBy("category_name", "date")
    df_price_per_category_per_day_tmp = df_category_name.withColumn("total_price_per_category_per_day", f.sum(f.col("price")).over(windowSpec))
    return df_price_per_category_per_day_tmp\
        .select("id", "date", "category_name", "total_price_per_category_per_day")
        

def price_per_category_per_day_last_30_days(df_price_per_category_per_day):
    windowSpec = Window.partitionBy("category_name").orderBy(f.col("date").asc()).rowsBetween(-30, 0)
    return df_price_per_category_per_day\
        .dropDuplicates((["date"]))\
        .withColumn("total_price_per_category_per_day_last_30_days", f.sum(f.col("total_price_per_category_per_day"))\
        .over(windowSpec))\
        .withColumnRenamed("total_price_per_category_per_day", "price")

def application(df):
    df_category_name = add_category_name(df)
    df_price_per_category_per_day = price_per_category_per_day(df_category_name)
    return price_per_category_per_day_last_30_days(df_price_per_category_per_day)

def main():
    start_time = time.time()
    df = spark.read.option("header", "true").csv("src/resources/exo4/sell.csv")
    df_category_name = add_category_name(df)
    df_category_name.coalesce(1).write.mode("overwrite").options(header='True', delimiter=',').csv(f'data/exo4/no_udf')
    print("--- %s seconds ---" % (time.time() - start_time))

    df_price_per_category_per_day = price_per_category_per_day(df_category_name)

    df_price_per_category_per_day_last_30_days = price_per_category_per_day_last_30_days(df_price_per_category_per_day)

    df_price_per_category_per_day_last_30_days.coalesce(1).write.mode("overwrite").options(header='True', delimiter=',').csv(f'data/exo4/no_udf_window_functions')
    print("--- %s seconds ---" % (time.time() - start_time))