import pyspark.sql.functions as F
from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.getOrCreate()
    df_init = spark.read.options(header=True, inferSchema='True').parquet(
        f'data/exo2/clean')
    df_stat = pop_calculation(df_init)
    df_stat.coalesce(1).write.mode("overwrite").options(header='True', delimiter=',').csv(f'data/exo2/aggregate')


def pop_calculation(df):
    return df.groupBy("departements") \
             .count() \
             .orderBy(F.col("count").desc(),F.col("departements").asc())

    