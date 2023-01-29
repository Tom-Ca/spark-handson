import pyspark.sql.functions as f
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.options(delimiter='\n', header=True, inferSchema='True').csv(
        f'src/resources/exo1/data.csv')
    df2 = wordcount(df,"text")
    df2.write.mode("overwrite").partitionBy("count").parquet(f'data/exo1/output')

def wordcount(df, col_name):
    return df.withColumn('word', f.explode(f.split(f.col(col_name), ' '))) \
        .groupBy('word') \
        .count()