import pyspark.sql.functions as f
from pyspark.sql import SparkSession
# from tests.fr.hymaia.spark_test_case import spark
from pyspark.sql.types import StringType
import time

def add_category_name_col_udf(str):
    arr = str.split(" ")
    for x in arr:
        if int(x) < 6:
            return "food" 
        else:
            return "furniture"

def add_category_name_col(df):
    convertUDF = f.udf(lambda z: add_category_name_col_udf(z),StringType())
    
    return df.withColumn("category_name", convertUDF(f.col("category"))) 
    

def main():
    spark = SparkSession.builder.getOrCreate()

    start_time = time.time()
    df = spark.read.option("header", "true").csv("src/resources/exo4/sell.csv")  
    
    result = add_category_name_col(df)
    result.coalesce(1).write.mode("overwrite").options(header='True', delimiter=',').csv(f'data/exo4/python_udf')
    
    print("--- %s seconds ---" % (time.time() - start_time))





