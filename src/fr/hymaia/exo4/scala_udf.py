import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.column import Column, _to_java_column, _to_seq
import time
spark = SparkSession.builder.appName("exo4").config('spark.jars', 'src/resources/exo4/udf.jar').master("local[*]").getOrCreate()


def addCategoryName(col):
    # on récupère le SparkContext
    sc = spark.sparkContext
    # Via sc._jvm on peut accéder à des fonctions Scala
    add_category_name_udf = sc._jvm.fr.hymaia.sparkfordev.udf.Exo4.addCategoryNameCol()
    # On retourne un objet colonne avec l'application de notre udf Scala
    return Column(add_category_name_udf.apply(_to_seq(sc, [col], _to_java_column)))


def main():
    start_time = time.time()
    df = spark.read.option("header", "true").csv("src/resources/exo4/sell.csv")
    result = df.withColumn("category_name", addCategoryName(df.category))
    result.coalesce(1).write.mode("overwrite").options(header='True', delimiter=',').csv(f'data/exo4/scala_udf')

    print("--- %s seconds ---" % (time.time() - start_time))