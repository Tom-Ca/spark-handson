import pyspark.sql.functions as F
from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.getOrCreate()
    df_city = spark.read.options(delimiter=',', header=True, inferSchema='True').csv(
        f'src/resources/exo2/city_zipcode.csv')
    df_cli = spark.read.options(delimiter=',', header=True, inferSchema='True').csv(
        f'src/resources/exo2/clients_bdd.csv')
    #df_city.show()
    #df_cli.show()
    df_cli2 = filter(df=df_cli)
    df_join = join(df_cli2, df_city)
    df_join.write.mode("overwrite").parquet(f'data/exo2/output')
    df_dep = add_departements(df_join)
    df_stat = calcul_pop(df_dep)
    df_stat.coalesce(1).write.mode("overwrite").options(header='True', delimiter=',').csv(f'data/exo2/aggregate')


def calcul_pop(df):
    return df.groupBy("departements") \
             .count() \
             .orderBy(F.col("count").desc(),F.col("departements").asc())

def add_departements(df):
    df.show()
    df2 = df.withColumn('departements', F.col('zip').substr(1, 2))
    df2.write.mode("overwrite").parquet(f'data/exo2/output')
    return df2

def join(df1, df2):
    #df1.join(df2,"zip","left").show()
    return df1.join(df2,"zip","right")

def filter(df):
    # df.filter(df.age >= 18).show()
    return df.filter(df.age >= 18)
    