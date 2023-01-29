import pyspark.sql.functions as F
from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.getOrCreate()
    df_city = spark.read.options(delimiter=',', header=True, inferSchema='True').csv(
        f'src/resources/exo2/city_zipcode.csv')
    df_cli = spark.read.options(delimiter=',', header=True, inferSchema='True').csv(
        f'src/resources/exo2/clients_bdd.csv')

    df = application(df_cli, df_city)
    df.write.mode("overwrite").parquet(f'data/exo2/clean')
    
def add_departements(df):
    df2 = df.withColumn('departements', F.when((df.zip.cast('int') >= 20000) & (df.zip.cast('int') <= 20190),"2A")
                                        .when((df.zip.cast('int') > 20190) & (df.zip.cast('int') <= 20999),"2B")
                                        .otherwise(F.col('zip').substr(1, 2)))
    return df2

def join(df1, df2):
    return df1.join(df2,"zip","right")

def filter_age(df):
    return df.filter(df.age >= 18)
    
def application(df_cli, df_city):
    df_cli2 = filter_age(df=df_cli)
    df_join = join(df_cli2, df_city)
    return add_departements(df_join)
    