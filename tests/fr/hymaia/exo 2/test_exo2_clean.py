import unittest
from pyspark.sql import Row
from src.fr.hymaia.exo2.spark_clean_job import filter_age, join, add_departements, application
from tests.fr.hymaia.spark_test_case import spark



class test_exo_deux_clean_job(unittest.TestCase):
    # spark = SparkSession.builder.getOrCreate()
    def test_func_filter_with_eighteen(self):
        # Given
        data = [("Cussac","27","75020"),\
                 ("Cussac","17","75020"),\
                 ("Cussac","18","75020")]
        columns= ["name","age","zip"]
        df_cli = spark.createDataFrame(data = data, schema = columns)

        true_output = [Row(name='Cussac', age='27', zip='75020'),\
                        Row(name='Cussac', age='18', zip='75020')]
        
        # When
        df_output = filter_age(df_cli)
        
        # Then
        self.assertEqual(df_output.collect(), true_output)

    def test_func_join(self):
        # Given
        data1 = [("Cussac","27","75020")]
        columns1= ["name","age","zip"]
        df_cli = spark.createDataFrame(data = data1, schema = columns1)

        data2 = [("75020","Paris")]
        columns2= ["zip","city"]
        df_city = spark.createDataFrame(data = data2, schema = columns2)

        true_output = [Row(zip='75020', name='Cussac', age='27', city='Paris')]
        
        # When
        df_output = join(df_cli, df_city)
        
        # Then
        self.assertEqual(df_output.collect(), true_output)

    def test_departements_with_around_two_thousand(self):
        # Given
        data1 = [("75020", "Cussac","27","VILLE DU PONT"),\
                 ("20020", "Cussac2","27","VILLE DU PONT2"),\
                 ("20888", "Cussac3","27","VILLE DU PONT3")]
        columns1= ["zip", "name","age","city"]
        df_cli = spark.createDataFrame(data = data1, schema = columns1)

        true_output = [Row(zip='75020', name='Cussac', age='27', city='VILLE DU PONT', departements='75'),\
                        Row(zip='20020', name='Cussac2', age='27', city='VILLE DU PONT2', departements='2A'),\
                        Row(zip='20888', name='Cussac3', age='27', city='VILLE DU PONT3', departements='2B')]
        
        # When
        df_output = add_departements(df_cli)
        
        # Then
        self.assertEqual(df_output.collect(), true_output)

    def test_integration_with_func_application(self):
        data1 = [("Cussac","27","75020"),\
                 ("Cussac","17","75020")]
        columns1= ["name","age","zip"]
        df_cli = spark.createDataFrame(data = data1, schema = columns1)

        data2 = [("75020","Paris")]
        columns2= ["zip","city"]
        df_city = spark.createDataFrame(data = data2, schema = columns2)

        true_output = [Row(zip='75020', name='Cussac', age='27', city='Paris', departements='75')]
        
        # When
        df_output = application(df_cli, df_city)
        
        # Then
        self.assertEqual(df_output.collect(), true_output)

if __name__ == '__main__':
    unittest.main()