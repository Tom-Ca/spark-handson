import unittest
from pyspark.sql import Row
from src.fr.hymaia.exo2.spark_aggregate_job import pop_calculation
from tests.fr.hymaia.spark_test_case import spark



class test_exo_deux_aggregate_job(unittest.TestCase):
    def test_func_pop_calculation_which_calculates_the_population_by_department_with_specificity_of_Corsica(self):
        # Given
        data1 = [("Cussac", "27", "75020", "Paris", "75"),\
                ("Titi", "35", "20200", "Bastia", "2B"),\
                ("Toto", "18", "20150", "Bastia", "2A"),\
                ("Tata", "40", "75008", "Paris", "75"),\
                ("Papa", "45", "33120", "Arcahcon", "33")]

        columns1= ["name", "age", "zip", "city", "departements"]
        df_cli = spark.createDataFrame(data = data1, schema = columns1)

        true_output = [Row(departements='75', count=2),\
                        Row(departements='2A', count=1),\
                        Row(departements='2B', count=1),\
                        Row(departements='33', count=1)]
        
        # When
        df_output = pop_calculation(df_cli)
        
        # Then
        self.assertEqual(df_output.collect(), true_output)

    def test_departements_error_case(self):
        # Given
        data = [("Cussac", "27", "75020", "Paris", "75"),\
                ("Titi", "35", "20200", "Bastia", "2B"),\
                ("Toto", "18", "20150", "Bastia", "2A"),\
                ("Tata", "40", "75008", "Paris", "75"),\
                ("Papa", "45", "33120", "Arcahcon", "33")]

        columns= ["name", "age", "zip", "city", "departements"]
        df_cli = spark.createDataFrame(data = data, schema = columns)
        
        # When
        df_output = pop_calculation(df_cli)
        # Then
        self.assertRaises(TypeError, df_output.collect())

if __name__ == '__main__':
    unittest.main()