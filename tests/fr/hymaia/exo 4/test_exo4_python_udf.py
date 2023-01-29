import unittest
from pyspark.sql import Row
from src.fr.hymaia.exo4.python_udf import add_category_name_col
from tests.fr.hymaia.spark_test_case import spark



class test_exo_quatre_python_udf(unittest.TestCase):
    def test_func_add_category_name(self):
        # Given
        data = [("0", "2019-02-17", "6", "40.0"),\
                 ("1", "2015-10-01", "4", "69.0")]
        columns= ["id", "date", "category", "price"]
        df_cli = spark.createDataFrame(data = data, schema = columns)

        true_output = [Row(id="0", date="2019-02-17", category="6", price="40.0", category_name="furniture"),\
                        Row(id="1", date="2015-10-01", category="4", price="69.0", category_name="food")]
        
        # When
        df_output = add_category_name_col(df_cli)
        
        # Then
        self.assertEqual(df_output.collect(), true_output)

if __name__ == '__main__':
    unittest.main()