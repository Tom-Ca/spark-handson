import unittest
from pyspark.sql import Row
from src.fr.hymaia.exo4.no_udf import add_category_name, price_per_category_per_day, price_per_category_per_day_last_30_days, application
from tests.fr.hymaia.spark_test_case import spark



class test_exo_quatre_no_udf(unittest.TestCase):
    def test_func_add_category_name(self):
        # Given
        data = [("0", "2019-02-17", "6", "40.0"),\
                 ("1", "2015-10-01", "4", "69.0")]
        columns= ["id", "date", "category", "price"]
        df_cli = spark.createDataFrame(data = data, schema = columns)

        true_output = [Row(id="0", date="2019-02-17", category="6", price="40.0", category_name="furniture"),\
                        Row(id="1", date="2015-10-01", category="4", price="69.0", category_name="food")]
        
        # When
        df_output = add_category_name(df_cli)
        
        # Then
        self.assertEqual(df_output.collect(), true_output)

    def test_func_price_per_category_per_day(self):
        # Given
        data = [("0", "2019-02-17", "6", "40.0", "furniture"),\
                ("1", "2019-05-03", "7", "60.0", "furniture"),\
                ("2", "2019-05-03", "12", "32.0", "furniture"),\
                ("3", "2019-02-17", "4", "69.0", "food"),\
                ("4", "2019-02-17", "3", "12.0", "food"),\
                ("5", "2019-05-03", "2", "3.0", "food")]
        columns= ["id", "date", "category", "price", "category_name"]
        df_cli = spark.createDataFrame(data = data, schema = columns)

        true_output = [Row(id='3', date='2019-02-17', category_name='food', total_price_per_category_per_day=81.0),\
                        Row(id='4', date='2019-02-17', category_name='food', total_price_per_category_per_day=81.0),\
                        Row(id='5', date='2019-05-03', category_name='food', total_price_per_category_per_day=3.0),\
                        Row(id='0', date='2019-02-17', category_name='furniture', total_price_per_category_per_day=40.0),\
                        Row(id='1', date='2019-05-03', category_name='furniture', total_price_per_category_per_day=92.0),\
                        Row(id='2', date='2019-05-03', category_name='furniture', total_price_per_category_per_day=92.0)]
        
        # When
        df_output = price_per_category_per_day(df_cli)
        
        # Then
        self.assertEqual(df_output.collect(), true_output)

    def test_func_price_per_category_per_day_last_30_days(self):
        # Given
        data = [('3', '2019-02-17', 'food', 81.0),\
                ('4', '2019-02-17', 'food', 81.0),\
                ('5', '2019-05-03', 'food', 3.0),\
                ('0', '2019-02-17', 'furniture', 40.0),\
                ('1', '2019-05-03', 'furniture', 92.0),\
                ('2', '2019-05-03', 'furniture', 92.0)]
        columns= ["id", "date", "category_name", "total_price_per_category_per_day"]
        df_cli = spark.createDataFrame(data = data, schema = columns)

        true_output = [Row(id='3', date='2019-02-17', category_name='food',price=81.0, total_price_per_category_per_day_last_30_days=81.0),\
                       Row(id='5', date='2019-05-03', category_name='food', price=3.0, total_price_per_category_per_day_last_30_days=84.0)]
        
        # When
        df_output = price_per_category_per_day_last_30_days(df_cli)
        
        # Then
        self.assertEqual(df_output.collect(), true_output)

    def test_integration_with_func_application(self):
            # Given
            data = [("0", "2019-02-17", "6", "40.0"),\
                ("1", "2019-05-03", "7", "60.0"),\
                ("2", "2019-05-03", "12", "32.0"),\
                ("3", "2019-02-17", "4", "69.0"),\
                ("4", "2019-02-17", "3", "12.0"),\
                ("5", "2019-05-03", "2", "3.0")]
            columns= ["id", "date", "category", "price"]
            df_cli = spark.createDataFrame(data = data, schema = columns)

            true_output = [Row(id='3', date='2019-02-17', category_name='food',price=81.0, total_price_per_category_per_day_last_30_days=81.0),\
                           Row(id='5', date='2019-05-03', category_name='food', price=3.0, total_price_per_category_per_day_last_30_days=84.0)]
            
            # When
            df_output = application(df_cli)
            
            # Then
            self.assertEqual(df_output.collect(), true_output)
if __name__ == '__main__':
    unittest.main()