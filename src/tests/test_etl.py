import unittest
from unittest import mock
from src.etl.sparkjob import add_fields
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql import SparkSession
from freezegun import freeze_time

    
class SparkETLTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession
                     .builder
                     .master("local[*]")
                     .appName("Unit-tests")
                     .getOrCreate())

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        
    # to test data transformation we need to freeze time to "2023-01-01" and mock standar uuid to always retun fix uuid
    @mock.patch("src.etl.sparkjob.add_fields.uuid.uuid4")
    @freeze_time("2023-01-01")
    def test_etl(self, mock_uuid):
        mock_uuid.return_value  = '8377060215b0443b87041c799c413636'
        input_schema = StructType(
        [
            StructField('id', StringType(), True), 
            StructField('name', StringType(), True),
            StructField('age', StringType(), True), 
        ]
        )

        #1. Prepare an input data frame that mimics our source data.
        input_data = [(1, "Steve", 35)]
        input_df = self.spark.createDataFrame(data=input_data, schema=input_schema)

        #2. Prepare an expected data frame which is the output that we expect.    
        expected_schema = StructType([
            StructField('id', StringType(), True), 
            StructField('name', StringType(), True),
            StructField('age', StringType(), True), 
            StructField('timestamp', StringType(), True), 
            StructField('batch_uuid', StringType(), True), 
                ])
        expected_data = [(1, "Steve", 35, '2023-01-01 00:00:000', '8377060215b0443b87041c799c413636') ]
        expected_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)
        
        #3. Apply our transformation to the input data frame
        output_df = add_fields(input_df)               
        
        #4. Assert the output of the transformation to the expected data frame.
        self.assertEqual(sorted(expected_df.collect()), sorted(output_df.collect()))

if __name__ == '__main__':
    main = SparkETLTestCase()
    sys.path.append('/home/jovyan')
    # This executes the unit test/(itself)
    import sys
    suite = unittest.TestLoader().loadTestsFromTestCase(SparkETLTestCase)
    unittest.TextTestRunner(verbosity=4,stream=sys.stderr).run(suite)   