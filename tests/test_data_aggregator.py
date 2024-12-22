import unittest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import DataFrame
from src.data_aggregator import DataAggregator

class TestDataAggregator(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Set up a Spark session for testing
        # Create a Spark session for testing
        cls.spark = SparkSession.builder \
            .appName("TestMainIntegration") \
            .master("local[*]") \
            .getOrCreate()

    def setUp(self):
        # Create sample transformed DataFrame
        self.transformed_data = [
            Row(Country='Netherlands', day_of_week='Mon', season='Winter', passengerType='ADT', flight_leg_id='leg1'),
            Row(Country='Netherlands', day_of_week='Mon', season='Winter', passengerType='CHD', flight_leg_id='leg2'),
            Row(Country='Netherlands', day_of_week='Mon', season='Winter', passengerType='ADT', flight_leg_id='leg3'),
            Row(Country='United Kingdom', day_of_week='Tue', season='Winter', passengerType='ADT', flight_leg_id='leg4'),
            Row(Country='United Kingdom', day_of_week='Tue', season='Winter', passengerType='CHD', flight_leg_id='leg5'),
            Row(Country='Netherlands', day_of_week='Tue', season='Winter', passengerType='ADT', flight_leg_id='leg6'),
        ]
        self.transformed_df = self.spark.createDataFrame(self.transformed_data)

        # Initialize the DataAggregator
        self.aggregator = DataAggregator(self.transformed_df)

    def test_aggregate_passengers(self):
        # Perform aggregation
        aggregated_df = self.aggregator.aggregate_passengers()

        # Collect results to validate
        results = aggregated_df.collect()

        # Expected output
        expected_output = [
            Row(Country='Netherlands', day_of_week='Mon', season='Winter', Adt_count=2, Chd_count=1, total_passenger_count=3),
            Row(Country='United Kingdom', day_of_week='Tue', season='Winter', Adt_count=1, Chd_count=1, total_passenger_count=2),
            Row(Country='Netherlands', day_of_week='Tue', season='Winter', Adt_count=1, Chd_count=0, total_passenger_count=1),
        ]

        # Check length of results
        self.assertEqual(len(results), len(expected_output))
        
        # Check each output against the expected output
        for result, expected in zip(sorted(results, key=lambda x: (x.Country, x.day_of_week)), 
                                     sorted(expected_output, key=lambda x: (x.Country, x.day_of_week))):
            self.assertEqual(result, expected)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

if __name__ == '__main__':
    unittest.main()