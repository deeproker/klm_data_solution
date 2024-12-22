import unittest
from src.data_loader import DataLoader
from pyspark.sql import SparkSession

class TestDataLoader(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Create a Spark session for testing
        cls.spark = SparkSession.builder \
            .appName("TestMainIntegration") \
            .master("local[*]") \
            .getOrCreate()
        cls.loader = DataLoader(
            airport_data_path="./data/airports/airports.dat",
            booking_data_path="./data/bookings/booking.json"
        )

    def test_load_airports(self):
        airport_df = self.loader.load_airports()
        self.assertIsNotNone(airport_df)
        self.assertGreater(airport_df.count(), 0)

    def test_load_bookings(self):
        bookings_df = self.loader.load_bookings()
        self.assertIsNotNone(bookings_df)
        self.assertGreater(bookings_df.count(), 0)

if __name__ == '__main__':
    unittest.main()