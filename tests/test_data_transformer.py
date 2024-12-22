import unittest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from src.data_transformer import DataTransformer
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, DoubleType,ArrayType

class TestDataTransformer(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Set up a Spark session for testing
        # Create a Spark session for testing
        cls.spark = SparkSession.builder \
            .appName("TestMainIntegration") \
            .master("local[*]") \
            .getOrCreate()

    def setUp(self):
        # Create sample airport DataFrame
        self.airport_data = [
            Row(IATA='AMS', Country='Netherlands', Timezone=1),
            Row(IATA='EDI', Country='United Kingdom', Timezone=0)
        ]
        self.airport_df = self.spark.createDataFrame(self.airport_data)

        self.booking_schema = StructType([
            StructField("timestamp", StringType(), True),
            StructField("event", StructType([
            StructField("DataElement", StructType([
            StructField("travelrecord", StructType([
                StructField("creationDate", StringType(), True),
                StructField("passengersList", ArrayType(StructType([
                    StructField("age", StringType(), True),
                    StructField("uci", StringType(), True),
                    StructField("passengerType", StringType(), True)
                ])), True),
                StructField("productsList", ArrayType(StructType([
                    StructField("type", StringType(), True),
                    StructField("tattoo", StringType(), True),
                    StructField("bookingStatus", StringType(), True),
                    StructField("flight", StructType([
                        StructField("marketingAirline", StringType(), True),
                        StructField("marketingFlightNumber", StringType(), True),
                        StructField("originAirport", StringType(), True),
                        StructField("destinationAirport", StringType(), True),
                        StructField("departureDate", StringType(), True),
                        StructField("arrivalDate", StringType(), True),
                        StructField("operatingAirline", StringType(), True),
                        StructField("operatingFlightNumber", StringType(), True),
                    ]))
                    ]), True))
                  ]))
                ]))
                ]))
                ])
        # Create sample bookings DataFrame based on the provided JSON structure
        self.bookings_data = [
            {   'timestamp':'2019-03-17T13:59:55.995Z',
                'event':{
                    'DataElement': {
                        'travelrecord': {
                            'creationDate': '2019-02-04T22:01:00Z',
                            'purgeDateAmd': '2019-04-05T00:00:00Z',
                            'lastEotDate': '2019-03-17T13:59:00Z',
                            'envelopNumber': 15,
                            'nbPassengers': 1,
                            'isMarketingBlockspace': False,
                            'isTechnicalLastUpdater': False,
                            'passengersList': [{
                                'age': 23, 'uci': '20050BDF000A7E23', 'passengerType': 'ADT'
                            }],
                            'productsList': [{
                                'type': 'ns2:Segment',
                                'tattoo': '2',
                                'bookingStatus': 'CONFIRMED',
                                'flight': {
                                    'marketingAirline': 'KL',
                                    'marketingFlightNumber': '1289',
                                    'originAirport': 'AMS',
                                    'destinationAirport': 'EDI',
                                    'departureDate': '2019-04-01T16:15:00Z',
                                    'arrivalDate': '2019-04-01T16:40:00Z',
                                    'operatingAirline': 'KL',
                                    'operatingFlightNumber': '1289'
                                }
                            }]
                        }
                    }
                }
            },
            {
                'timestamp':'2019-03-17T13:59:55.995Z',
                'event':{
                    'DataElement': {
                        'travelrecord': {
                            'creationDate': '2019-02-04T22:01:00Z',
                            'purgeDateAmd': '2019-04-05T00:00:00Z',
                            'lastEotDate': '2019-03-17T13:59:00Z',
                            'envelopNumber': 15,
                            'nbPassengers': 1,
                            'isMarketingBlockspace': False,
                            'isTechnicalLastUpdater': False,
                            'passengersList': [{
                                'age': 30, 'uci': '20050BDF000A7E24', 'passengerType': 'ADT'
                            }],
                            'productsList': [{
                                'type': 'ns2:Segment',
                                'tattoo': '1',
                                'bookingStatus': 'CANCELLED',
                                'flight': {
                                    'marketingAirline': 'KL',
                                    'marketingFlightNumber': '1286',
                                    'originAirport': 'EDI',
                                    'destinationAirport': 'AMS',
                                    'departureDate': '2019-03-18T17:05:00Z',
                                    'arrivalDate': '2019-03-18T19:35:00Z',
                                    'operatingAirline': 'KL',
                                    'operatingFlightNumber': '1286'
                                }
                            }]
                        }
                    }
                }
            }
        ]

        self.bookings_df = self.spark.createDataFrame(self.bookings_data,self.booking_schema)
        # Initialize the DataTransformer
        self.transformer = DataTransformer(self.airport_df, self.bookings_df)

    def test_transform_bookings(self):
        start_date = '2019-04-01 00:00:00'
        end_date = '2019-04-01 23:59:59'
        transformed_df = self.transformer.transform_bookings(start_date, end_date)

        # Collect results to validate
        results = transformed_df.collect()

        # Check results
        self.assertEqual(len(results), 1)  # Only one booking should be confirmed
        self.assertEqual(results[0]['uci'], '20050BDF000A7E23')  # Verify the UCI of the passenger
        self.assertEqual(results[0]['passengerType'], 'ADT')  # Check passenger type
        self.assertEqual(results[0]['Country'], 'Netherlands')  # Ensure country is Netherlands

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

if __name__ == '__main__':
    unittest.main()