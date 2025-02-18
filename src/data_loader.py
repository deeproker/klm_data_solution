from src.spark_session_manager import SparkSessionManager
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, DoubleType, ArrayType, BooleanType
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataLoader:
    def __init__(self, airport_data_path, booking_data_path):
        self.spark = SparkSessionManager.get_spark_session()  # Get the singleton Spark session
        self.airport_data_path = airport_data_path
        self.booking_data_path = booking_data_path
        
        

        # Define the airport schema
        self.airport_schema = StructType([
            StructField("Airport ID", IntegerType(), True),
            StructField("Name", StringType(), True),
            StructField("City", StringType(), True),
            StructField("Country", StringType(), True),
            StructField("IATA", StringType(), True),
            StructField("ICAO", StringType(), True),
            StructField("Latitude", DoubleType(), True),
            StructField("Longitude", DoubleType(), True),
            StructField("Altitude", IntegerType(), True),
            StructField("Timezone", IntegerType(), True),
            StructField("DST", StringType(), True),
            StructField("Tz Database time zone", StringType(), True),
            StructField("Type", StringType(), True),
            StructField("Source", StringType(), True)
        ])

        # Define the booking schema
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

    # Method to load the airports data
    def load_airports(self):
        try:
            logger.info("Loading airport data from: %s", self.airport_data_path)
            airports_df = self.spark.read.csv(
                self.airport_data_path,
                header=False,
                schema=self.airport_schema,
                encoding="UTF-8",
                mode="PERMISSIVE"
            )
            logger.info("Successfully loaded airport data.")
            return airports_df
        except Exception as e:
            logger.error("Error loading airport data: %s", e, exc_info=True)
            return None

    # Method to load the bookings data
    def load_bookings(self):
        try:
            logger.info("Loading booking data from: %s", self.booking_data_path)
            bookings_df = self.spark.read.json(
                self.booking_data_path,
                schema=self.booking_schema,
                mode="PERMISSIVE"
            )
            logger.info("Successfully loaded booking data.")
            return bookings_df
        except Exception as e:
            logger.error("Error loading bookings data: %s", e, exc_info=True)
            return None
