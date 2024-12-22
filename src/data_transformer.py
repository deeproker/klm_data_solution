from pyspark.sql import DataFrame
from pyspark.sql import functions as F

class DataTransformer:
    def __init__(self, airport_df:DataFrame, bookings_df:DataFrame):
        self.airport_df = airport_df
        self.bookings_df = bookings_df

    # Method to explode the passenger and product list and filter by start and end date provided by user input
    def transform_bookings(self, start_date: str, end_date: str) -> DataFrame:
        bookings_cols_df = self.bookings_df.select(
            F.col("timestamp"),
            F.explode(F.col("event.DataElement.travelrecord.passengerslist")).alias("exploded_passenger"),
            F.explode(F.col("event.DataElement.travelrecord.productsList")).alias("exploded_product")
        )

        bookings_cols_df = bookings_cols_df.select(
            F.col("exploded_product.bookingStatus"),
            F.col("exploded_product.flight.operatingAirline"),
            F.col("exploded_product.flight.originAirport"),
            F.col("exploded_product.flight.destinationAirport"),
            F.col("exploded_product.flight.departureDate"),
            F.col("exploded_product.flight.arrivalDate"),
            F.col("exploded_passenger.uci"),
            F.col("exploded_passenger.passengerType"),
            F.col("exploded_passenger.age")
        ).filter((F.col("operatingAirline") == 'KL') & (F.col("bookingStatus") == 'CONFIRMED'))

        return bookings_cols_df.join(
            self.airport_df,
            bookings_cols_df.originAirport == self.airport_df.IATA,
            "left"
        ).filter(F.col("Country") == "Netherlands").withColumn(
            "localDepartureDate",
            F.to_timestamp(F.col("departureDate"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
        ).withColumn(
            "localDepartureTimestamp",
            F.col("localDepartureDate") + F.expr("INTERVAL 1 HOURS") * F.col("Timezone")
        ).withColumn(
            "day_of_week",
            F.date_format("localDepartureTimestamp", "E")).withColumn(
            "season",
            F.when(F.month("localDepartureTimestamp").isin([12, 1, 2]), "Winter")
             .when(F.month("localDepartureTimestamp").isin([3, 4, 5]), "Spring")
             .when(F.month("localDepartureTimestamp").isin([6, 7, 8]), "Summer")
             .otherwise("Fall")
        ).withColumn(
            "flight_leg_id",
            F.concat_ws("_", 
                F.col("localDepartureTimestamp").cast("string"),
                F.col("originAirport"),
                F.col("uci")
            )
        ).filter((F.col("localDepartureTimestamp") >= start_date) & \
                 (F.col("localDepartureTimestamp") <= end_date)).\
            select("Country",\
                    "localDepartureTimestamp",\
                    "day_of_week",\
                    "season",\
                    "flight_leg_id",\
                    "uci",\
                    "age",\
                    "passengerType")