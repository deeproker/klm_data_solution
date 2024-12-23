import logging
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataAggregator:
    def __init__(self, transformed_df: DataFrame):
        self.transformed_df = transformed_df

    def aggregate_passengers(self) -> DataFrame:
        """Counts passengers per country, day of the week, and season."""
        try:
            logger.info("Starting to aggregate passengers.")
            aggregated_counts = self.transformed_df.groupBy(
                "Country", 
                "day_of_week", 
                "season", 
                "passengerType"
            ).agg(
                F.countDistinct("flight_leg_id").alias("total_passenger_count_legid")
            )

            logger.info("Passenger aggregation completed successfully.")
            return aggregated_counts.groupBy(
                "Country",
                "day_of_week", 
                "season"
            ).agg(
                F.sum(F.when(F.col("passengerType") == 'ADT', F.col("total_passenger_count_legid")).otherwise(0)).alias("Adt_count"),
                F.sum(F.when(F.col("passengerType") == 'CHD', F.col("total_passenger_count_legid")).otherwise(0)).alias("Chd_count"),
                F.sum("total_passenger_count_legid").alias("total_passenger_count")
            ).orderBy(
                F.col("total_passenger_count").desc()
            )

        except Exception as e:
            logger.error("Error occurred while aggregating passengers: %s", e, exc_info=True)
            raise

    def write_aggregated_counts(self, output_path: str):
        """Writes the aggregated counts to a Parquet file with specified partitioning."""
        try:
            # Obtain the aggregated counts DataFrame
            aggregated_counts = self.aggregate_passengers()
            logger.info("Writing aggregated counts to the output path: %s", output_path)

            # Write the aggregated DataFrame to Parquet format
            aggregated_counts.write \
                .mode("overwrite") \
                .option("mergeSchema", "true") \
                .partitionBy("Country", "season") \
                .parquet(output_path)  # Specify the output path

            logger.info("Aggregated counts written successfully to %s", output_path)
        
        except Exception as e:
            logger.error("Error occurred while writing aggregated counts: %s", e, exc_info=True)
            raise
