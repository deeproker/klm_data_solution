from pyspark.sql import DataFrame
from pyspark.sql import functions as F

class DataAggregator:
    def __init__(self, transformed_df:DataFrame):
        self.transformed_df = transformed_df
    
    def aggregate_passengers(self) -> DataFrame:
        """Counts passengers per country, day of the week, and season."""
        aggregated_counts= self.transformed_df.groupBy(
                "Country", \
                "day_of_week", \
                "season", \
                "passengerType"
                 ).agg(
                 F.countDistinct("flight_leg_id").\
                 alias("total_passenger_count_legid")
                 )
        
        return aggregated_counts.groupBy\
               ("Country",
                "day_of_week", \
                "season")\
                .agg(
                F.sum(F.when(F.col("passengerType") == 'ADT', \
                    F.col("total_passenger_count_legid")).otherwise(0)).\
                            alias("Adt_count"),
                F.sum(F.when(F.col("passengerType") == 'CHD',\
                    F.col("total_passenger_count_legid")).otherwise(0)).\
                            alias("Chd_count"),
                F.sum("total_passenger_count_legid")\
                            .alias("total_passenger_count")
                    ).orderBy\
                        (F.col("total_passenger_count").desc())
    
    """Write the aggregated counts to destination output path  """
    def write_aggregated_counts(self, output_path: str):
        """Writes the aggregated counts to a Parquet file with specified partitioning."""
        # Obtain the aggregated counts DataFrame
        aggregated_counts = self.aggregate_passengers()

        # Write the aggregated DataFrame to Parquet format
        return aggregated_counts.write \
                .mode("overwrite")\
                .option("mergeSchema", "true") \
                .partitionBy("Country", "season") \
                .parquet(output_path)  # Specify the output path    