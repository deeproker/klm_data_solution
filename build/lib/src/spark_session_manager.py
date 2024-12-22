from pyspark.sql import SparkSession

class SparkSessionManager:
    _instance = None

    @classmethod
    def get_spark_session(cls):
        """Get a singleton Spark session."""
        if cls._instance is None:
            cls._instance = SparkSession.builder \
                .appName("KLM Data Analysis") \
                .getOrCreate()
        return cls._instance