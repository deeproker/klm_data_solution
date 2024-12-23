import logging
from pyspark.sql import SparkSession

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SparkSessionManager:
    _instance = None

    @classmethod
    def get_spark_session(cls):
        """Get a singleton Spark session."""
        if cls._instance is None:
            try:
                logger.info("Creating a new Spark session.")
                cls._instance = SparkSession.builder \
                    .appName("KLM Data Analysis") \
                    .getOrCreate()
                logger.info("Spark session created successfully.")
            except Exception as e:
                logger.error("Error while creating Spark session: %s", e, exc_info=True)
                raise
        else:
            logger.info("Returning existing Spark session.")
        
        return cls._instance