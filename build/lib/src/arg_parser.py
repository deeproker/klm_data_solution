import argparse
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ArgParser:
    """Initialize the parser."""
    def __init__(self):
        self.parser = argparse.ArgumentParser(description='KLM Data Analysis')
        try:
            self._setup_arguments()
            logger.info("Arguments setup successfully.")
        except Exception as e:
            logger.error("Error setting up arguments: %s", e, exc_info=True)
            raise

    """Set up the arguments here."""
    def _setup_arguments(self):
        """Set up command-line arguments."""
        self.parser.add_argument('-s', '--start_date', type=str, 
                                 default='1990-01-01',  # Default start date
                                 help='Start date in format YYYY-MM-DD')
        self.parser.add_argument('-e', '--end_date', type=str,
                                 default='2024-12-31',  # Default end date
                                 help='End date in format YYYY-MM-DD')
        self.parser.add_argument('-a', '--airport_data_path', type=str, 
                                 default='./data/airports/', 
                                 help='Path to the airport data file')
        self.parser.add_argument('-b', '--booking_data_path', type=str, 
                                 default='./data/bookings/', 
                                 help='Path to the booking data file')
        self.parser.add_argument('-o', '--output_data_path', type=str, 
                                 default='./data/output/', 
                                 help='Path to the output data file')

    def parse(self):
        """Parse the command-line arguments."""
        try:
            args = self.parser.parse_args()
            logger.info("Arguments parsed successfully.")
            return args
        except Exception as e:
            logger.error("Error parsing arguments: %s", e, exc_info=True)
            raise