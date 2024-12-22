import unittest
from unittest.mock import patch, MagicMock
import main

class TestMainFunction(unittest.TestCase):

    @patch('main.ArgParser')
    @patch('main.DataLoader')
    @patch('main.DataTransformer')
    @patch('main.DataAggregator')
    def test_main(self, MockDataAggregator, MockDataTransformer, MockDataLoader, MockArgParser):

        # Setup the mocks
        mock_args = MagicMock()
        mock_args.start_date = '2021-01-01'
        mock_args.end_date = '2021-12-31'
        mock_args.airport_data_path = 'fake_airport_path'
        mock_args.booking_data_path = 'fake_booking_path'
        mock_args.output_data_path = 'fake_output_path'

        MockArgParser.return_value.parse.return_value = mock_args

        mock_loader = MockDataLoader.return_value
        mock_loader.load_airports.return_value = 'mock_airport_df'
        mock_loader.load_bookings.return_value = 'mock_bookings_df'

        mock_transformer = MockDataTransformer.return_value
        mock_transformer.transform_bookings.return_value = 'mock_transformed_df'

        mock_aggregator = MockDataAggregator.return_value

        # Call the main function
        main.main()

        # Assertions
        # Assert ArgParser is called and parses arguments
        MockArgParser.return_value.parse.assert_called_once()

        # Assert DataLoader is called with expected file paths
        MockDataLoader.assert_called_once_with('fake_airport_path', 'fake_booking_path')
        mock_loader.load_airports.assert_called_once()
        mock_loader.load_bookings.assert_called_once()

        # Assert DataTransformer is called with airport and bookings dataframes
        MockDataTransformer.assert_called_once_with('mock_airport_df', 'mock_bookings_df')
        mock_transformer.transform_bookings.assert_called_once_with('2021-01-01', '2021-12-31')

        # Assert DataAggregator is called with transformed data and writes output
        MockDataAggregator.assert_called_once_with('mock_transformed_df')
        mock_aggregator.write_aggregated_counts.assert_called_once_with('fake_output_path')

if __name__ == '__main__':
    unittest.main()