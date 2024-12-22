import unittest
from unittest.mock import patch
from src.arg_parser import ArgParser

class TestArgParser(unittest.TestCase):
    @patch('sys.argv', ['main.py', '-s', '2023-01-01', '-e', '2023-12-31'])
    def test_parse_with_custom_dates(self):
        parser = ArgParser()
        args = parser.parse()
        
        self.assertEqual(args.start_date, '2023-01-01')
        self.assertEqual(args.end_date, '2023-12-31')

    @patch('sys.argv', ['main.py'])  # No arguments, should use defaults
    def test_parse_with_default_dates(self):
        parser = ArgParser()
        args = parser.parse()
        
        self.assertEqual(args.start_date, '1990-01-01')  # Default start date
        self.assertEqual(args.end_date, '2024-12-31')    # Default end date

    @patch('sys.argv', ['main.py', '-a', '/custom/path/to/airports/', '-b', '/custom/path/to/bookings/','-o','/custom/path/to/output/'])
    def test_parse_with_custom_paths(self):
        parser = ArgParser()
        args = parser.parse()
        
        self.assertEqual(args.airport_data_path, '/custom/path/to/airports/')
        self.assertEqual(args.booking_data_path, '/custom/path/to/bookings/')
        self.assertEqual(args.output_data_path, '/custom/path/to/output/')

if __name__ == '__main__':
    unittest.main()