from src.data_loader import DataLoader
from src.data_transformer import DataTransformer
from src.data_aggregator import DataAggregator
from src.arg_parser import ArgParser

def main():
    # Initialize the argument parser and parse the arguments
    arg_parser = ArgParser()
    args = arg_parser.parse()

    # Accessing parsed arguments
    start_date = args.start_date
    end_date = args.end_date
    airport_data_path = args.airport_data_path
    booking_data_path = args.booking_data_path
    output_data_path = args.output_data_path

    # Load the data
    loader = DataLoader(airport_data_path, booking_data_path)
    airport_df = loader.load_airports()
    bookings_df = loader.load_bookings()

    # Transform the bookings data
    transformer = DataTransformer(airport_df, bookings_df)
    transformed_df = transformer.transform_bookings(start_date, end_date)

    # Aggregate the data
    aggregator = DataAggregator(transformed_df)
    aggregator.write_aggregated_counts(output_data_path)

if __name__ == "__main__":
    main()