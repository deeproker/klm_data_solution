# KLM Data Analysis

## Project Structure - >
data/: Contains the sample data for airports and bookings .
src/: Contains all source code files.

    data_loader.py: Logic for loading airport and booking data.
    data_transformer.py: Transforms raw data into structured data.
    data_aggregator.py: Aggregates the transformed data.
    arg_parser.py: Parses command-line arguments.
    
tests/: Contains all test scripts.

dist/ : klm_data_analysis-0.1-py3-none-any.whl 

setup.py: Script for packaging and distributing the package.

requirements.txt: List of Python dependencies.

README.md: Project information and instructions.

coverage_results/ : Test cases coverage for all components.

## Usage 1

spark-submit --master "local[*]" --py-files dist/klm_data_analysis-0.1-py3-none-any.whl main.py --start_date '1990-12-01' --end_date '2024-12-31' --airport_data_path 'path/to/airports/' --booking_data_path 'path/to/bookings/' --output_data_path 'path/to/output'

## Usage 2

python main.py --start_date '1990-12-01' --end_date '2024-12-31' --airport_data_path 'path/to/airports/' --booking_data_path 'path/to/bookings/' --output_data_path 'path/to/output'



## Sample Output -> 

+-----------+-----------+------+---------+---------+---------------------+     

|Country    |day_of_week|season|Adt_count|Chd_count|total_passenger_count|

+-----------+-----------+------+---------+---------+---------------------+
|Netherlands|Mon        |Spring|1014     |20       |1034                 |

|Netherlands|Sun        |Spring|752      |12       |764                  |

|Netherlands|Fri        |Spring|417      |13       |430                  |

|Netherlands|Tue        |Spring|409      |10       |419                  |

|Netherlands|Wed        |Spring|414      |4        |418                  |



## Overview

The aggregated metrics (ADT_Counts , CHD_Counts and Total_passenger_counts ) is based on departure date and timezone of the departing airport .

Passenger_Flight_leg_id = Combination of Departure datetime , Departing airport , uci of passenger - > For per passenger per flight leg.

## Features
- Load data from csv and json files using PySpark.
- Transform booking data to extract flight and passenger information.
- Aggregate data to count passengers by type, day of the week, country, and season.
- Easily extensible for additional data processing tasks.

## Prerequisites
- Python 3.6 or higher
- discover
- coverage
- pyspark

### Install dependencies
pip install -r requirements.txt

### Build and install package (if necessary)
python setup.py install


## Command-Line Arguments ->
--start_date: Start date for filtering bookings (format: YYYY-MM-DD).
--end_date: End date for filtering bookings (format: YYYY-MM-DD).
--airport_data_path: File path to the airport data .dat/csv file.
--booking_data_path: File path to the bookings data JSON file.

## Unit test running using Docker- >
Dockerfile -- > Contains spark image with python package and scripts to run the unit tests.
docker-compose.yml will publish the coverage_test results in your location (mounted volume ) from where docker-compose build will run.

## steps for tests :
cd klm_data_analytics
## option 1:
docker-compose up --build
you will get latest test coverage results in new sub folder coverage_results
## option 2 without using Docker :
pip install --no-cache-dir -r requirements.txt
coverage run -m unittest discover -s tests
coverage html

