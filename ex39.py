import sys
import os
from pathlib import Path

from pandas import SparseDtype
from pyspark.sql import SparkSession
from operator import add

os.environ['PYSPARK_PYTHON'] = 'python'

def split_data(line):
    """
    Split each line into columns
    """
    data = str.split(line, ',')
    return [data[0], data[1]]

def filter_by_threshold(line):
    """
    Select line that has temperature > 50
    """
    temp = float(str.split(line, ',')[2])
    return temp>50


if __name__ == "__main__":
    
    # Create an instace of spark
    spark = SparkSession.builder.appName('Exercise 39').getOrCreate()

    # Get absolute path
    absolute_path = Path().absolute()

    # Input path
    input_path = os.path.join(absolute_path, 'ex-39/input.csv')

    # Output path
    output_path = os.path.join(absolute_path, 'ex-39/output')

    # Get a spark context
    sc = spark.sparkContext

    # Input data from CSV file
    lines = sc.textFile(input_path)

    # Filter out all readings below threshold 50 and return (sensor_id, date)
    lines_above = lines.filter(filter_by_threshold).map(split_data)

    # Map each reading with a counter
    lines_above_by_key = lines_above.groupByKey().mapValues(list)

    # Print the result on the standar output
    print(lines_above_by_key.collect())

    # Save result in output
    lines_above_by_key.coalesce(1).saveAsTextFile(output_path)

    # Stop spark
    spark.stop()
    
