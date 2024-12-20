import sys
import os
from pathlib import Path

from pyspark.sql import SparkSession

os.environ['PYSPARK_PYTHON'] = 'python'

def split_data(line):
    """
    Return sensorId, date of readings with temperature above 50
    """
    data = str.split(line, ',')
    key = data[0]
    value = data[1] if filter_by_threshold(float(data[2])) else None
    return [key, value]

def filter_by_threshold(temperature):
    return temperature > 50

def map_values_func(data_list):
    """
    Remove all None values from the list
    """
    data = list(data_list)
    return [x for x in data if x != None]

if __name__ == "__main__":

     # Create an instance of spark
    spark = SparkSession.builder.appName('Exercise-39-bis').getOrCreate()

    # Current path
    absolute_path = Path().absolute()

    # Input path
    input_path = os.path.join(absolute_path, 'ex-39-bis/input.csv')
    output_path = os.path.join(absolute_path, 'ex-39-bis/output')

    # Get a spark context
    sc = spark.sparkContext

    # Input data from CSV file
    lines = sc.textFile(input_path)

    # Filter out all readings below threshold 50 and return (sensorId, date)
    lines_key_value = lines.map(split_data)

    # Map each reading with a counter
    lines_above_by_key = lines_key_value.groupByKey().mapValues(map_values_func).sortByKey()

    # Print the resuult on the standard output
    lines_above_by_key.coalesce(1).saveAsTextFile(output_path)

    # Stop spark
    spark.stop()
