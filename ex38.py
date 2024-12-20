# type: ignore
import sys
import os
from pathlib import Path

from pyspark.sql import SparkSession
from operator import add

def split_data_add_counter(line):
    """
    Split each line into columns
    """
    data = str.split(line, ',')
    return [data[0], 1]


if __name__ == "__main__":
    os.environ['PYSPARK_PYTHON'] =  "python"

    # Create an instance of spark
    spark = SparkSession.builder.appName('Exercise-38').getOrCreate()

    # Current path
    absolute_path = Path().absolute()

    # Input path
    input_path = os.path.join(absolute_path, 'input/input_38.csv')

    # Output path
    output_path = os.path.join(absolute_path, 'output')

    # Get a spark context
    sc = spark.sparkContext

    # Input data from CSV file
    lines = sc.textFile(input_path)

    # Filter out all readings below threshold 50
    lines_above = lines.filter(lambda x: float(str.split(x,',')[2]) > 50)

    # Map each reading with a counter
    counter_readings = lines_above.map(split_data_add_counter)

    # Reduce by key, counting how many times each reading occurs. Select readings with at least two occurrences
    readings_above_threshold = counter_readings.reduceByKey(add).filter(lambda x: x[1] > 1)

    # Print the result on the standard output
    print(readings_above_threshold.collect())

    # Save data on the output file
    readings_above_threshold.coalesce(1).saveAsTextFile(os.path.join(output_path, 'exercise-38'))
    
    # Stop spark
    spark.stop()