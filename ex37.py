import sys
import os
from pathlib import Path

from pyspark.sql import SparkSession

os.environ['PYSPARK_PYTHON'] = 'python'


def split_data(line):
    """
    Split each line into columns
    """
    data = str.split(line, ',')
    return [data[0], data[2]]

if __name__ == "__main__":

    # Create an instace of spark
    spark = SparkSession.builder.appName('Exercise 37').getOrCreate()
    
    # Current path
    absolute_path = Path().absolute()

    # Input path
    input_path = os.path.join(absolute_path, 'ex-37/input.csv')

    # Output path
    output_path = os.path.join(absolute_path, 'ex-37/output')

    # Get a spark context
    sc = spark.sparkContext

    # Input data from CSV file 
    lines = sc.textFile(input_path)

    # Map each line to a pair (sensorID, temp)
    sensor_temp = lines.map(split_data)

    # Select the maximum temperature for each sensor
    max_temp_by_sensor = sensor_temp.reduceByKey(lambda x, y: x if x>y else y)

    # Print the result on the standard output
    print(max_temp_by_sensor.collect())

    # Save to output path
    max_temp_by_sensor.coalesce(1).saveAsTextFile(output_path)

    # Stop spark
    spark.stop()