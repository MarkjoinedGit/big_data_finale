import sys
import os
from pathlib import Path

from numpy import absolute
from pyspark.sql import SparkSession
os.environ['PYSPARK_PYTHON'] = 'python'

def split_data(line):
    """
    Return the temperatures of each line
    """
    data = str.split(line, ',')
    return float(data[2])

if __name__ == '__main__':

    # Create an instance of spark
    spark = SparkSession.builder.appName('Exercise-36').getOrCreate()

    # Current path
    absolute_path = Path().absolute()

    # Input path
    input_path = os.path.join(absolute_path, 'ex-36/input.csv')

    # Output path
    output_path = os.path.join(absolute_path, 'ex-36/ouput')

    # Get a spark context
    sc = spark.sparkContext

    # Input data from CSV file
    lines = sc.textFile(input_path)

    # Select the temperature column only and compute the mean
    mean_temp = sc.parallelize([lines.map(split_data).mean()])

    # Print the resulte on the standard output
    print(''.join(map(str, mean_temp.collect())))

    # Save in output path
    mean_temp.coalesce(1).saveAsTextFile(output_path)


    # Stop spark
    spark.stop()

