import sys
import os
from pathlib import Path
from tarfile import AbsolutePathError

from numpy import absolute
from pyspark.sql import SparkSession
os.environ['PYSPARK_PYTHON'] = 'python'

def split_data(line):
    """
    Return the temparature of each line
    """
    data = str.split(line, ',')
    return data[2]



if __name__ == "__main__":

    # Create an instance of spark
    spark = SparkSession.builder.appName('Exercise-33').getOrCreate()

    # Current path
    absolute_path = Path().absolute()

    # Input path
    input_path = os.path.join(absolute_path, 'ex-33/input.csv')

    # Ouput path
    output_path = os.path.join(absolute_path, 'ex-33/output')

    # Get a spark context
    sc = spark.sparkContext

    # Input data from csv file 
    lines = sc.textFile(input_path)

    # Get temparatures only
    temperatures = lines.map(split_data)

    # Get the top-3 tempartures 
    top_3 = sc.parallelize(temperatures.top(3))

    # Print the temperatures
    print('\n'.join(map(str, top_3.collect())))

    # Save results in output file
    top_3.coalesce(1).saveAsTextFile(output_path)

    # Stop spark
    spark.stop()