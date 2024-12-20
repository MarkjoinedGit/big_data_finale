import sys #provides access to system-specific parameters and functions
import os #provides a way to interact with the operating system

from pathlib import Path #provides an object-oriented approach to handle filesystem paths

from pyspark.sql import SparkSession
#leverage Apache Spark in Python program
#SparkSession is the entry point to used PySpark's features. It encapsulates the context and configuration for Spark jobs.

os.environ['PYSPARK_PYTHON'] =  "python"

if __name__ == "__main__":

    # Create an instance of spark
    spark = SparkSession.builder.appName('Exercise-31').getOrCreate()
    #--> create an entry point spark for interacting with Spark's features

    # Current path
    absolute_path = Path().absolute()

    # Input path
    input_path = os.path.join(absolute_path, 'ex-31/input.txt')

    # Output path
    output_path = os.path.join(absolute_path, 'ex-31/output')

    # Input data
    lines = spark.read.text(input_path).rdd.map(lambda x: x[0])
    #--> Load text data and converts it into an RDD (Resilient Distributed Data) of string.

    # Filter out data not containing the word 'google'
    lines_w_google = lines.filter(lambda x: 'google' in x)

    # Take unique IPs
    unique_ips = lines_w_google.map(lambda x: x.split()[0]).distinct()
    #--> take the ips from lines_w_google and distinct it

    # Save data on the output file
    unique_ips.coalesce(1).saveAsTextFile(output_path)

    # Stop spark
    spark.stop()