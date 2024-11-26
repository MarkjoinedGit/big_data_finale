# type: ignore
import shutil
import sys
import os
from pathlib import Path

from pyspark.sql import SparkSession


if __name__ == "__main__":
    os.environ['PYSPARK_PYTHON'] =  "python"

    # Create an instance of spark
    spark = SparkSession.builder.appName('Exercise-30').getOrCreate()

    # Current path
    absolute_path = Path().absolute()

    # Input path
    input_path = os.path.join(absolute_path, 'input\input_30.txt')

    # Output path
    output_path = os.path.join(absolute_path, 'output')

    # Input data    
    lines = spark.read.text(input_path).rdd.map(lambda x: x[0])

    # Filter out data not containing the work 'google'
    lines_w_google = lines.filter(lambda x: 'google' in x)

    # Save data on the output file
    lines_w_google.coalesce(1).saveAsTextFile(os.path.join(output_path, 'exercise-30'))
    
    # Stop spark
    spark.stop() 