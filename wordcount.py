# type: ignore
import sys
import os
from pathlib import Path
from pyspark.sql import SparkSession
from operator import add


def split_data_add_counter(line):
    """
    Split each line into columns and create (key, value) pairs.
    """
    data = str.split(line, ' ')
    return [(word, 1) for word in data]


if __name__ == "__main__":
    os.environ['PYSPARK_PYTHON'] = "python"

    # Create an instance of Spark
    spark = SparkSession.builder.appName('Wordcount').getOrCreate()

    # Current path
    absolute_path = Path().absolute()

    # Input path
    input_path = os.path.join(absolute_path, 'input/wordcount/file02')

    # Output path
    output_path = os.path.join(absolute_path, 'output')

    # Get a spark context
    sc = spark.sparkContext

    # Input data from text file
    lines = sc.textFile(input_path)

    # Map each word with a counter
    flat_mapped_words = lines.flatMap(split_data_add_counter)

    # Reduce by key, counting how many times each word occurs
    word_counts = flat_mapped_words.reduceByKey(add)

    # Print the result on the standard output
    print(word_counts.collect())

    # Save data on the output file
    word_counts.coalesce(1).saveAsTextFile(os.path.join(output_path, 'wordcount'))

    # Stop spark
    spark.stop()
