import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag
from pyspark.sql.window import Window

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Exercise-46").getOrCreate()

    absolute_path = Path().absolute()

    input_path = os.path.join(absolute_path, "ex-46\input.txt")
    output_path = os.path.join(absolute_path, "ex-46\output")

    # Read the data into a Dataframe
    data = spark.read.csv(input_path, schema='timestamp LONG, temperature DOUBLE', header=False)

    # Add lagged temperature and timestamp columns 
    window_spec = Window.orderBy("timestamp")
    data_with_lags = data \
        .withColumn("prev_temp", lag("temperature", 1).over(window_spec)) \
        .withColumn("prev_timestamp", lag("timestamp", 1).over(window_spec)) \
        .withColumn("prev_temp2", lag("temperature", 2).over(window_spec)) \
        .withColumn("prev_timestamp2", lag("timestamp", 2).over(window_spec))
    
    # Filter for increasing windows 
    increasing_windows = data_with_lags.filter(
        (col("timestamp") - col("prev_timestamp") == 60) &
        (col("prev_timestamp") - col("prev_timestamp2") == 60) &
        (col("temperature") > col("prev_temp")) & 
        (col("prev_temp") > col("prev_temp2"))
    ).select(
        col("prev_timestamp2").alias("t1"), col("prev_temp2").alias("temp1"),
        col("prev_timestamp").alias("t2"), col("prev_temp").alias("temp2"),
        col("timestamp").alias("t3"), col("temperature").alias("temp3")
    )

    # Save in output
    increasing_windows.rdd.coalesce(1).map(
        lambda row: f"{row.t1}, {row.temp1}, {row.t2}, {row.temp2}, {row.t3}, {row.temp3}"
    ).saveAsTextFile(output_path)

    spark.stop()