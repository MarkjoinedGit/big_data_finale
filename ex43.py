from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, split, explode, lit
import pyspark.sql.functions as F
import os
from pathlib import Path

def assign_timeslot(hour):
    """
    Assign a timeslot based on the given hour using PySpark's conditional functions.
    """
    return (F.when((hour >= 0) & (hour <= 3), "[0-3]")
            .when((hour >= 4) & (hour <= 7), "[4-7]")
            .when((hour >= 8) & (hour <= 11), "[8-11]")
            .when((hour >= 12) & (hour <= 15), "[12-15]")
            .when((hour >= 16) & (hour <= 19), "[16-19]")
            .otherwise("[20-23]"))


if __name__ == "__main__":
    # Initialize Spark session
    spark = SparkSession.builder.appName("Exercise-43").getOrCreate()

    # Current path
    absolute_path = Path().absolute()

    # Input paths
    stations_file = os.path.join(absolute_path, 'input', 'input_43', 'stations.csv')
    neighbors_file = os.path.join(absolute_path, 'input', 'input_43', 'neighbors.csv')

    # Output path
    output_path = os.path.join(absolute_path, 'output', 'exercise-43')

    # Reading the stations.csv file
    stations_df = spark.read.csv(stations_file, header=False)
    stations_df = stations_df.withColumnRenamed("_c0", "stationId") \
                            .withColumnRenamed("_c1", "date") \
                            .withColumnRenamed("_c2", "hour") \
                            .withColumnRenamed("_c3", "minute") \
                            .withColumnRenamed("_c4", "num_of_bikes") \
                            .withColumnRenamed("_c5", "num_of_free_slots")
    # Add timeslot column
    stations_df = stations_df.withColumn("timeslot", assign_timeslot(col("hour").cast("int")))

    # Reading the neighbors.csv file
    neighbors_df = spark.read.csv(neighbors_file, header=False)
    neighbors_df = neighbors_df.withColumnRenamed("_c0", "stationId") \
                            .withColumnRenamed("_c1", "neighbors")
    # Split the neighbors column into a list of neighbors
    neighbors_df = neighbors_df.withColumn("neighbors", split(col("neighbors"), " "))

    # Compute Percentage of Critical Situations for Each Station
    # User-defined threshold for critical situations
    critical_threshold = 3

    # Add 'is_critical' column to identify critical situations
    critical_situations = stations_df.withColumn("is_critical", when(col("num_of_free_slots") < critical_threshold, 1).otherwise(0))

    # Calculate the percentage of critical situations for each station
    station_critical_percentage = critical_situations.groupBy("stationId").agg(
        (100 * F.sum("is_critical") / F.count("stationId")).alias("critical_percentage")
    )

    # Filter stations with critical situations above 80% and sort
    critical_stations = station_critical_percentage.filter(col("critical_percentage") > 80)
    critical_stations = critical_stations.sort(col("critical_percentage").desc())

    # Save the critical stations
    critical_stations.coalesce(1).write.csv(os.path.join(output_path, "critical_stations"), header=True)

    #Compute Percentage of Critical Situations for Each (Timeslot, Station)
    # Calculate the percentage of critical situations for each (timeslot, station)
    timeslot_critical = critical_situations.groupBy("timeslot", "stationId").agg(
        (100 * F.sum("is_critical") / F.count("stationId")).alias("critical_percentage")
    )

    # Filter pairs with critical situations above 80% and sort
    critical_timeslots = timeslot_critical.filter(col("critical_percentage") > 80)
    critical_timeslots = critical_timeslots.sort(col("critical_percentage").desc())

    # Save the critical timeslots to HDFS
    critical_timeslots.coalesce(1).write.csv(os.path.join(output_path, "critical_timeslots"), header=True)

    #Select Full Station Situations Where All Neighbors Are Also Full
    # Identify full stations where num_of_free_slots == 0
    full_stations = stations_df.filter(col("num_of_free_slots") == 0)

    # Join with the neighbors data to check if neighbors are also full
    neighbor_full = full_stations.alias("s1").join(
        neighbors_df.alias("n"), col("s1.stationId") == col("n.stationId")
    ).select(
        col("s1.*"), explode(col("n.neighbors")).alias("neighborId")
    )

    # Join with the stations data again to get the status of neighbors
    neighbor_status = neighbor_full.alias("nf").join(
        stations_df.alias("s2"),
        (col("nf.neighborId") == col("s2.stationId")) &
        (col("nf.date") == col("s2.date")) &
        (col("nf.hour") == col("s2.hour")) &
        (col("nf.minute") == col("s2.minute"))
    ).filter(col("s2.num_of_free_slots") == 0)

    # Calculate the size of neighbors and filter based on the condition
    neighbor_status = neighbor_status.withColumn(
        "neighbor_count", F.size(F.split(col("nf.neighborId"), " "))  # Convert to array and calculate size
    )

    # Filter fully blocked situations based on neighbor count
    fully_blocked = neighbor_status.groupBy("nf.stationId", "nf.date", "nf.hour", "nf.minute") \
        .agg(F.count("*").alias("count"))

    # Join back with the neighbor_count to apply the filter
    fully_blocked = fully_blocked.join(
        neighbor_status.select("nf.stationId", "neighbor_count"),
        "stationId"
    ).filter(col("count") == col("neighbor_count"))

    # Save the fully blocked situations to HDFS and print the total count
    fully_blocked.coalesce(1).write.csv(os.path.join(output_path, "fully_blocked"), header=True)
    print(f"Total fully blocked situations: {fully_blocked.count()}")


    # Stop Spark session
    spark.stop()
