import sys
import os
from pathlib import Path
from pyspark.sql.functions import col, count, when, lit

from pyspark.sql import SparkSession

os.environ['PYSPARK_PYTHON'] = 'python'

if __name__ == "__main__":

    threshold = 50

    # Create an instance of spark
    spark = SparkSession.builder.appName('Exercise-44').getOrCreate()

    # Current path
    absolute_path = Path().absolute()

    # Specificed path
    movies_path = os.path.join(absolute_path, 'ex-44/data/movies.txt')
    preferences_path = os.path.join(absolute_path, 'ex-44/data/preferences.txt')
    watchmovies_path = os.path.join(absolute_path, 'ex-44/data/watchedmovies.txt')
    output_path = os.path.join(absolute_path, 'ex-44/output')

    # Load data
    movies = spark.read.option("headers", "false")\
        .csv(movies_path) \
        .toDF("movieid", "title", "movie_genre")
    
    watched_movies = spark.read.option("header", "false") \
        .csv(watchmovies_path) \
        .toDF("userid", "movieid", "start_timestamp", "end_timestamp")
    
    preferences = spark.read.option("header", "false")\
        .csv(preferences_path) \
        .toDF("userid", "preferred_genre")
    
    # Join watched_movies and movies to get the genres of watched movies
    user_movies = watched_movies.join(movies, "movieid")

    # Join user_moives with preferences to get preferred genres
    user_movies_with_preferences = user_movies.join(preferences, "userid","left_outer")

    # Identify whether a watched movie matches the user's preference
    user_movies_with_match = user_movies_with_preferences.withColumn(
        "is_match", 
        when(col("movie_genre") == col("preferred_genre"), 1).otherwise(0)
    )

    # Calculate total watch moives and mismatched movies for each user
    user_stats = user_movies_with_match.groupBy("userid").agg(
        count("movieid").alias("total_movies"),
        (count(when(col("is_match") == 0, 1)) * 100/ count("movieid")).alias("mistmatch_percentage")
    )

    print(user_stats.collect())

    # Filter users with mismatch percentage greater than the threshold
    mistleading_users = user_stats.filter(col("mistmatch_percentage") > lit(threshold))

    print(mistleading_users.select("userid"))

    # Select only userids and save the result to HDFS
    mistleading_users.select("userid").rdd.coalesce(1).map(
        lambda row: f"{row.userid}"
    ).saveAsTextFile(output_path)

    spark.stop()