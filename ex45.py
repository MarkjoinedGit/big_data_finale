# type: ignore
import sys
import os
from pathlib import Path
from operator import add

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType

def perform_analysis_sql(session, df_movies, df_prefer, df_watch, output_path):
    """
    Perform this analysis by using SQL queries and tempViews    
    """

    # Create a temporary view associated with the input data
    df_movies.createOrReplaceTempView("movies")
    df_prefer.createOrReplaceTempView("preferences")
    df_watch.createOrReplaceTempView("watchedmovies")

    # Submit SQL queries on the view
    df_movies_watch = session.sql("SELECT w.userid, m.movie_genre FROM watchedmovies w JOIN movies m ON w.movieid = m.movieid")
    df_movies_watch.createOrReplaceTempView("users_watch_genre")

    df_non_preferred_genres = session.sql("SELECT wm.userid, wm.movie_genre FROM users_watch_genre wm LEFT JOIN preferences p ON wm.userid = p.userid AND wm.movie_genre = p.movie_genre WHERE p.movie_genre IS NULL")
    df_non_preferred_genres.createOrReplaceTempView("non_preferred_genres")

    df_misleading_genres  = session.sql("SELECT userid, movie_genre AS misleading_genre FROM non_preferred_genres GROUP BY userid, movie_genre HAVING COUNT(*) >= 5")
    df_misleading_genres.repartition(1).write.csv(output_path, mode='overwrite', header=True)

    # Drop the view
    session.catalog.dropTempView("movies")
    session.catalog.dropTempView("preferences")
    session.catalog.dropTempView("watchedmovies")
    session.catalog.dropTempView("users_watch_genre")
    session.catalog.dropTempView("non_preferred_genres")
    session.catalog.dropTempView("misleading_genres")

if __name__ == "__main__":
    # Set the path for the python interpreter
    os.environ['PYSPARK_PYTHON'] =  "python"

    # Create an instance of spark
    spark = SparkSession.builder.appName('Exercise-50').getOrCreate()

    # Current path
    absolute_path = Path().absolute()

    # Input path
    movies_path = os.path.join(absolute_path, 'input/input_45/movies.txt')
    prefer_path = os.path.join(absolute_path, 'input/input_45/preferences.txt')
    watch_path = os.path.join(absolute_path, 'input/input_45/watchedmovies.txt')


    # Output paths
    output_path = os.path.join(absolute_path, 'output/exercise-45')

    # Load input data into a dataframe
    df_movies = spark.read.csv(movies_path, header=True, inferSchema=True)
    df_prefer = spark.read.csv(prefer_path, header=True, inferSchema=True)
    df_watch = spark.read.csv(watch_path, header=True, inferSchema=True)
    # Perform the analysis by using the dataframe
    perform_analysis_sql(spark, df_movies, df_prefer, df_watch, output_path)
    # Stop Spark execution
    spark.stop()