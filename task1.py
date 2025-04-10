from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import os

# Initialize Spark Session
spark = SparkSession.builder.appName("User Favorite Genres").getOrCreate()

# Load datasets
listening_logs_df = spark.read.csv("listening_logs.csv", header=True, inferSchema=True)
songs_metadata_df = spark.read.csv("songs_metadata.csv", header=True, inferSchema=True)

# Create output directory
os.makedirs("output/user_favorite_genres", exist_ok=True)

# Join logs with metadata to get genre information
logs_with_genre = listening_logs_df.join(songs_metadata_df, "song_id")

# Count plays by user and genre, then find max for each user
user_genre_counts = logs_with_genre.groupBy("user_id", "genre").count()

# Use window function to find max count for each user
window_spec = Window.partitionBy("user_id").orderBy(F.desc("count"))
ranked_genres = user_genre_counts.withColumn("rank", F.rank().over(window_spec))
user_favorite_genres = ranked_genres.filter(col("rank") == 1).select("user_id", "genre", "count")

# Display results
print("Users' Favorite Genres:")
user_favorite_genres.show(10)

# Save results
user_favorite_genres.write.mode("overwrite").csv("output/user_favorite_genres/data")
print("Results saved to output/user_favorite_genres/")

# Stop Spark session
spark.stop()