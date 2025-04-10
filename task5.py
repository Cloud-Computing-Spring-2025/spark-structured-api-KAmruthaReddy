from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import os

# Initialize Spark Session
spark = SparkSession.builder.appName("Genre Loyalty Scores").getOrCreate()

# Load datasets
listening_logs_df = spark.read.csv("listening_logs.csv", header=True, inferSchema=True)
songs_metadata_df = spark.read.csv("songs_metadata.csv", header=True, inferSchema=True)

# Create output directory
os.makedirs("output/genre_loyalty_scores", exist_ok=True)

# Join logs with metadata to get genre information
logs_with_genre = listening_logs_df.join(songs_metadata_df, "song_id")

# Count total plays per user
user_total_plays = logs_with_genre.groupBy("user_id").count().withColumnRenamed("count", "total_plays")

# Find favorite genre plays for each user
user_genre_counts = logs_with_genre.groupBy("user_id", "genre").count()
window_spec = Window.partitionBy("user_id").orderBy(F.desc("count"))
ranked_genres = user_genre_counts.withColumn("rank", F.rank().over(window_spec))
user_favorite_genre_plays = ranked_genres.filter(col("rank") == 1).select("user_id", "genre", col("count").alias("favorite_genre_plays"))

# Join to calculate loyalty score
genre_loyalty = user_favorite_genre_plays.join(user_total_plays, "user_id") \
    .withColumn("loyalty_score", col("favorite_genre_plays") / col("total_plays")) \
    .select("user_id", "genre", "loyalty_score") \
    .filter(col("loyalty_score") > 0.8) \
    .orderBy(F.desc("loyalty_score"))

# Display results
print("Genre Loyalty Scores (>0.8):")
genre_loyalty.show(10)

# Save results
genre_loyalty.write.mode("overwrite").csv("output/genre_loyalty_scores/data")
print("Results saved to output/genre_loyalty_scores/")

# Stop Spark session
spark.stop()