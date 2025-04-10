from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col
import os

# Initialize Spark Session
spark = SparkSession.builder.appName("Average Listen Time Per Song").getOrCreate()

# Load datasets
listening_logs_df = spark.read.csv("listening_logs.csv", header=True, inferSchema=True)
songs_metadata_df = spark.read.csv("songs_metadata.csv", header=True, inferSchema=True)

# Create output directory
os.makedirs("output/avg_listen_time_per_song", exist_ok=True)

# Group by song_id and calculate average duration
avg_listen_time = listening_logs_df.groupBy("song_id") \
    .agg(avg("duration_sec").alias("avg_duration_sec")) \
    .orderBy("song_id")

# Join with metadata to get song titles
avg_listen_time_with_titles = avg_listen_time.join(
    songs_metadata_df.select("song_id", "title", "artist"),
    "song_id"
)

# Display results
print("Average Listen Time Per Song:")
avg_listen_time_with_titles.show(10)

# Save results
avg_listen_time_with_titles.write.mode("overwrite").csv("output/avg_listen_time_per_song/data")
print("Results saved to output/avg_listen_time_per_song/")

# Stop Spark session
spark.stop()