from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, max as max_
import pyspark.sql.functions as F
import os

# Initialize Spark Session
spark = SparkSession.builder.appName("Top Songs This Week").getOrCreate()

# Load datasets
listening_logs_df = spark.read.csv("listening_logs.csv", header=True, inferSchema=True)
songs_metadata_df = spark.read.csv("songs_metadata.csv", header=True, inferSchema=True)

# Create output directory
os.makedirs("output/top_songs_this_week", exist_ok=True)

# Convert timestamp string to timestamp type if needed
if not str(listening_logs_df.schema["timestamp"].dataType).startswith("Timestamp"):
    listening_logs_df = listening_logs_df.withColumn("timestamp", F.to_timestamp("timestamp"))

# Filter for the current week (last 7 days from the latest timestamp in the dataset)
max_date = listening_logs_df.agg(max_("timestamp")).collect()[0][0]
week_start = F.date_sub(lit(max_date), 7)

this_week_logs = listening_logs_df.filter(F.col("timestamp") >= week_start)

# Count plays by song for the week
top_songs_this_week = this_week_logs.groupBy("song_id") \
    .count() \
    .orderBy(F.desc("count")) \
    .limit(10)

# Join with metadata to get song details
top_songs_details = top_songs_this_week.join(
    songs_metadata_df.select("song_id", "title", "artist", "genre"),
    "song_id"
)

# Display results
print("Top 10 Songs This Week:")
top_songs_details.show(10, False)

# Save results
top_songs_details.write.mode("overwrite").csv("output/top_songs_this_week/data")
print("Results saved to output/top_songs_this_week/")

# Stop Spark session
spark.stop()