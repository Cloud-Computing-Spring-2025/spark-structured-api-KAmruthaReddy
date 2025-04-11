from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg, col, max, datediff, current_timestamp, lit, hour, expr
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import os

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Music Listener Behavior Analysis") \
    .getOrCreate()

# Load datasets
listening_logs_df = spark.read.csv("listening_logs.csv", header=True, inferSchema=True)
songs_metadata_df = spark.read.csv("songs_metadata.csv", header=True, inferSchema=True)

# Register as temp views for SQL queries (if needed)
listening_logs_df.createOrReplaceTempView("listening_logs")
songs_metadata_df.createOrReplaceTempView("songs_metadata")

# Create output directories
output_dirs = [
    "output/user_favorite_genres",
    "output/avg_listen_time_per_song",
    "output/top_songs_this_week",
    "output/happy_recommendations",
    "output/genre_loyalty_scores",
    "output/night_owl_users",
    "output/enriched_logs"
]

for directory in output_dirs:
    if not os.path.exists(directory):
        os.makedirs(directory)

# Display samples of data
print("Listening Logs Sample:")
listening_logs_df.show(5)

print("Songs Metadata Sample:")
songs_metadata_df.show(5)

# Task 1: Find each user's favorite genre
print("\n=== Task 1: Finding each user's favorite genre ===")

# Join logs with metadata to get genre information
logs_with_genre = listening_logs_df.join(songs_metadata_df, "song_id")

# Count plays by user and genre, then find max for each user
user_genre_counts = logs_with_genre.groupBy("user_id", "genre").count()

# Use window function to find max count for each user
window_spec = Window.partitionBy("user_id").orderBy(F.desc("count"))
ranked_genres = user_genre_counts.withColumn("rank", F.rank().over(window_spec))
user_favorite_genres = ranked_genres.filter(col("rank") == 1).select("user_id", "genre", "count")

# Display results
user_favorite_genres.show(10)

# Save results
user_favorite_genres.write.mode("overwrite").csv("output/user_favorite_genres/data")
print("Task 1 results saved to output/user_favorite_genres/")

# Task 2: Calculate the average listen time per song
print("\n=== Task 2: Calculating average listen time per song ===")

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
avg_listen_time_with_titles.show(10)

# Save results
avg_listen_time_with_titles.write.mode("overwrite").csv("output/avg_listen_time_per_song/data")
print("Task 2 results saved to output/avg_listen_time_per_song/")

# Task 3: List the top 10 most played songs this week
print("\n=== Task 3: Finding top 10 most played songs this week ===")

# Convert timestamp string to timestamp type if needed
if not str(listening_logs_df.schema["timestamp"].dataType).startswith("Timestamp"):
    listening_logs_df = listening_logs_df.withColumn("timestamp", F.to_timestamp("timestamp"))

# Filter for the current week (last 7 days from the latest timestamp in the dataset)
max_date = listening_logs_df.agg(F.max("timestamp")).collect()[0][0]
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
top_songs_details.show(10, False)

# Save results
top_songs_details.write.mode("overwrite").csv("output/top_songs_this_week/data")
print("Task 3 results saved to output/top_songs_this_week/")

# Task 4: Recommend "Happy" songs to users who mostly listen to "Sad" songs
print("\n=== Task 4: Recommending Happy songs to users who listen to Sad songs ===")

# Find users who primarily listen to "Sad" songs
user_mood_counts = logs_with_genre.groupBy("user_id", "mood").count()

window_spec_mood = Window.partitionBy("user_id").orderBy(F.desc("count"))
ranked_moods = user_mood_counts.withColumn("rank", F.rank().over(window_spec_mood))
sad_song_listeners = ranked_moods.filter((col("rank") == 1) & (col("mood") == "Sad")).select("user_id")

# Find songs with "Happy" mood that these users haven't listened to
happy_songs = songs_metadata_df.filter(col("mood") == "Happy").select("song_id", "title", "artist", "genre")

# Get songs that sad listeners have already played
sad_listeners_songs = listening_logs_df.join(sad_song_listeners, "user_id").select("user_id", "song_id")

# For each sad listener, find Happy songs they haven't listened to
recommendations = []

for user_row in sad_song_listeners.collect():
    user_id = user_row["user_id"]
    
    # Get songs this user has already listened to
    user_songs = sad_listeners_songs.filter(col("user_id") == user_id).select("song_id")
    
    # Find Happy songs they haven't heard
    new_happy_songs = happy_songs.join(user_songs, "song_id", "left_anti")
    
    # Get up to 3 recommendations
    user_recommendations = new_happy_songs.limit(3)
    
    # Add user_id to recommendations
    user_recommendations_with_id = user_recommendations.withColumn("user_id", lit(user_id))
    
    # Add to results
    if not recommendations:
        recommendations = user_recommendations_with_id
    else:
        recommendations = recommendations.union(user_recommendations_with_id)

# Show recommendations
if recommendations:
    recommendations.show(10, False)
else:
    # Alternative approach if the above doesn't work well with the generated data
    # Get all Happy songs for all Sad listeners as a fallback
    recommendations = sad_song_listeners.crossJoin(
        happy_songs.limit(3)
    )
    recommendations.show(10, False)

# Save results
if recommendations:
    recommendations.write.mode("overwrite").csv("output/happy_recommendations/data")
    print("Task 4 results saved to output/happy_recommendations/")
else:
    print("No recommendations generated for Task 4")

# Task 5: Compute the genre loyalty score for each user
print("\n=== Task 5: Computing genre loyalty scores ===")

# Count total plays per user
user_total_plays = logs_with_genre.groupBy("user_id").count().withColumnRenamed("count", "total_plays")

# Get favorite genre for each user (from Task 1)
user_favorite_genre_plays = user_favorite_genres.select("user_id", "genre", col("count").alias("favorite_genre_plays"))

# Join to calculate loyalty score
genre_loyalty = user_favorite_genre_plays.join(user_total_plays, "user_id") \
    .withColumn("loyalty_score", col("favorite_genre_plays") / col("total_plays")) \
    .select("user_id", "genre", "loyalty_score") \
    .filter(col("loyalty_score") > 0.8) \
    .orderBy(F.desc("loyalty_score"))

# Display results
genre_loyalty.show(10)

# Save results
genre_loyalty.write.mode("overwrite").csv("output/genre_loyalty_scores/data")
print("Task 5 results saved to output/genre_loyalty_scores/")

# Task 6: Identify users who listen to music between 12 AM and 5 AM
print("\n=== Task 6: Identifying night owl users ===")

# Extract hour from timestamp
night_listening = listening_logs_df.withColumn("hour", hour("timestamp")) \
    .filter((col("hour") >= 0) & (col("hour") < 5))

# Count night listens per user
night_owl_counts = night_listening.groupBy("user_id").count().withColumnRenamed("count", "night_listens")

# Calculate total listens per user
user_listens = listening_logs_df.groupBy("user_id").count().withColumnRenamed("count", "total_listens")

# Calculate percentage of night listening
night_owl_users = night_owl_counts.join(user_listens, "user_id") \
    .withColumn("night_listening_percentage", (col("night_listens") / col("total_listens")) * 100) \
    .filter(col("night_listening_percentage") > 20)  # Users who listen at night more than 20% of the time

# Display results
night_owl_users.select("user_id", "night_listens", "total_listens", "night_listening_percentage") \
    .orderBy(F.desc("night_listening_percentage")) \
    .show(10)

# Save results
night_owl_users.write.mode("overwrite").csv("output/night_owl_users/data")
print("Task 6 results saved to output/night_owl_users/")

# Bonus: Create an enriched logs dataset that combines logs with song metadata
print("\n=== Bonus: Creating enriched logs dataset ===")

enriched_logs = listening_logs_df.join(songs_metadata_df, "song_id")
enriched_logs.show(5)

# Save results
enriched_logs.write.mode("overwrite").csv("output/enriched_logs/data")
print("Enriched logs saved to output/enriched_logs/")

# Stop Spark session
spark.stop()