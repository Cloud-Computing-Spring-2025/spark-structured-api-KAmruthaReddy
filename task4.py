from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import os

# Initialize Spark Session
spark = SparkSession.builder.appName("Happy Song Recommendations").getOrCreate()

# Load datasets
listening_logs_df = spark.read.csv("listening_logs.csv", header=True, inferSchema=True)
songs_metadata_df = spark.read.csv("songs_metadata.csv", header=True, inferSchema=True)

# Create output directory
os.makedirs("output/happy_recommendations", exist_ok=True)

# Join logs with metadata to get mood information
logs_with_mood = listening_logs_df.join(songs_metadata_df, "song_id")

# Find users who primarily listen to "Sad" songs
user_mood_counts = logs_with_mood.groupBy("user_id", "mood").count()

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
print("Happy Song Recommendations for Sad Listeners:")
if recommendations:
    recommendations.show(10, False)
    
    # Save results
    recommendations.write.mode("overwrite").csv("output/happy_recommendations/data")
    print("Results saved to output/happy_recommendations/")
else:
    print("No recommendations generated")

# Stop Spark session
spark.stop()