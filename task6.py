from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour
import pyspark.sql.functions as F
import os

# Initialize Spark Session
spark = SparkSession.builder.appName("Night Owl Users").getOrCreate()

# Load datasets
listening_logs_df = spark.read.csv("listening_logs.csv", header=True, inferSchema=True)

# Create output directory
os.makedirs("output/night_owl_users", exist_ok=True)

# Convert timestamp string to timestamp type if needed
if not str(listening_logs_df.schema["timestamp"].dataType).startswith("Timestamp"):
    listening_logs_df = listening_logs_df.withColumn("timestamp", F.to_timestamp("timestamp"))

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
print("Night Owl Users (>20% night listening):")
night_owl_users.select("user_id", "night_listens", "total_listens", "night_listening_percentage") \
    .orderBy(F.desc("night_listening_percentage")) \
    .show(10)

# Save results
night_owl_users.write.mode("overwrite").csv("output/night_owl_users/data")
print("Results saved to output/night_owl_users/")

# Stop Spark session
spark.stop()