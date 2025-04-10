import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

# Set seed for reproducibility
np.random.seed(42)
random.seed(42)

# Generate random user IDs
num_users = 100
user_ids = [f"user_{i}" for i in range(1, num_users+1)]

# Generate random song IDs
num_songs = 500
song_ids = [f"song_{i}" for i in range(1, num_songs+1)]

# Generate listening logs
num_logs = 10000
logs = []

# Current date as reference point
end_date = datetime(2025, 3, 23, 23, 59, 59)
start_date = end_date - timedelta(days=30)  # One month of data

for _ in range(num_logs):
    user_id = random.choice(user_ids)
    song_id = random.choice(song_ids)
    
    # Random timestamp within the last month
    random_seconds = random.randint(0, int((end_date - start_date).total_seconds()))
    timestamp = start_date + timedelta(seconds=random_seconds)
    
    # Random duration between 30 seconds and 5 minutes
    duration_sec = random.randint(30, 300)
    
    logs.append({
        'user_id': user_id,
        'song_id': song_id,
        'timestamp': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
        'duration_sec': duration_sec
    })

# Create DataFrame
logs_df = pd.DataFrame(logs)

# Save to CSV
logs_df.to_csv('listening_logs.csv', index=False)
print(f"Generated listening_logs.csv with {num_logs} entries")