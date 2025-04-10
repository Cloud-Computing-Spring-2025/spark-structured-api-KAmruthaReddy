import pandas as pd
import random

# Set seed for reproducibility
random.seed(42)

# Create song metadata
num_songs = 500
songs = []

# Define possible values for genre and mood
genres = ["Pop", "Rock", "Hip-Hop", "R&B", "Jazz", "Classical", "Electronic", "Country", "Metal", "Folk"]
moods = ["Happy", "Sad", "Energetic", "Chill", "Angry", "Romantic", "Melancholy", "Upbeat"]

# Sample artist names
artists = [
    "The Echoes", "Midnight Serenade", "Electric Dreams", "Harmony Central", 
    "Sonic Wave", "Rhythm Republic", "Melody Lane", "Beats Foundation",
    "Synth Collective", "Acoustic Alliance", "Sunset Boulevard", "Northern Lights",
    "Sound Pioneers", "Velvet Voice", "Digital Frontier", "Analog Hearts",
    "Echo Chamber", "Neon Pulse", "Crystal Clear", "Starlight Symphony"
]

# Sample song title prefixes and suffixes
title_prefixes = ["Love", "Lost", "Forever", "Dreams", "Night", "Summer", "Winter", "Dancing", "City", "Heart"]
title_suffixes = ["Nights", "Days", "Dreams", "Light", "Shadow", "Beat", "Rhythm", "Song", "Memories", "Journey"]

for i in range(1, num_songs+1):
    song_id = f"song_{i}"
    
    # Generate a random title
    if random.random() < 0.7:
        title = f"{random.choice(title_prefixes)} {random.choice(title_suffixes)}"
    else:
        title = f"The {random.choice(title_prefixes)}"
    
    # Add some uniqueness to titles
    if random.random() < 0.3:
        title += f" #{random.randint(1, 99)}"
    
    artist = random.choice(artists)
    genre = random.choice(genres)
    mood = random.choice(moods)
    
    songs.append({
        'song_id': song_id,
        'title': title,
        'artist': artist,
        'genre': genre,
        'mood': mood
    })

# Create DataFrame
songs_df = pd.DataFrame(songs)

# Save to CSV
songs_df.to_csv('songs_metadata.csv', index=False)
print(f"Generated songs_metadata.csv with {num_songs} entries")