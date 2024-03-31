import pandas as pd
from spotipy.oauth2 import SpotifyClientCredentials
import spotipy

# Spotify API credentials
client_id = 'e1eb579c154a4422b3cbd0bda6b4f896'
client_secret = '4244fed6f63541c7b20817abd602a23b'

# Initialize Spotify client
client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

# Function to retrieve track data
def get_track_data(track_id):
    track_info = sp.track(track_id)
    album_info = sp.album(track_info['album']['id'])
    artist_names = [artist['name'] for artist in track_info['artists']]
    artist_ids = [artist['id'] for artist in track_info['artists']]
    
    # Extract relevant information
    data = {
        'id': track_info['id'],
        'name': track_info['name'],
        'album': track_info['album']['name'],
        'album_id': track_info['album']['id'],
        'artists': ', '.join(artist_names),
        'artist_ids': ', '.join(artist_ids),
        'track_number': track_info['track_number'],
        'disc_number': track_info['disc_number'],
        'explicit': track_info['explicit'],
        'danceability': None,  # Fill these in using audio features endpoint
        'energy': None,
        'key': None,
        'loudness': None,
        'mode': None,
        'speechiness': None,
        'acousticness': None,
        'instrumentalness': None,
        'liveness': None,
        'valence': None,
        'tempo': None,
        'duration_ms': track_info['duration_ms'],
        'time_signature': None,
        'year': int(track_info['album']['release_date'][:4]),  # Extract year from release date
        'release_date': track_info['album']['release_date']
    }
    
    # Fetch audio features
    audio_features = sp.audio_features([track_id])[0]
    for key, value in audio_features.items():
        if key in data:
            data[key] = value
    
    return data

def read_track_ids(file_path):
    with open(file_path, 'r') as file:
        track_ids = [line.strip() for line in file]
    return track_ids

# Example usage
#track_id = '4iJyoBOLtHqaGxP12qzhQI'  # Example track ID (Mood by 24kGoldn)
#track_data = get_track_data(track_id)
#df = pd.DataFrame([track_data])

# Display DataFrame
#print(df)


# Example usage
file_path = 'data/spotiy_id.txt'  # Path to the text file containing track IDs
track_ids = read_track_ids(file_path)

# Retrieve track data for each track ID
track_data_list = []
for track_id in track_ids:
    track_data = get_track_data(track_id)
    track_data_list.append(track_data)

# Create DataFrame
df = pd.DataFrame(track_data_list)

# Display DataFrame
print(df)

