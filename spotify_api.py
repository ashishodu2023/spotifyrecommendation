import pandas as pd
from spotipy.oauth2 import SpotifyClientCredentials
import spotipy

# Spotify API credentials
client_id = 'e1eb579c154a4422b3cbd0bda6b4f896'
client_secret = '4244fed6f63541c7b20817abd602a23b'

# Initialize Spotify client
client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

# Function to search for tracks released between two years

# Function to search for tracks released between two years
def search_tracks_by_year(start_year, end_year, limit=50):
    tracks = []
    for year in range(start_year, end_year + 1):
        query = f'year:{year}'
        offset = 0
        while True:
            try:
                response = sp.search(q=query, type='track', limit=limit, offset=offset)
                items = response['tracks']['items']
                if len(items) == 0:
                    break
                for item in items:
                    track_info = item
                    artist_names = [artist['name'] for artist in track_info['artists']]
                    artist_ids = [artist['id'] for artist in track_info['artists']]
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
                    tracks.append(data)
                offset += limit
            except spotipy.SpotifyException as e:
                print(f"Error occurred: {e}")
                break
    return tracks

# Example usage: Search for tracks released between 2020 and 2024
start_year = 2020
end_year = 2024
search_results = search_tracks_by_year(start_year, end_year)
df = pd.DataFrame(search_results)

# Display DataFrame
print(df)
