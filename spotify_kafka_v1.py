import pandas as pd
import logging
import requests
from spotipy.oauth2 import SpotifyClientCredentials
import spotipy
from kafka import KafkaProducer

class SpotifyDataToKafka:
    def __init__(self, client_id, client_secret, kafka_bootstrap_servers, kafka_topic):
        self.client_id = client_id
        self.client_secret = client_secret
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.kafka_topic = kafka_topic

        # Initialize Spotify client
        client_credentials_manager = SpotifyClientCredentials(client_id=self.client_id, client_secret=self.client_secret)
        self.sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

        # Initialize Kafka Producer
        self.producer = KafkaProducer(bootstrap_servers=self.kafka_bootstrap_servers,
                                      value_serializer=lambda x: str(x).encode('utf-8'))

        # Configure logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def get_track_data(self, track_id):
        track_info = self.sp.track(track_id)
        album_info = self.sp.album(track_info['album']['id'])
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
            # Fill these in using audio features endpoint
            'danceability': None,
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

        # Fetch audio features from Spotify audio features endpoint
        audio_features_url = f'https://api.spotify.com/v1/audio-features/{track_id}'
        headers = {'Authorization': f'Bearer {self.sp._auth.access_token}'}
        response = requests.get(audio_features_url, headers=headers)
        if response.status_code == 200:
            audio_features = response.json()
            for key, value in audio_features.items():
                if key in data:
                    data[key] = value
        else:
            self.logger.error(f"Failed to fetch audio features for track ID {track_id}. Status code: {response.status_code}")

        return data

    def produce_to_kafka(self, message):
        self.producer.send(self.kafka_topic, value=message)
        self.producer.flush()

    def process_track_ids_from_file(self, file_path):
        all_track_data = []

        with open(file_path, 'r') as file:
            track_ids = [line.strip() for line in file]

        for track_id in track_ids:
            self.logger.info(f"Processing track ID: {track_id}")
            try:
                track_data = self.get_track_data(track_id)
                all_track_data.append(track_data)
                self.logger.info(f"Track data collected for track ID: {track_id}")
            except Exception as e:
                self.logger.error(f"Error processing track ID {track_id}: {e}")

        df = pd.DataFrame(all_track_data)
        return df

if __name__ == "__main__":
    # Example usage
    client_id = 'e1eb579c154a4422b3cbd0bda6b4f896'
    client_secret = '4244fed6f63541c7b20817abd602a23b'
    kafka_bootstrap_servers = 'localhost:9092'
    kafka_topic = 'spotify_track_data'
    file_path = 'data/spotiy_id.txt'

    spotify_to_kafka = SpotifyDataToKafka(client_id, client_secret, kafka_bootstrap_servers, kafka_topic)
    df = spotify_to_kafka.process_track_ids_from_file(file_path)
    print(df)

