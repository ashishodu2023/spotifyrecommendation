import pandas as pd
from kafka import KafkaProducer
from datetime import datetime
import time
import random
import numpy as np

# pip install kafka-python

KAFKA_TOPIC_NAME_CONS = "songTopic"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'

if __name__ == "__main__":
    print("Kafka Producer Application Started ... ")

    kafka_producer_obj = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
                                       value_serializer=lambda x: x.encode('utf-8'))
    
    filepath = "data/track_data.csv"
    
    
    songs_df = pd.read_csv(filepath)
    #songs_df = songs_df[songs_df['release_date'] > '2020-01-01']
    #songs_df = songs_df[songs_df['popularity'] > 50]
    
    
    songs_df['order_id'] = np.arange(len(songs_df))
    
    songs_df['artists'] = songs_df['artists'].str.replace('[^a-zA-Z]', '')
    songs_df['artist_ids'] = songs_df['artist_ids'].str.replace('[^a-zA-Z]', '')
    
    #print(songs_df.head(1))
    
    song_list = songs_df.to_dict(orient="records")
    
    
    print("Kafka producer application started")    

    message_list = []
    message = None
    for message in song_list:
        
        message_fields_value_list = []
        
        
        message_fields_value_list.append(message["id"])
        message_fields_value_list.append(message["name"])
        message_fields_value_list.append(message["album"])
        message_fields_value_list.append(message["album_id"])
        message_fields_value_list.append(message["artists"])
        message_fields_value_list.append(message["artist_ids"])
        message_fields_value_list.append(message["track_number"])
        message_fields_value_list.append(message["disc_number"])
        message_fields_value_list.append(message["explicit"])
        message_fields_value_list.append(message["danceability"])
        message_fields_value_list.append(message["energy"])
        message_fields_value_list.append(message["key"])
        message_fields_value_list.append(message["loudness"])
        message_fields_value_list.append(message["mode"])
        message_fields_value_list.append(message["speechiness"])
        message_fields_value_list.append(message["acousticness"])
        message_fields_value_list.append(message["instrumentalness"])
        message_fields_value_list.append(message["liveness"])
        message_fields_value_list.append(message["valence"])
        message_fields_value_list.append(message["tempo"])
        message_fields_value_list.append(message["duration_ms"])
        message_fields_value_list.append(message["time_signature"])
        message_fields_value_list.append(message["year"])
        message_fields_value_list.append(message["release_date"])



        message = ','.join(str(v) for v in message_fields_value_list)
        print("Message Type: ", type(message))
        print("Message: ", message)
        kafka_producer_obj.send(KAFKA_TOPIC_NAME_CONS, message)
        time.sleep(1)


    print("Kafka Producer Application Completed. ")
