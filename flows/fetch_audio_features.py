import pandas as pd
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import os
import pandas as pd
from google.cloud import storage
import logging
import threading
import time
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
from prefect import flow, task 
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash
from pathlib import Path
import json

sp = spotipy.Spotify(auth_manager=SpotifyOAuth())

def flatten_list(lst):
    return [item for inner_list in lst for item in inner_list]

def get_audio_features():

    tracks = pd.read_csv('tracks.csv')

    tracks['track_id'] = tracks['track_id'].str.strip()

    track_ids = tracks.track_id.unique().tolist()

    audio_features = []
    i = 0
    while i < len(track_ids):
        audio_features.append(sp.audio_features(tracks=track_ids[i:i+100]))
        i += 100

    audio_features = flatten_list(audio_features)

    audio_features = [i for i in audio_features if i is not None]

    audio_features = pd.DataFrame.from_dict(audio_features)

    audio_features.to_csv('audio_features.csv')


if __name__ == '__main__':
    start = time.time()
    get_audio_features()
    end = time.time()
    print(f'took {end - start} seconds to get audio features')