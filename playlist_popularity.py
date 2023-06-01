import spotipy
from spotipy.oauth2 import SpotifyOAuth
import os
import pandas as pd
from google.cloud import storage
import logging
import threading
import time
import concurrent.futures
import pyspark
from pyspark.sql import SparkSession
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
from prefect import flow, task 
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash
from pathlib import Path



# client_id = os.environ['SPOTIPY_CLIENT_ID']
# client_secret = os.environ['SPOTIPY_CLIENT_SECRET']
# BUCKET = os.environ["SPOTIFY_GCS_BUCKET"]



sp = spotipy.Spotify(auth_manager=SpotifyOAuth())

# def upload_files_to_gcs(bucket_name, local_file_name, destination):
#     """Uploads a local file to GCS bucket."""

#     storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
#     storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

#     storage_client = storage.Client()
#     bucket = storage_client.bucket(bucket_name)
#     blob = bucket.blob(destination)

#     try:
#         blob.upload_from_filename(local_file_name)

#         print(
#             f"File {local_file_name} uploaded to {destination}.\n"
#         )
#     except Exception as e:
#         print(f'failed to upload bucket: {e}\n')




def get_playlists_from_genre():
    categories = [
                    {'id': '0JQ5DAqbMKFQ00XGBls6ym', 'name': 'Hip-Hop'},
                    {'id': '0JQ5DAqbMKFEC4WFtoNRpw', 'name': 'Pop'},
                    {'id': '0JQ5DAqbMKFDXXwE9BDJAr', 'name': 'Rock'}
                ]

    playlists = []
    for category in categories:
        playlist = sp.category_playlists(category_id=category['id'], limit=10)
        for item in playlist['playlists']['items']:
            playlists.append({
                'genre_id': category['id'],
                'genre': category['name'],
                'playlist_id': item['id'],
                'playlist_name': item['name'],
                'playlist_description': item['description']
    #                 'tracks': item['tracks']['href']
            })

    playlists_df = pd.DataFrame(playlists)
    return playlists_df


def get_playlist_tracks_from_playlists(df):
    
    playlist_identifiers = df[['genre', 'playlist_id', 'playlist_name']].values.tolist()
    playlist_tracks = []
    
    for category, pid, pname in playlist_identifiers:
        #     print(category, pid, pname)

        offset = 0
        total = 1
        limit = 50

        while offset < total:
            tracks = \
            sp.playlist_items(playlist_id=pid, limit=limit, offset=offset)
            total = tracks['total']
            offset += 50

            for idx, track in enumerate(tracks['items']):
                playlist_tracks.append(
                    {
                        'track_id': track['track']['id'],
                        'track_name': track['track']['name'],
                        'artist_name': track['track']['artists'][0]['name'],
                        'artist_id': track['track']['artists'][0]['id'],
                        'track_popularity': track['track']['popularity'],
                        'album_name': track['track']['album']['name'],
                        'album_id': track['track']['album']['id'],
                        'track_length': track['track']['duration_ms'],
                        'release_date': track['track']['album']['release_date'],
                        'playlist_id': pid,
                        'playlist_name': pname,
                        'genre': category
                    }
                )
    return pd.DataFrame(playlist_tracks)

def get_playlist_and_track_dfs():
    playlists_df = get_playlists_from_genre()
    
    tracks_df = get_playlist_tracks_from_playlists(playlists_df)
    tracks_df['track_length'] = round(tracks_df['track_length'] / 60_000, 2)

    return playlists_df, tracks_df

# def df_to_csv_to_gcs(bucket):
#     playlists_df, tracks_df = get_playlist_and_track_dfs()

#     tracks_df.to_csv('playlist_tracks.csv')
#     playlists_df.to_csv('playlists.csv')

#     tracks = pd.read_csv('playlist_tracks.csv')
#     playlists = pd.read_csv('playlists.csv')

#     playlist_tracks_file = 'playlist_tracks.csv'.replace('.csv', '.parquet')
#     playlists_file = 'playlists.csv'.replace('.csv', '.parquet')

#     tracks.to_parquet(playlist_tracks_file, engine='pyarrow')

#     playlists.to_parquet(playlists_file, engine='pyarrow')


#     upload_files_to_gcs(bucket, playlist_tracks_file, f"data/{playlist_tracks_file}")
#     upload_files_to_gcs(bucket, playlists_file, f"data/{playlists_file}")

# if __name__ == '__main__':
#     df_to_csv_to_gcs(BUCKET)

@task(retries=3)
def fetch() -> pd.DataFrame:
    """Read taxi data from web into pandas df"""
    # if randint(0, 1) > 0:
    #     raise Exception
    playlists_df, tracks_df = get_playlist_and_track_dfs()
    return playlists_df, tracks_df

@task(log_prints=True)
def clean(df = pd.DataFrame) -> pd.DataFrame:
    # clean up years without month and date by adding placeholder month/day
    df['release_date'] = df['release_date'].apply(lambda year: f"{year}-01-01" if len(year) == 4 else year)

    # convert to datetime
    df['release_date'] = pd.to_datetime(df['release_date'], format='%Y-%m-%d')

    return df

@task()
def write_local(df:pd.DataFrame, dataset_file:str) -> Path:

    # create a filepath to parquet file
    path = Path(f"data/{dataset_file}.parquet")
    # check to see if data/yellow exists as a parent to parquet file
    # if not, create them
    print('path parent is dir: ', path.parent.is_dir())
    if not path.parent.is_dir():
        path.parent.mkdir(parents=True)
    print('PATH: ', path)
    df.to_parquet(path, compression='gzip')
    return path

@task()
def write_gcs(path: Path) -> None:
    """Uploading local parquet file to gcs"""
    gcp_cloud_storage_bucket_block = GcsBucket.load("data-pipeline-block")
    gcp_cloud_storage_bucket_block.upload_from_path(from_path=f"{path}", to_path=path)
    return


@flow()
def etl_web_to_gcs(playlist_file, track_file) -> None:
    playlists_df, tracks_df = fetch()
    tracks_df_clean = clean(tracks_df)
    playlist_path = write_local(playlists_df, playlist_file)
    track_path = write_local(tracks_df_clean, track_file)
    write_gcs(playlist_path)
    write_gcs(track_path)


if __name__ == '__main__':
    start = time.time()
    etl_web_to_gcs('playlists', 'tracks')
    end = time.time()
    print(f'took {end - start} seconds to upload to GCS')

