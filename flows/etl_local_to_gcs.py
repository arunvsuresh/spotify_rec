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


@task(retries=3)
def fetch(filename) -> pd.DataFrame:
    """Read spotify data (stored locally) into pandas df"""
    df = pd.read_csv(f'{filename}.csv')
    # tracks_df = pd.read_csv(filename)
    # playlists_df, tracks_df = get_playlist_and_track_dfs()
    return df

@task(log_prints=True)
def clean(filename, df = pd.DataFrame) -> pd.DataFrame:
    # for tracks, clean up years without month and date by adding placeholder month/day
    def parse_dates(date):
        if len(date) == 4:
            return f"{date}-01-01"
        elif len(date) == 7:
            return f"{date}-01"
        else:
            return date

    if filename == 'tracks':
        # df['release_date'] = df['release_date'].apply(lambda year: f"{year}-01-01" if len(year) == 4 else year)
        df['release_date'] = df['release_date'].apply(parse_dates)

        # convert to datetime
        df['release_date'] = pd.to_datetime(df['release_date'], format='%Y-%m-%d')

    return df

@task()
def write_local(df:pd.DataFrame, dataset_file:str) -> Path:

    # create a filepath to parquet file
    path = Path(f"data/{dataset_file}.parquet")
    # check to see if data/{filename} exists as a parent to parquet file
    # if not, create them
    if not path.parent.is_dir():
        path.parent.mkdir(parents=True)
    df.to_parquet(path, compression='gzip')
    return path

@task()
def write_gcs(path: Path) -> None:
    """Uploading local parquet file to gcs"""
    gcp_cloud_storage_bucket_block = GcsBucket.load("data-pipeline-block")
    gcp_cloud_storage_bucket_block.upload_from_path(from_path=f"{path}", to_path=path)
    return


@flow()
def etl_web_to_gcs(filename) -> None:
    df = fetch(filename)
    clean_df = clean(filename, df)
    path = write_local(clean_df, filename)
    write_gcs(path)

@flow()
def etl_parent_flow(spotify_attributes: list[str]):
    for spotify_attribute in spotify_attributes:
        etl_web_to_gcs(spotify_attribute)


if __name__ == '__main__':
    start = time.time()
    # spotify_attrs = ['playlists', 'tracks']
    etl_parent_flow(['playlists', 'tracks'])
    end = time.time()
    print(f'took {end - start} seconds to upload to GCS')