from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
from prefect_gcp import GcpCredentials
from etl_local_to_gcs import etl_parent_flow


@flow()
def load_local_to_gcs(spotify_attrs):
    etl_parent_flow(spotify_attrs)

@task(retries=3, log_prints=True)
def extract_from_gcs(dataset_file) -> list[Path]:
    """Download spotify data from GCS"""
    paths = []
    # for month in months:
    gcs_path = f"data/{dataset_file}.parquet"

    print('gcs path', gcs_path)
    gcs_block = GcsBucket.load("data-pipeline-block")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    paths.append(Path(f"../data/{gcs_path}"))
    print('paths', paths)
    return paths

# @task()
# def transform(path: Path) -> pd.DataFrame:
#     """Data cleaning example"""
#     df = pd.read_parquet(path)
#     print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
#     df['passenger_count'].fillna(0, inplace=True)
#     print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")

#     return df

@task()
def read_parquet_file_to_df(paths: list[Path]) -> pd.DataFrame:
    """Simply read parquet file into pandas df"""
    dfs = []
    for path in paths:
        print('path', path)
        df = pd.read_parquet(path)
        dfs.append(df)
        print('rows of curr df', df.shape[0], 'cols of curr df', df.shape[1])
        print('rows of curr df', len(df))
    dfs = pd.concat(dfs)
    print('rows of curr df', dfs.shape[0], 'cols of curr df', dfs.shape[1])
    print('rows of curr df', len(dfs))
    return dfs

@task()
def write_bq(df: pd.DataFrame, spotify_attr) -> None:
    """Write dataframe into BigQuery"""

    gcp_credentials_block = GcpCredentials.load("zoomcamp-gcp-creds")

    df.to_gbq(
        destination_table=f"spotify.{spotify_attr}",
        project_id="astute-atlas-387920",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists='append'
    )

@flow(log_prints=True)
def etl_gcs_to_bq(spotify_attr):
    """Main ETL flow to load data into Big Query DW"""
    # color = 'yellow'
    # year = 2021
    # month = 1

    
    paths = extract_from_gcs(spotify_attr)
    df = read_parquet_file_to_df(paths)
    write_bq(df, spotify_attr)

@flow()
def etl_parent_gcs_to_bq_flow(spotify_attrs: list[str]):
    for spotify_attr in spotify_attrs:
        etl_gcs_to_bq(spotify_attr)


if __name__ == "__main__":
    spotify_attrs = ['playlists', 'tracks', 'audio_features']
    # load local data to gcs
    load_local_to_gcs(spotify_attrs)
    # load gcs data to bq
    etl_parent_gcs_to_bq_flow(spotify_attrs)