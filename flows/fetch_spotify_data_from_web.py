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

# f = open('api.json')
# api_details = json.load(f)
# client_id, client_secret, redirect_uri = api_details['SPOTIPY_CLIENT_ID'], api_details['SPOTIPY_CLIENT_SECRET'], api_details['SPOTIPY_REDIRECT_URI']
sp = spotipy.Spotify(auth_manager=SpotifyOAuth())

def get_playlists_from_genre():
    categories = [
                    {'id': '0JQ5DAqbMKFQ00XGBls6ym', 'name': 'Hip-Hop'},
                    {'id': '0JQ5DAqbMKFEC4WFtoNRpw', 'name': 'Pop'},
                    {'id': '0JQ5DAqbMKFDXXwE9BDJAr', 'name': 'Rock'},
                    {'id': '0JQ5DAqbMKFCWjUTdzaG0e', 'name': 'Indie'},
                    {'id': '0JQ5DAqbMKFHOzuVTgTizF', 'name': 'Dance/Electronic'},
                    {'id': '0JQ5DAqbMKFPrEiAOxgac3', 'name': 'Classical'},
                    {'id': '0JQ5DAqbMKFGvOw3O4nLAf', 'name': 'K-Pop'},
                    {'id': '0JQ5DAqbMKFKLfwjuJMoNC', 'name':
                    'Country'
                    },
                    {'id': '0JQ5DAqbMKFAJ5xb0fwo9m', 'name':
                     'Jazz'},
                     {'id': '0JQ5DAqbMKFIpEuaCnimBj', 'name':
                      'Soul'}
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
            })

    playlists_df = pd.DataFrame(playlists)
    return playlists_df


def get_playlist_tracks_from_playlists(df):

    playlist_identifiers = df[['genre', 'playlist_id', 'playlist_name']].values.tolist()
    playlist_tracks = []

    for category, pid, pname in playlist_identifiers:

        offset = 0
        total = 1
        limit = 50

        while offset < total:
            tracks = \
            sp.playlist_items(playlist_id=pid, limit=limit, offset=offset)
            total = tracks['total']
            offset += 50

            for idx, track in enumerate(tracks['items']):
                if track['track'] is not None:
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

    playlists_df.to_csv('playlists.csv')
    tracks_df.to_csv('tracks.csv')


if __name__ == '__main__':
    start = time.time()
    get_playlist_and_track_dfs()
    end = time.time()
    print(f'took {end - start} seconds to pull spotify data')