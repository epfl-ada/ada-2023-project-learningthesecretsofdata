import asyncio
import urllib.parse

import aiohttp
import pandas as pd
import requests
from requests.exceptions import HTTPError

from config import config
from spotify.Music import Music


class SpotifyAPI:
    def __init__(self):
        self._tcp_connector = aiohttp.TCPConnector(limit=50)
        self._header = {
            'Authorization': f'Bearer {config["SPOTIFY_ACCESS_TOKEN"]}',
            'Content-Type': 'application/json',
        }
        self._session = aiohttp.ClientSession(connector=self._tcp_connector, headers=self._header)
        self._base_url = 'https://api.spotify.com/v1/'

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._session.close()

    def _perform_request(self, url: str) -> dict:
        """Perform specific request given a URL

                Parameters
                ----------
                url: correct formatted endpoint/url

                Return
                ------
                Result of the request
                """

        try:
            response = requests.get(url, headers=self._header)
            response.raise_for_status()
            return response.json()
        except HTTPError as exc:
            print(f'Error while performing request: {exc.response} \n {exc.request}')
            raise HTTPError()

    async def _perform_async_request(self, url: str):
        """Perform specific request asynchronously given a URL

        Parameters
        ----------
        url: correct formatted endpoint/url

        Return
        ------
        Result of the request
        """

        try:
            async with self._session.get(url) as response:
                response.raise_for_status()
                return await response.json()
        except HTTPError as e:
            print(f'Error while performing request: {e}')
            raise e

    @staticmethod
    async def _async_sync_result(ret):
        """ Helper function to simulate an asynchron function

        Parameter
        ---------
        ret: the parameter to return

        Return
        ------
        The ret parameter
        """
        return ret

    def search_composer_id(self, composer_name: str) -> str:
        """Search for the composer id given a composer name

        Parameters
        ----------
        composer_name: str
            Name of the composer

        Return
        ------
        composer_id: str
            Id of the composer
        """
        # Encode the name to be used in the URL
        encoded_name = urllib.parse.quote(composer_name)
        url = f'{self._base_url}search?q={encoded_name}&type=artist&limit=1'

        result = self._perform_request(url)

        # Extract the composer id from the result
        composer_id = result['artists']['items'][0]['id']

        return composer_id

    def get_composer_albums(self, composer_id: str) -> list:
        """Get the albums of a composer given his id

        Parameters
        ----------
        composer_id: str
            Id of the composer

        Return
        ------
        albums: list
            List of albums of the composer
        """
        url = f'{self._base_url}artists/{composer_id}/albums'

        result = self._perform_request(url)

        # Extract the albums from the result
        albums = result['items']

        return albums

    def get_album_tracks(self, album_id: str) -> list:
        """Get the tracks of an album given its id

        Parameters
        ----------
        album_id: str
            Id of the album

        Return
        ------
        tracks: list
            List of tracks of the album
        """
        url = f'{self._base_url}albums/{album_id}/tracks'

        result = self._perform_request(url)

        # Extract the tracks from the result
        tracks = result['items']

        return tracks

    def get_music_from_track(self, track: dict) -> Music:
        """Get the music Object from a track

        Parameters
        ----------
        track: dict

        Return
        ------
        music: Music
        """
        artist_id = track['artists'][0]['id']
        url = f'{self._base_url}artists/{artist_id}'
        result = self._perform_request(url)
        genres = result['genres']
        # Extract the music from the result
        music = Music(
            id=track['id'],
            name=track['name'],
            genre=genres,
            composer_id=track['artists'][0]['id'],
            popularity=track['popularity'],
        )

        return music

    async def append_music(self, composer_names: list) -> pd.DataFrame:
        """Append the music to the spotify_dataset.pickle file"""
        result = await asyncio.gather(
            *[self._perform_async_request(f'{self._base_url}search?q={urllib.parse.quote(name)}&type=artist&limit=1')
              for name in composer_names])

        composer_ids = [result['artists']['items'][0]['id'] for result in result if result['artists']['items']]
        albums = await asyncio.gather(
            *[self._perform_async_request(f'{self._base_url}artists/{id}/albums') for id in composer_ids])
        albums_ids = [result['items'][0]['id'] for result in albums if result['items']]
        tracks_id = await asyncio.gather(
            *[self._perform_async_request(f'{self._base_url}albums/{album_id}/tracks') for album_id in albums_ids])
        tracks_id = [result['items'][0]['id'] for result in tracks_id if result['items']]

        tracks = await asyncio.gather(
            *[self._perform_async_request(f'{self._base_url}tracks/{track_id}') for track_id in tracks_id])
        musics = []
        for track in tracks:
            musics.append(self.get_music_from_track(track))
        return pd.DataFrame(musics)
