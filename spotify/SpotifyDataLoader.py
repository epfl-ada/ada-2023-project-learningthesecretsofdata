import asyncio
import urllib.parse

import aiohttp
import pandas as pd
from requests.exceptions import HTTPError

from config import config
from spotify.Music import Music


class SpotifyDataLoader:
    def __init__(self):
        self._tcp_connector = aiohttp.TCPConnector(limit=50)
        self._header = {
            'Authorization': f'Bearer {config["SPOTIFY_ACCESS_TOKEN"]}',
            'Content-Type': 'application/json',
        }
        timeout = aiohttp.ClientTimeout(total=None)
        self._session = aiohttp.ClientSession(connector=self._tcp_connector, headers=self._header, timeout=timeout)
        self._base_url = 'https://api.spotify.com/v1/'

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._session.close()

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

    async def get_music_from_track(self, track: dict) -> Music:
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
        result = await self._perform_async_request(url)
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

    async def search_composers_by_name(self, names: list[str]) -> list[str]:
        """
        Search for the composer ids given a list of composer names

        Parameters
        ----------
        names: list[str]
            List of composer names

        Return
        ------
        composer_ids: list[str]
            List of composer ids
        """
        result = await asyncio.gather(
            *[self._perform_async_request(f'{self._base_url}search?q={urllib.parse.quote(name)}&type=artist&limit=1')
              for name in names])

        composer_ids = [result['artists']['items'][0]['id'] for result in result if result['artists']['items']]
        return composer_ids

    async def get_composers_albums_async(self, composers_id: list[str]) -> list:
        """
        Get the albums ids of all the composers

        Parameters
        ----------
        composers_id: list[str]
            List of composers ids

        Return
        ------
        albums_ids: list[str]
            List of albums ids
        """
        albums = await asyncio.gather(
            *[self._perform_async_request(f'{self._base_url}artists/{artist_id}/albums') for artist_id in composers_id])
        albums_items = [a['items'] for a in albums if a['items']]
        albums_ids = []
        for sublist in albums_items:
            for item in sublist:
                albums_ids.append(item['id'])
        return albums_ids

    async def get_albums_tracks_async(self, albums_ids: list[str]) -> list:
        """
        Get the tracks ids of all the albums

        Parameters
        ----------
        albums_ids: list[str]
            List of albums ids

        Return
        ------
        tracks_ids: list[str]
            List of tracks ids
        """
        tracks = await asyncio.gather(
            *[self._perform_async_request(f'{self._base_url}albums/{album_id}/tracks') for album_id in albums_ids])
        tracks_items = [t['items'] for t in tracks if t['items']]
        print(len(tracks_items))
        tracks_id = []
        ban_words = ["Remastered", "Remaster", "remaster", "live", "Live", "Bonus"]
        for sublist in tracks_items:
            for item in sublist:
                if not any(word in item['name'] for word in ban_words):
                    tracks_id.append(item['id'])
        return tracks_id

    async def get_tracks_from_tracks_ids(self, tracks_ids: list[str]) -> list:
        """
        Get the tracks from tracks ids

        Parameters
        ----------
        tracks_ids: list[str]
            List of tracks ids

        Return
        ------
        tracks: list[dict]
            List of tracks
        """
        tracks = await asyncio.gather(
            *[self._perform_async_request(f'{self._base_url}tracks/{track_id}') for track_id in tracks_ids])
        return tracks

    async def append_music(self, composer_names: list) -> pd.DataFrame:
        """Append the music to the spotify_dataset.pickle file

        Parameters
        ----------
        composer_names: list
            List of composers names

        Return
        -----
        music: pd.DataFrame
            Dataframe of the music tracks
        """
        # Limit the number of composers to 1 for Milestone 2 (to be removed for Milestone 3)
        composer_names = composer_names[:1]

        composer_ids = await self.search_composers_by_name(composer_names)
        print("Composers: ", len(composer_ids))
        albums_ids = await self.get_composers_albums_async(composer_ids)
        print("Albums: ", len(albums_ids))
        tracks_id = await self.get_albums_tracks_async(albums_ids)
        print("Tracks: ", len(tracks_id))

        tracks = await self.get_tracks_from_tracks_ids(tracks_id)
        musics = await asyncio.gather(*[self.get_music_from_track(track) for track in tracks])
        return pd.DataFrame(musics)
