import asyncio
import time
import urllib.parse

import aiohttp
import pandas as pd
from aiohttp import ClientResponseError
from requests.exceptions import HTTPError

from config import config
from spotify.Composer_Spotify import ComposerSpotify
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

    _REQUESTS_LIMIT = 50

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
        except ClientResponseError as e:
            print(f'Error while performing request: {e}')
            raise e

    async def _perform_async_batch_request(self, url: str, args: list, batch_size: int = 100) -> list:
        """Perform specific request asynchronously given a URL

        Parameters
        ----------
        url: correct formatted endpoint/url

        *args: list of arguments to pass to the url

        batch_size: size of the batch

        Return
        ------
        Result of the request
        """
        result = []
        for i in range(0, len(args), batch_size):
            success = False
            batch_items = args[i:i + batch_size]
            while not success:
                try:
                    result += await asyncio.gather(
                        *[self._perform_async_request(url % batch_item) for batch_item in batch_items])
                    success = True
                except ClientResponseError as e:
                    if e.status == 429:
                        print("Spotify API threshold reached:\n\tSleeping for 30 seconds!")
                        await asyncio.sleep(30)
                    else:
                        raise e

        return result

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

    async def search_albums_by_name(self, names: list[str]) -> list[str]:
        """
        Search for the album ids given a list of album names

        Parameters
        ----------
        names: list[str]
            List of album names

        Return
        ------
        album_ids: list[str]
            List of album ids
        """

        result = await self._perform_async_batch_request(f'{self._base_url}search?q=%s&type=album&limit=50',
                                                         [urllib.parse.quote(name) for name in names])

        albums = [result['albums']['items'] for result in result if result['albums']['items']]
        return albums

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
        results = await self._perform_async_batch_request(f'{self._base_url}search?q=%s&type=artist&limit=1',
                                                          [urllib.parse.quote(name) for name in names])

        composer_ids = [result['artists']['items'][0]['id'] for result in results if result['artists']['items']]
        return composer_ids

    async def get_composers_by_id(self, composers_id: list[str]) -> list[ComposerSpotify]:
        """
        Get the composers given a list of composer ids
        """
        composer_ids_batch = [",".join(composers_id[i:i + self._REQUESTS_LIMIT]) for i in range(0, len(composers_id),
                                                                                                self._REQUESTS_LIMIT)]
        results = await self._perform_async_batch_request(f'{self._base_url}artists?ids=%s', composer_ids_batch)

        composers = [item for sublist in results for item in sublist['artists'] if item]
        print("Composers: ", len(composers))

        composers_parsed = []
        for c in composers:
            try:
                composers_parsed.append(ComposerSpotify(
                    id=c['id'],
                    name=c['name'],
                    genres=c['genres'],
                    followers=c['followers']['total'],
                    popularity=c['popularity'],
                ))
            except Exception as e:
                print(e)
                print(c)
        return composers_parsed

    async def get_composer_albums(self, composer_id: str) -> list[str]:
        """
        Get the albums of a composer

        Parameters
        ----------
        composer_id: str
            Composer id

        Return
        ------
        albums_ids: list[str]
            List of albums ids
        """
        result = await self._perform_async_request(f'{self._base_url}artists/{composer_id}/albums')
        return result['items']

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

    async def get_tracks_from_tracks_ids(self, tracks_ids: list[str]):
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

    async def append_music(self,
                           composer_names: list) -> pd.DataFrame:  # TODO: don't forget to remove all usage if not used
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

    async def create_composers_table(self, composers_names: list[str]):
        """Create the composers table

        Parameters
        ----------
        composers_names: list[str]
            List of composers names

        Return
        -----
        composers: pd.DataFrame
            Dataframe of the composers
        """
        composers_ids = await self.search_composers_by_name(composers_names)

        print("Composers: ", len(composers_ids))

        composers = await self.get_composers_by_id(composers_ids)

        return pd.DataFrame(composers)
