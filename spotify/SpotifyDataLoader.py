import asyncio
import urllib.parse
from typing import Any

import aiohttp
import pandas as pd
from aiohttp import ClientResponseError

from config import config, reload_env_config
from spotify.Composer_Spotify import ComposerSpotify
from spotify.Music import Music


class SpotifyDataLoader:
    def __init__(self):
        reload_env_config()
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

    _REQUESTS_LIMIT = 49

    def reload_config(self):
        """Reload the config file"""
        self.__init__()

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
                try:
                    response.raise_for_status()
                except ClientResponseError as e:
                    print(f'Error while performing request: {e}')
                    if response.headers.get("Retry-After"):
                        print(f"Sleeping for {response.headers.get('Retry-After')} seconds")
                        await asyncio.sleep(int(response.headers.get('Retry-After')))
                        raise e
                    else:
                        raise e

                await asyncio.sleep(2)
                return await response.json()
        except ClientResponseError as e:
            if e.status == 400:
                print(f'Error while performing request: {e}')
                return None
            print(f'Error while performing request: {e}')
            raise e

    async def _perform_async_batch_request(self, url: str, args: list, batch_size: int = 100, lists=False) -> list:
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
                    print(f'Performing request for {len(args)} requests')
                    if not lists:
                        result += await asyncio.gather(
                            *[self._perform_async_request(url % batch_item) for batch_item in batch_items])
                    else:
                        result.append(await asyncio.gather(
                            *[self._perform_async_request(url % batch_item) for batch_item in batch_items]))
                    success = True
                except ClientResponseError as e:
                    if e.status == 429:
                        print("Spotify API threshold reached:\n\tSleeping for 30 seconds!")
                        await asyncio.sleep(30)
                    else:
                        raise e

        return result

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
        tracks = await self._perform_async_batch_request(f'{self._base_url}albums/%s/tracks',
                                                         [album_id for album_id in albums_ids])

        tracks_items = [t['items'] for t in tracks if t['items']]

        tracks_ids = []
        ban_words = ["Remastered", "Remaster", "remaster", "live", "Live", "Bonus"]
        for sublist in tracks_items:
            tracks_id = []
            for item in sublist:
                if not any(word in item['name'] for word in ban_words):
                    tracks_id.append(item['id'])
            tracks_ids.append(tracks_id)
        return tracks_ids

    async def get_tracks_from_tracks_ids(self, tracks_ids: pd.core.series.Series, genre: bool = False) -> tuple[
        list, list[Any]]:
        """
        Get the tracks from tracks ids

        Parameters
        ----------
        tracks_ids: list[str]
            List of tracks ids

        genre: bool
            If True, return the genres of the artist

        Return
        ------
        tracks: list[dict]
            List of tracks
        """
        tracks_ids = tracks_ids.astype(str)

        batched_track_ids = [",".join(tracks_ids[i:i + self._REQUESTS_LIMIT].values) for i in
                             range(0, len(tracks_ids), self._REQUESTS_LIMIT)]

        tracks = await self._perform_async_batch_request(f'{self._base_url}tracks/?ids=%s',
                                                         [track_id for track_id in batched_track_ids])

        genres = []
        if genre:
            artist_id = [artist["id"] for batch in tracks for track in batch if track for artist in track['artists']]
            genres = await self._perform_async_batch_request(f'{self._base_url}artists/%s',
                                                             [a_id for a_id in artist_id])
            if genres:
                genres = [g['genres'] for g in genres if g['genres']]

        return tracks, genres

    def get_music_from_track(self, track: dict, genre: list[str]) -> Music:
        """Get the music Object from a track

        Parameters
        ----------
        track: dict
            dict of the track

        genre: list[str]
            List of genres of the artist

        Return
        ------
        music: Music
        """
        # Extract the music from the result
        music = Music(
            id=track['id'],
            name=track['name'],
            genre=genre,
            composer_id=track['artists'][0]['id'],
            popularity=track['popularity'],
        )

        return music

    async def search_albums_by_name(self, names: list[str]) -> list[list]:
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

        results = await self._perform_async_batch_request(f'{self._base_url}search?q=%s&type=album&limit=50',
                                                          [urllib.parse.quote(name) for name in names], lists=True)
        albums = []
        for result in results:
            albums.append([result1['albums']['items'] for result1 in result if result1['albums']['items']])
        return albums[0]

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
