"""Explain the existence of this python file"""

import asyncio
import datetime
import urllib.parse
from datetime import datetime

import aiohttp
import numpy as np
import pandas
import requests
from requests.exceptions import HTTPError

from config import config
from tmdb.Composer import Composer


class TMDB:
    """
    Class representing a tmdb connection, to perform some request in order to enhance the dataset with tmdb data

    This class should be instantiated inside an 'async with' block, to automatically close the session once the block
    is exited

    e.g. async with TMDB() as tmdb:
            tmdb.SOME_METHOD()
            ...
    """

    def __init__(self):
        # Create special connector to limit number of connection per host
        self._tcp_connector = aiohttp.TCPConnector(limit_per_host=50)
        # Create header to use with the session
        self._header = headers = {"accept": "application/json",
                                  "Authorization": f"Bearer {config['TMDB_BEARER_TOKEN']}"}
        # create the session
        self._session = aiohttp.ClientSession(headers=headers, connector=self._tcp_connector)

        self._base_url = "https://api.themoviedb.org/3"

    async def __aenter__(self):
        """ Method called when entering the 'async with' block

        Returns
        -------
        The object itself
        """
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """
        Method called when exiting the 'async with' block, to close the session
        """
        await self._session.close()

    def _perform_request(self, url) -> dict:
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

    def search_movie_id(self, movie_name: str, year: int, language: str = 'en-US',
                        adult: bool = True) -> int:
        """Retrieve data of a movie using TMDB API

        Parameters
        ----------
        movie_name: name of the movie
        year: movie release date
        language: movie language
        adult: movie for adult

        Return
        ------
        Movie ID result of API request
        """
        url = (f"{self._base_url}/search/movie?"
               f"query={urllib.parse.quote(movie_name)}&"
               f"include_adult={adult}&"
               f"language={urllib.parse.quote(language)}&"
               f"page=1&year={year}")

        movie_infos_results = self._perform_request(url)['results']
        return movie_infos_results[0]['id'] if movie_infos_results else -1

    def search_movie_credits(self, movie_id) -> dict:
        """Retrieve all the movie credits

        Parameters
        ----------
        movie_id: id of specific movie

        Return
        ------
        Dictionary of movie credits
        """
        # TMDb API endpoint for movie details including credits
        movie_url = f'{self._base_url}/movie/{movie_id}/credits?language=en-US'

        # GET request to the TMDb API for the movie details
        movie_response = self._perform_request(movie_url)
        movie_credits = movie_response

        return movie_credits

    def movie_composer(self, movie_name: str, year: int, language: str = 'en-US',
                       adult: bool = True) -> list[str]:
        """Retrieve the movie composer

        Parameters
        ----------
        movie_name: name of the movie
        year: movie release date
        language: movie language
        adult: movie for adult

        Return
        ------
        Movie soundtrack composer
        """
        # Retrieve information about movie credits
        movie_id = self.search_movie_id(movie_name, year, language, adult)

        if movie_id == -1:
            return []
        else:
            movie_credits = self.search_movie_credits(movie_id)

            # Search for composer in the crew information in the movie's credits
            composers = [person['name'] for person in movie_credits['crew'] if 'composer' in person['job'].lower()]
            return composers

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

    async def _search_all_movie_ids(self, urls: pandas.Series) -> list[int]:
        """Search for all movies ids given the received urls

        Parameters
        ----------
        urls: The list of urls to request

        Return
        ------
        The list of movie ids
        """

        ids_requests = [self._perform_async_request(url) for url in urls]
        ids_responses = await asyncio.gather(*ids_requests)

        results = map(lambda response: response['results'], ids_responses)
        return [res[0]['id'] if res else -1 for res in results]

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

    @staticmethod
    def find_oldest_date_credits(credit) -> str:
        """Given all the credit in which a given composer appear, find the date of the first movie for which he has
           composed the music

        Parameters
        ----------
        credit: All the credit in which the composer appears

        Returns
        -------
        The date of the first movie for which a composer composed the soundtrack
        """

        list_release_date = [datetime.strptime(crew['release_date'], '%Y-%m-%d') for crew in credit['crew'] if
                             'composer' in crew['job'].lower() and crew['release_date']]

        min_date = min(list_release_date)
        return min_date.strftime('%Y-%m-%d')

    async def _search_all_composers(self, person_ids) -> list[Composer]:
        """
        Helper function to query all the composers that appears in a movie
        
        Parameters
        ----------
        person_urls The list of url for the composers of the movie

        Returns
        -------
        The list of composers
        """

        # search for the compositors basics infos
        person_urls = map(lambda composer_id:
                          f'{self._base_url}/person/{composer_id}?append_to_response=movie_credits&language=en-US',
                          person_ids)

        request_person = [self._perform_async_request(url) for url in person_urls]
        responses_person = await asyncio.gather(*request_person)

        composers = map(
            lambda r: Composer(r['id'], r['name'], r['birthday'], r['gender'], r['homepage'], r['place_of_birth'],
                               self.find_oldest_date_credits(r['movie_credits'])),
            responses_person)

        return list(composers)

    async def _search_all_movie_composers(self, ids_urls: pandas.Series) -> list[list[Composer]]:
        """Helper function to search for the movie composers from the credits of a movie

        Parameter
        ---------
        ids_urls: A series that contains a tuple with the movie id along with the url to perform the movie credits
                  request.

        Return
        ------
        The list of composers
        """

        # request the cast to retrieve the ids of the composer
        requests_cast = [self._perform_async_request(url) if idx != -1 else self._async_sync_result([]) for
                         (idx, url) in ids_urls]
        responses_cast = await asyncio.gather(*requests_cast)

        # map cast to only crew
        crews = map(lambda res: res['crew'] if res else [], responses_cast)

        # Extract composer id from crew
        list_composer_ids = map(
            lambda crew: [person['id'] for person in crew if person and 'composer' in person['job'].lower()],
            crews)

        # request the composer from his id
        requests_composer = [
            self._search_all_composers(person_ids) if person_ids else self._async_sync_result([]) for
            person_ids in list_composer_ids]
        responses_composer = await asyncio.gather(*requests_composer)

        # map empty list to nan values
        composers_nan = map(lambda composers: composers if composers else np.nan, responses_composer)

        return list(composers_nan)

    async def append_tmdb_movie_ids(self, df: pandas.DataFrame) -> pandas.DataFrame:
        """Retrieve list of ids for the received dataframe

        Parameters
        ----------
        df: The dataframe containing the information on the movies. Should have a 'name' and 'release_date' column

        Return
        ------
        A copy of the received dataframe where the tmdb movie ids were append
        """

        search_movies_urls = df.agg(lambda entry: f"{self._base_url}/search/movie?"
                                                  f"query={urllib.parse.quote(entry['name'])}&"
                                                  f"include_adult=true&"
                                                  f"language=en-US&"
                                                  f"page=1&year={entry['release_date']}", axis='columns')

        # perform the async request
        results = await self._search_all_movie_ids(search_movies_urls)

        res_df = df.copy()
        res_df['tmdb_id'] = results

        return res_df

    async def append_movie_composers(self, df: pandas.DataFrame) -> pandas.DataFrame:
        """Retrieve the composer for the received dataframe

        Parameters
        ----------
        df: The movies dataframe for which to append the composers. Need tmdb_id column in dataframe

        Return
        ------
        A copy of the received dataframe where the composers were append
        """

        # If tmdb ids not already present start by fetching them
        if 'tmdb_id' not in df.columns:
            df = await self.append_tmdb_movie_ids(df)

        # Creates url to fetch all movies composers in credits
        credit_movies_ids_urls = df.tmdb_id.apply(
            lambda idx: (idx, f'{self._base_url}/movie/{idx}/credits?language=en-US'))

        # Performs requests
        results = await self._search_all_movie_composers(credit_movies_ids_urls)

        # Append the composers to the dataframe
        res_df = df.copy()
        res_df['composers'] = results

        return res_df
