import asyncio
import datetime
import urllib.parse
from datetime import datetime

import aiohttp
import numpy as np
import pandas
from requests.exceptions import HTTPError

from config import config
from tmdb.Composer import Composer
from rapidfuzz import fuzz


class TMDBDataLoader:
    """
    Class representing a tmdb connection, to perform some request in order to enhance the dataset with tmdb data

    This class should be instantiated inside an 'async with' block, to automatically close the session once the block
    is exited

    e.g. async with TMDBDataLoader() as tmdb:
            tmdb.SOME_METHOD()
            ...
    """

    def __init__(self, debug=True):
        # Create special connector to limit number of connection per host
        self._tcp_connector = aiohttp.TCPConnector(limit_per_host=50)
        # Create header to use with the session
        self._header = headers = {"accept": "application/json",
                                  "Authorization": f"Bearer {config['TMDB_BEARER_TOKEN']}"}

        # create a timeout set to None, to bypass the timeout and prevent error after 5min, which is the default timeout
        timeout = aiohttp.ClientTimeout(total=None)
        # create the session
        self._session = aiohttp.ClientSession(headers=headers, connector=self._tcp_connector, timeout=timeout)

        self._base_url = "https://api.themoviedb.org/3"

        self._debug = debug

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

    async def _perform_async_request(self, url: str, request_nb: int, request_descr: str):
        """Perform specific request asynchronously given a URL

        Parameters
        ----------
        url: correct formatted endpoint/url
        request_nb: The index of the request we are processing, to be able to print a debug status of the
        execution status
        request_descr: A quick description of the request, to have a context in the debug print

        Return
        ------
        Result of the request
        """

        try:
            async with self._session.get(url) as response:
                response.raise_for_status()
                response = await response.json()
                if self._debug and request_nb % 1000 == 0:
                    print(f'{request_descr} - nb: {request_nb} - completed.')
                return response
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

    async def _search_all_movie_ids(self, urls: pandas.Series) -> (list[int], list[str]):
        """Search for all movies ids given the received urls.
            The id might be "-1" if the movie was not found in tmdb, this is because to easily add it to the
            dataframe, it needs to have the same length.

        Parameters
        ----------
        urls: The list of urls to request

        Return
        ------
        The list of movie ids along with the title of the found movie, duplicate element
        """

        ids_requests_name_year = [(self._perform_async_request(url, int(idx), 'request movie id'), name, year)
                                  for idx, (url, name, year) in urls.items()]

        urls, names, years = zip(*ids_requests_name_year)

        ids_responses = await asyncio.gather(*urls)

        results = map(lambda response: response['results'], ids_responses)

        results_name = zip(results, names, years)

        return self._get_best_match_movie_id(results_name)  # [res[0]['id'] if res else -1 for res in results]

    @staticmethod
    def _get_best_match_movie_id(results_with_expected_name: zip) -> tuple[list, list]:
        """
        Return the list of movie ids along with a list of their names found on tmdb. The name will be useful
        later if there is still some duplicated id, we can filter out the name that are further away from the movie
        title we had

        Parameters
        ----------
        results_with_expected_name: a zip containing the request, the name of the movie from the dataframe, and the
        year of the movie from the dataframe as well

        Returns
        -------
        A tuple containing the list of movie ids found and a list of the movie names coming from tmdb
        """
        id_results = []
        name_results = []

        for movies, title, year in results_with_expected_name:
            if movies:
                movies_titles = [movie['title'] for movie in movies]  # append all title
                # Need original title, as some movies are given with original title and some not
                movies_titles += [movie['original_title'] for movie in movies]  # append all original titles
                comparison_ratio = [fuzz.ratio(t.lower(), title.lower()) for t in movies_titles]

                # As we now concatenate title with original_title, we have to duplicate the movies list as well
                movies += movies
                max_ratio = np.argmax(comparison_ratio)
                max_ratio_occurrences = np.where(np.array(comparison_ratio) == comparison_ratio[max_ratio])[0]
                if len(max_ratio_occurrences) > 1:
                    movies_subset = np.array(movies)[max_ratio_occurrences]
                    # If same ratio occurs more than once, check year to choose
                    matching_years = [year in movie['release_date'] for movie in movies_subset]
                    date_occurrence = np.where(np.array(matching_years))[0]
                    if len(date_occurrence) > 0:
                        # take the first date occurrence
                        id_results.append(movies_subset[date_occurrence[0]]['id'])
                        name_results.append(movies_titles[date_occurrence[0]])
                    else:
                        # If the year did not match any movies then append -1
                        id_results.append(-1)
                        name_results.append('NOT_FOUND')
                else:
                    id_results.append(movies[max_ratio]['id'])
                    name_results.append(movies_titles[max_ratio])
            else:
                id_results.append(-1)
                name_results.append('NOT_FOUND')

        return id_results, name_results

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
        requests_cast = [self._perform_async_request(url, int(row_idx), 'request movie composer')
                         if idx != -1 else self._async_sync_result([])
                         for row_idx, (idx, url) in ids_urls.items()]

        responses_cast = await asyncio.gather(*requests_cast)

        # map cast to only crew
        crews = map(lambda res: res['crew'] if res else [], responses_cast)

        # Extract composer id from crew
        list_composer_ids = map(
            lambda crew: [person['id'] for person in crew if person and 'composer' in person['job'].lower()],
            crews)

        # request the composer from his id
        requests_composer = [self._search_all_composers(person_ids, idx)
                             if person_ids else self._async_sync_result([])
                             for idx, person_ids in enumerate(list_composer_ids)]

        responses_composer = await asyncio.gather(*requests_composer)

        # map empty list to nan values
        composers_nan = map(lambda composers: composers if composers else np.nan, responses_composer)

        return list(composers_nan)

    async def _search_all_composers(self, person_ids, idx) -> list[Composer]:
        """
        Helper function to query all the composers that appears in a movie

        Parameters
        ----------
        person_urls The list of url for the composers of the movie
        idx: The list index of the movie we are currently processing

        Returns
        -------
        The list of composers
        """

        # search for the compositors basics infos
        person_urls = map(lambda composer_id:
                          f'{self._base_url}/person/{composer_id}?append_to_response=movie_credits&language=en-US',
                          person_ids)

        request_person = [self._perform_async_request(url, idx, 'request person details') for url in person_urls]
        responses_person = await asyncio.gather(*request_person)

        composers = map(
            lambda r: Composer(r['id'], r['name'], r['birthday'], r['gender'], r['homepage'], r['place_of_birth'],
                               self._find_oldest_date_credits(r['movie_credits'])),
            responses_person)

        return list(composers)

    @staticmethod
    def _find_oldest_date_credits(credit) -> str:
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

    async def _search_all_movie_revenue(self, urls: pandas.Series) -> list[int]:
        """Retrieve list of revenue for the received list of urls

                Parameters
                ----------
                urls: The list of urls which to query revenue from movie

                Return
                ------
                A list of corresponding revenue
                """
        # perform the async request
        movies_requests = [self._perform_async_request(url, int(row_idx), 'request movie revenue')
                           if idx != -1 else self._async_sync_result({"revenue": 0})
                           for row_idx, (idx, url) in urls.items()]

        movies_responses = await asyncio.gather(*movies_requests)

        results = map(
            lambda response: np.nan if response['revenue'] is not None and response['revenue'] == 0 else
            response['revenue'], movies_responses)

        return list(results)

    @staticmethod
    def _filter_dataset(df: pandas.DataFrame) -> pandas.DataFrame:
        """
        Helper function to filter the dataset. Filter out movies that could not be found on tmdb, and filter out
        movie that have duplicated tmdb id

        Parameters
        ----------
        df The dataframe to filter

        Returns
        -------
        The filtered dataframe
        """

        # Start by filtering out the -1
        result = df.copy()
        result.query('tmdb_id != -1', inplace=True)

        duplicated_tmdb_id = result[result.tmdb_id.duplicated(keep=False)][['tmdb_id', 'name', 'tmdb_title']]

        # Extract unique id for the name of the movie that has the highest similarity with the one found on tmdb
        best_unique_id = duplicated_tmdb_id.groupby('tmdb_id').apply(lambda grouped_df: grouped_df.apply(
            lambda row: fuzz.ratio(row['name'].lower(), row['tmdb_title'].lower()), axis='columns'
        ).idxmax())

        # Drop the best unique id, so that only the one we don't want remains, so that we can remove them from
        # full complete dataframe
        duplicated_to_drop = duplicated_tmdb_id.drop(index=best_unique_id).index

        return result.drop(index=duplicated_to_drop).drop(columns='tmdb_title')

    @staticmethod
    def _generate_df_chunk(df: pandas.DataFrame, chunk_size: int):
        """ Helper function to return a Generator with the dataframe chunked

        Parameters
        ----------
        df: The dataframe to split into chunks
        chunk_size: The to use to chunk the dataframe

        Returns
        -------
        A tuple with the start index, end index and the dataframe chunk

        """
        start = 0
        end = chunk_size
        while end < len(df):
            yield start, end, df.iloc[start:end]
            start = end
            end += chunk_size
        yield start, end, df.iloc[start:len(df)]

    async def append_tmdb_movie_ids(self, df: pandas.DataFrame, filter_dataset: bool = True) -> pandas.DataFrame:
        """Retrieve list of ids for the received dataframe

        Parameters
        ----------
        df: The dataframe containing the information on the movies. Should have a 'name' and 'release_date' column
        filter_dataset: Whether to filter movies that were not found on tmdb and filter movies for which the same
        tmdb_id was returned.
        Return
        ------
        A copy of the received dataframe where the tmdb movie ids were append
        """

        search_movies_urls_name_year = df.agg(lambda entry: (f"{self._base_url}/search/movie?"
                                                             f"query={urllib.parse.quote(entry['name'])}&"
                                                             f"include_adult=true&"
                                                             f"language=en-US&"
                                                             f"page=1&year={entry['release_date']}",
                                                             entry['name'], entry['release_date']),
                                              axis='columns')

        # perform the async request
        movie_ids, movie_names = await self._search_all_movie_ids(search_movies_urls_name_year)

        res_df = df.copy()
        res_df['tmdb_id'] = movie_ids
        res_df['tmdb_title'] = movie_names

        if filter_dataset:
            res_df = self._filter_dataset(res_df)

        return res_df

    async def append_movie_composers(self, df: pandas.DataFrame, filter_dataset: bool = True) -> pandas.DataFrame:
        """Retrieve the composer for the received dataframe

        Parameters
        ----------
        df: The movies dataframe for which to append the composers. Need tmdb_id column in dataframe
        filter_dataset: Whether to filter movies that were not found on tmdb and filter movies for which the same
        tmdb_id was returned.

        Return
        ------
        A copy of the received dataframe where the composers were append
        """

        # If tmdb ids not already present start by fetching them
        if 'tmdb_id' not in df.columns:
            df = await self.append_tmdb_movie_ids(df, filter_dataset)

        # Creates url to fetch all movies composers in credits
        credit_movies_ids_urls = df.tmdb_id.apply(
            lambda idx: (idx, f'{self._base_url}/movie/{idx}/credits?language=en-US'))

        # Performs requests
        results = await self._search_all_movie_composers(credit_movies_ids_urls)

        # Append the composers to the dataframe
        res_df = df.copy()
        res_df['composers'] = results

        return res_df

    async def append_movie_revenue(self, df: pandas.DataFrame, chunk_size=15000, filter_dataset: bool = True) \
            -> pandas.DataFrame:
        """Retrieve the revenue for the received dataframe

        Parameters
        ----------
        df: The movies dataframe for which to append the revenue. Need tmdb_id column in dataframe
        chunk_size: The size of the chunk
        filter_dataset: Whether to filter movies that were not found on tmdb and filter movies for which the same
        tmdb_id was returned.
        Return
        ------
        A copy of the received dataframe where the composers were append
        """

        res = df.copy()
        # prepared df with new column
        res = res.reindex(columns=list(set(res.columns.tolist() + ['tmdb_id', 'tmdb_title', 'tmdb_revenue'])))
        # enforce tmdb_title to be of type object, needed to be able to assign via loc
        # https://stackoverflow.com/questions/37619314/does-loc-a-b-assignment-allow-to-change-the-dtype-of-the-columns
        res = res.astype({'tmdb_title': 'object'})

        # chunked the dataframe querying to retry chunk upon failure
        for start, end, df_chunk in self._generate_df_chunk(df, chunk_size):
            retry = True
            while retry:
                try:
                    # Fetch and append tmdb_id to the df (not filter yet as we need to keep consistent index in df)
                    df_chunk = await self.append_tmdb_movie_ids(df_chunk, False)

                    # add tmdb_id to res
                    res.iloc[start:end].loc[:, ['tmdb_id', 'tmdb_title']] = df_chunk[['tmdb_id', 'tmdb_title']]

                    # Creates url to fetch all movies composers in credits
                    movies_ids_urls = df_chunk.tmdb_id.apply(
                        lambda idx: (idx, f'{self._base_url}/movie/{idx}?language=en-US'))

                    # Performs requests
                    results = await self._search_all_movie_revenue(movies_ids_urls)

                    # Append the revenue to the dataframe
                    res.iloc[start:end].loc[:, 'tmdb_revenue'] = results

                    # if everything ok, go out of while
                    retry = False
                except Exception as e:
                    print(f'Received error: {e}, retry for block {start} - {end}')

        if filter_dataset:
            res = self._filter_dataset(res)

        return res
