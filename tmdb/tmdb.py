"""Explain the existence of this python file"""

import requests
import urllib.parse
from config import config
from requests.exceptions import HTTPError


class TMDB:

    def __init__(self):
        self._base_url = "https://api.themoviedb.org/3"
        self._tmbd_auth_token = config['TMDB_BEARER_TOKEN']

    def _perform_request(self, url) -> dict:
        """Perform specific request given a URL

        Parameters
        ----------
        url: correct formatted endpoint/url

        Return
        ------
        Result of the request
        """
        headers = {"accept": "application/json",
                   "Authorization": f"Bearer {self._tmbd_auth_token}"}

        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return response.json()
        except HTTPError as exc:
            print(f'Error while performing request: {exc.response}')
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


    def search_movie_credits(self, movie_ID) -> dict:
        """Retrieve all the movie credits

        Parameters
        ----------
        movie_ID: id of specific movie

        Return
        ------
        Dictionary of movie credits
        """
        # TMDb API endpoint for movie details including credits
        movie_url = f'{self._base_url}/movie/{movie_ID}/credits?language=en-US'

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
