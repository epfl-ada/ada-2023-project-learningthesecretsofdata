"""
This script has been used to create our personal, specific and enrich dataframe based on
the CMU dataset. Our analysis performed in the JupyterNotebook is using this processed
data.
"""
import asyncio
import time

import pandas

from helpers import load_movies, clean_movies
from tmdb.tmdb import TMDB


async def enhanced_with_composer(movies: pandas.DataFrame):
    async with TMDB() as tmdb:
        start_time = time.time()

        # retrieve 8327 composers information in 150.43 seconds
        result = await tmdb.append_movie_composers(movies)

        end_time = time.time()

        print(f'Elapsed time: {end_time - start_time}')

        # Finally create a pickle file of this new enrich dataframe
        # pickle, as it takes less space on disk, and allows to directly
        # parse the composer column as a list of Composer without having to cast anything
        result.to_pickle('dataset/clean_enrich_movies.pickle')


if __name__ == '__main__':
    # Load movies data set
    raw_movies = load_movies('dataset/MovieSummaries/movie.metadata.tsv')

    # Clean data to filter only observation with all needed features
    cleaned_movies = clean_movies(raw_movies)

    # Initialize list to store composer for all movies
    movies_composers = []

    # Retrieve composers of all movies
    asyncio.run(enhanced_with_composer(cleaned_movies))
