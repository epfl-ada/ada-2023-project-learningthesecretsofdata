"""
This script has been used to create our personal, specific and enrich dataframe based on
the CMU dataset. Our analysis performed in the JupyterNotebook is using this processed
data.
"""
import time

import pandas

from helpers import *
from tmdb.tmdb import *


async def enhanced_with_composer(clean_movies: pandas.DataFrame):
    async with TMDB() as tmdb:
        start_time = time.time()

        # retrieve 8327 composers information in 150.43 seconds
        result = await tmdb.append_movie_composers(clean_movies)

        end_time = time.time()

        print(f'Elapsed time: {end_time - start_time}')

        # Finally create a pickle file of this new enrich dataframe
        # pickle, as it takes less space on disk, and allows to directly
        # pars de composer column as a list of Composer without having to cast anything
        result.to_pickle('dataset/clean_enrich_movies.pickle')


if __name__ == '__main__':
    # Load movies data set
    movies = load_movies('dataset/MovieSummaries/movie.metadata.tsv')

    # Clean data to filter only observation with all needed features
    clean_movies = clean_movies(movies)

    # Initialize list to store composer for all movies
    movies_composers = []

    # Retrieve composers of all movies
    asyncio.run(enhanced_with_composer(clean_movies))
