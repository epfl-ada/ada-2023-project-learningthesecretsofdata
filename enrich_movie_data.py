"""
This script has been used to create our personal, specific and enrich dataframe based on
the CMU dataset. Our analysis performed in the JupyterNotebook is using this processed
data.
"""
import asyncio
import time

import pandas

from helpers import load_movies, clean_movies, clean_movies_revenue
from tmdb.tmdbDataLoader import TMDBDataLoader


async def enhanced_with_composer(movies: pandas.DataFrame):
    """Enhanced the dataset with the composers, and directly save it as a pickle

    Parameters
    ----------
    movies: the dataframe to enhance with the composers

    """
    async with TMDBDataLoader() as tmdb:
        start_time = time.time()

        result = await tmdb.append_movie_composers(movies)

        end_time = time.time()

        print(f'Elapsed time: {end_time - start_time}')

        # Finally create a pickle file of this new enrich dataframe
        # pickle, as it takes less space on disk, and allows to directly
        # parse the composer column as a list of Composer without having to cast anything
        result.to_csv('dataset/clean_enrich_movies.csv')
        result.to_pickle('dataset/clean_enrich_movies.pickle')


async def enhanced_with_revenue(movies: pandas.DataFrame, chunk_size=15000) -> pandas.DataFrame:
    """Enhanced the dataset with the revenue

    Parameters
    ----------
    movies: The dataset of the movie to enhanced
    chunk_size: The size of the chunk to split the requests to periodically save the work in case of an error

    Returns
    -------
    The enhanced dataset
    """
    async with TMDBDataLoader() as tmdb:
        result = await tmdb.append_movie_revenue(movies, chunk_size)
        return result


def create_enhanced_movie_dataset():
    """
    This function enhance the movie dataset. It does:
    - Loads a movie dataset
    - enhances it with revenue information
    - enriches it with composer details for each movie.
    """
    # Load movies data set
    raw_movies = load_movies('dataset/MovieSummaries/movie.metadata.tsv')

    # Clean data to filter only observation with all needed features (without looking at box office revenue)
    cleaned_movies_without_revenue_cleaned = clean_movies(raw_movies)

    # Merge revenue from cmu and tmdb and drop nan
    res = asyncio.run(enhanced_with_revenue(cleaned_movies_without_revenue_cleaned, 15000))

    cleaned_movies = clean_movies_revenue(res)

    # Retrieve composers of all movies
    asyncio.run(enhanced_with_composer(cleaned_movies))


if __name__ == '__main__':
    create_enhanced_movie_dataset()
