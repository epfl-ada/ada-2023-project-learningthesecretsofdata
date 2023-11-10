"""
This script has been used to create our personal, specific and enrich dataframe based on
the CMU dataset. Our analysis performed in the JupyterNotebook is using this processed
data.
"""
import numpy as np

from helpers import *
from tmdb.tmdb import *

if __name__ == '__main__':

    # Load movies data set
    movies = load_movies()

    # Clean data to filter only observation with all needed features
    clean_movies = clean_movies(movies).head(500)

    # Initialize list to store composer for all movies
    movies_composers = []

    # Retrieve composers of all movies
    tmdb = TMDB()
    for idx, (_, movie) in enumerate(clean_movies.iterrows()):
        movie_composer = tmdb.movie_composer(movie_name=movie['name'],
                                             year=movie['release_date'],
                                             language='en-US',
                                             adult=True)

        # print for evolution
        if idx % 100 == 0:
            print(f'{idx}: {movie_composer}')

        movies_composers.append(movie_composer if movie_composer else np.nan)

    # Add this new information to the cleaned dataframe
    clean_movies['composer'] = movies_composers

    # Finally create a CSV file of this new enrich dataframe
    csv_file_path = 'dataset/clea_enrich_movies.csv'
    clean_movies.to_csv(csv_file_path, index=False)
