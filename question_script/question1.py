"""
Answer first question of project:
Which are the most frequent music genre appearing in movies ?
"""
import pandas as pd


def create_db_to_link_composers_to_movies(movies: pd.DataFrame) -> pd.DataFrame:
    """Description if needed"""
    # Initialize the new database
    db_to_link_composers_to_movies = pd.DataFrame(
        columns=['tmdb_id', 'comp_id', 'movie_name', 'movie_revenue', 'composer_name', 'release_date']
    )
    # Set the index to be unique (pair of ids)
    db_to_link_composers_to_movies.set_index(['tmdb_id', 'comp_id'], inplace=True)

    # Description TODO
    for _, movie in movies.iterrows():
        movie_id = movie['tmdb_id']
        movie_name = movie['name']
        movie_revenue = movie['box_office_revenue']
        composers = movie['composers']
        release_date = movie['release_date']

        if type(composers) == list:  # meaning we have information about composers, otherwise float nan returned
            for composer in composers:
                comp_id = composer.id
                comp_name = composer.name
                db_to_link_composers_to_movies.loc[(movie_id, comp_id), :] = {'movie_name': movie_name,
                                                                              'movie_revenue': movie_revenue,
                                                                              'composer_name': comp_name,
                                                                              'release_date': release_date}
        else:
            pass

    return db_to_link_composers_to_movies


def movie_selection_over_revenue(df: pd.DataFrame, min_revenue: int, max_revenue: int) -> pd.DataFrame:
    """Description"""
    movie_in_revenue_range = df.movie_revenue.apply(lambda r: True if min_revenue <= r <= max_revenue else False)
    return df[movie_in_revenue_range]


def genre_distribution_over_movies(df: pd.DataFrame) -> pd.DataFrame:
    """Description"""
    # Delete movies with no information about their genre
    movies_with_info = df.genres.apply(lambda g: False if not g else True)  # check if g is empty
    df = df[movies_with_info]
    number_of_movies = len(df.groupby('movie_name'))  # TODO: Discuss with the group the pertinence to tell the user on
    # how many movies the current analysis is done.

    # Initialize the new database
    genre_distribution = pd.DataFrame(
        columns=['genre', 'count']
    )
    # Set the index to be unique (pair of ids)
    genre_distribution.set_index(['genre'], inplace=True)

    # Retrieve all genre that exist in the df
    genres_dict = dict()
    for _, genres in df.genres.items():
        for genre in genres:
            if genre in genres_dict.keys():
                genres_dict[genre] += 1
            else:
                genres_dict[genre] = 1

    return pd.DataFrame(list(genres_dict.items()), columns=['genre', 'count'])
