"""
Answer first question of project:
Which are the most frequent music genre appearing in movies ?
"""
import pandas as pd


def question_1(movie_music_genre_df: pd.DataFrame, min_revenue: int, max_revenue: int,
               soundtrack_in_genre: bool = True) -> pd.DataFrame:
    """Create a dataframe classifying the music genre in function of given parameters

    :param movie_music_genre_df: dataframe with [genre, movie_names, box_office_revenue]
    :param min_revenue: allows to filter some movies (private information to see if relevant difference to show in web)
    :param max_revenue: allows to filter some movies (private information to see if relevant difference to show in web)
    :param soundtrack_in_genre: tell the function to display the genres w/ (or w/o) "soundtrack" in it.

    :returns: Dataframe with sorted music genre in function of their number of appearance in movies.
    """
    selected_movie_music_genre_df = _movie_selection_over_revenue(df=movie_music_genre_df,
                                                                  min_revenue=min_revenue,
                                                                  max_revenue=max_revenue)

    genre_count = _genre_distribution_over_movies(selected_movie_music_genre_df).sort_values(by='count',
                                                                                             ascending=False)

    if soundtrack_in_genre:
        return genre_count
    else:
        return genre_count[~genre_count["genre"].str.contains('soundtrack', case=False)]


def _movie_selection_over_revenue(df: pd.DataFrame, min_revenue: int, max_revenue: int) -> pd.DataFrame:
    """private function to select movie over their box office revenue"""
    movie_in_revenue_range = df.box_office_revenue.apply(lambda r: True if min_revenue <= r <= max_revenue else False)
    return df[movie_in_revenue_range]


def _genre_distribution_over_movies(df: pd.DataFrame) -> pd.DataFrame:
    """private function to compute the genre distribution"""
    # Delete movies with no information about their genre
    movies_with_info = df.genres.apply(lambda g: False if not g else True)  # check if g is empty
    df = df[movies_with_info]

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
