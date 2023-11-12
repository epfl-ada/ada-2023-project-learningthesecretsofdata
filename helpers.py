import json

import pandas as pd


def insight(x: pd.DataFrame) -> pd.DataFrame:
    """Look at the dataframe and return a well-structured DataFrame resuming information of x.

    Parameters
    ----------
    x: dataframe to gain insight

    Returns
    -------
    Insight of dataframe x
    """
    # TODO: Create a function returning all information asked by TAs. Refer to notebook, first Markdown.
    return x.info(), x.describe()


def load_movies(movie_metadata_path: str) -> pd.DataFrame:
    """Load movie metadata dataframe

    Parameters
    ----------
    movie_metadata_path: path of the movie.metadata.tsv

    Returns
    -------
    Loaded dataframe of movie metadata
    """

    movies_df = pd.read_csv(movie_metadata_path, sep='\t', names=['wiki_movieID', 'freebase_movieID', 'name',
                                                                  'release_date', 'box_office_revenue', 'runtime',
                                                                  'languages', 'countries', 'genres'], header=None)

    return movies_df.convert_dtypes()


def clean_movies(df: pd.DataFrame) -> pd.DataFrame:
    """Clean the movies dataframe to keep only relevant observation

    Parameters
    ----------
    df: movie metadate dataframe

    Returns
    -------
    Cleaned version of dataframe
    """
    # retain only the features we'll use
    df_used_features = df[['name', 'release_date', 'box_office_revenue', 'countries', 'genres']].copy()
    # map the dictionaries to list of values, since we do not use the Freebase IDs
    for dic in ['countries', 'genres']:
        df_used_features[dic] = df_used_features[dic].apply(json.loads)
        df_used_features[dic] = df_used_features[dic].apply(dict.values)
        df_used_features[dic] = df_used_features[dic].apply(list)
    # drop NaNs
    df_no_nans = df_used_features.dropna().copy()
    # keep only the year of the release date
    reg_map = lambda d: d.group(0)[:4]
    reg = r"\d{4}-\d{2}(-\d{2})?"
    df_no_nans['release_date'] = df_no_nans['release_date'].str.replace(reg, reg_map, regex=True)
    df_no_nans['release_date'].astype('int', copy=False)

    # we want the tuple (name, release date) to be unique
    # if two movies with the same name were released the same year, keep the one with the biggest box office revenue
    df_no_nans.sort_values(by='box_office_revenue', axis='rows', ascending=False, inplace=True)
    df_no_nans.drop_duplicates(subset=['name', 'release_date'], keep='first', inplace=True)

    return df_no_nans.reset_index(drop=True)


def main():
    df = clean_movies(load_movies('dataset/MovieSummaries/movie.metadata.tsv'))
    df.info()
    df.to_csv("dataset/tmp.csv")


if __name__ == "__main__":
    main()
