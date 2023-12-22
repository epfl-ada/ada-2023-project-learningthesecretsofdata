import json

import pandas as pd
from IPython.core.display_functions import display


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
    # drop NaNs excepts from box_office_revenue
    df_no_nans = df_used_features.dropna(subset=df_used_features.columns.difference(['box_office_revenue'])).copy()
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


def clean_movies_revenue(df: pd.DataFrame) -> pd.DataFrame:
    """Merge the revenue, by keeping the one from the cmu if present, otherwise keep the one retrieved from tmdb.
    then drop the nan values and return the result.

    Parameters
    ----------
    df: dataframe with the revenue retrieved from tmdb

    Returns
    -------
    The cleaned dataset
    """
    result = df.copy()
    result["box_office_revenue"] = result.agg(
        lambda x: x["box_office_revenue"] if not pd.isna(x["box_office_revenue"]) else x["tmdb_revenue"], axis=1)

    result = result.drop(["tmdb_revenue"], axis=1).dropna(subset='box_office_revenue')
    # sort by box_office_revenue
    result.sort_values(by='box_office_revenue', axis='rows', ascending=False, inplace=True)
    return result.reset_index(drop=True)


def insight(x: pd.DataFrame):
    """Display a structured and relevant insight of the current movie dataframe.

    Parameters
    ----------
    x: dataframe to gain insight

    Returns
    -------
    None
    """
    # Initialize DataFrame giving data type for all column.
    insight_df = pd.DataFrame(columns_type(x), index=x.columns, columns=["class"])

    # Add missing values information
    insight_df["missing_values"] = (x.isna().sum() / x.shape[0] * 100).round(2)

    display(insight_df)


def columns_type(x: pd.DataFrame) -> list:
    """Return the type of all columns of a pandas DataFrame.

    Parameters
    ----------
    x: dataframe to gain insight

    Returns
    -------
    List of all types
    """
    # Clean the rows where there is Nan values to not interfere the result
    x = x[~x.isnull().any(axis=1)]

    # Check each column type(s) (if few of them, all are return)
    columns_data_type = []
    for column in x.columns:
        column_data_types = x[column].apply(type)
        column_data_types = column_data_types.unique()
        columns_data_type.append(*column_data_types)

    return columns_data_type


def insight_enhance(x: pd.DataFrame):
    """Display a structured and relevant insight of the enhanced movie dataframe.

    Parameters
    ----------
    x: dataframe to gain insight

    Returns
    -------
    None
    """
    # Initialize the dataframe with statistical insight of box_office_revenue
    display(x.box_office_revenue.describe())

    # Gain insight over movies' composers
    composers = x.composers
    na_composers_sum = composers.isna().sum()

    # Only check for first composer of the list if multiple are returned
    composers_no_na = composers.dropna()
    composers_no_na_name = composers_no_na.transform(lambda c: c[0].name)
    composers_no_na_birthday = composers_no_na.transform(lambda c: c[0].birthday)
    composers_no_na_gender = composers_no_na.transform(lambda c: c[0].gender)
    composers_no_na_homepage = composers_no_na.transform(lambda c: c[0].homepage)
    composers_no_na_place_of_birth = composers_no_na.transform(lambda c: c[0].place_of_birth)
    composers_no_na_first_appearance_in_movie = composers_no_na.transform(lambda c: c[0].date_first_appearance)

    # Print result
    print(f'There is {na_composers_sum / len(composers) * 100:.2f}% of nan composers\n')
    print(
        'Considering the first composer of the list if multiple have been returned for a movie, we can compute the '
        'following statistics on the retrieved data:\n')
    print(
        f'\t - There is {composers_no_na_name.isna().sum() / len(composers_no_na_name) * 100:.2f}% '
        f'of nan name for composers')
    print(
        f'\t - There is {composers_no_na_birthday.isna().sum() / len(composers_no_na_birthday) * 100:.2f}% '
        f'of nan birthday for composers')
    print(
        f'\t - There is {composers_no_na_gender.isna().sum() / len(composers_no_na_gender) * 100:.2f}% '
        f'of nan gender for composers')
    print(
        f'\t - There is {composers_no_na_homepage.isna().sum() / len(composers_no_na_homepage) * 100:.2f}% '
        f'of nan homepage for composers')
    print(
        f'\t - There is {composers_no_na_place_of_birth.isna().sum() / len(composers_no_na_place_of_birth) * 100:.2f}% '
        f'of nan place of birth for composers')
    print(
        f'\t - There is {composers_no_na_first_appearance_in_movie.isna().sum() / len(composers_no_na_first_appearance_in_movie) * 100:.2f}% '
        f'of nan first appearance in movie for composers')
