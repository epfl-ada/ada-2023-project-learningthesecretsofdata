"""
Answer second question of project:
What is the average composer's age at their :
   - first movie appearance ?
   - biggest box office revenue ?
"""
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt


# TODO, refactor this function to extract explode logic and rest
def extract_composers_dataframe(df: pd.DataFrame, column_to_drop: list = None,
                                filter_duplicate: bool = True) -> pd.DataFrame:
    """
    Extract the composers dataframe from the movies dataframe.

    Parameters
    ----------
    df: The movies dataframe
    column_to_drop: List of columns to drop from the dataframe before returning the composers dataframe
    keep_duplicate: Whether to keep duplicated composers value

    Returns
    -------
    The composers dataframe
    """

    exploded_df = df.dropna(subset='composers').explode('composers')

    if filter_duplicate:
        # Only keep first occurrences of each composer
        exploded_df.drop_duplicates(subset='composers', inplace=True)

    (exploded_df['c_id'], exploded_df['c_name'], exploded_df['c_birthday'], exploded_df['c_gender'],
     exploded_df['c_homepage'], exploded_df['c_place_of_birth'], exploded_df['c_date_first_appearance']) = \
        zip(*exploded_df.composers.apply(
            lambda c: (c.id, c.name, c.birthday, c.gender, c.homepage, c.place_of_birth, c.date_first_appearance)
        ))

    if column_to_drop is not None:
        # Keep only the composers
        exploded_df.drop(columns=column_to_drop, inplace=True)
        # exploded_df.drop(columns=['box_office_revenue', 'name', 'genres', 'tmdb_id', 'release_date', 'countries'],
        #                 inplace=True)

    # transform date column in date type
    exploded_df['c_birthday'] = pd.to_datetime(exploded_df.c_birthday)
    exploded_df['c_date_first_appearance'] = pd.to_datetime(exploded_df.c_date_first_appearance)

    return exploded_df.drop('composers', axis='columns')


def calculate_composer_age_fst_appearance(composer_df: pd.DataFrame, filter_outlier: bool = True) -> pd.DataFrame:
    """
    Calculate and append the composer's age at their first movie appearance
    Calculate the age in days and in years

    Parameters
    ----------
    composer_df: The dataframe of the composers
    filter_outlier: Whether to filter out the outliers or not.

    Returns
    -------
    The dataframe of the composers with the new columns
    """

    result = composer_df.copy()
    result.dropna(subset=['c_birthday', 'c_date_first_appearance'], inplace=True)

    result[['c_age_first_appearance_days', 'c_age_first_appearance_years']] = \
        result.apply(lambda row: ((row['c_date_first_appearance'] - row['c_birthday']).days,
                                  (row['c_date_first_appearance'] - row['c_birthday']).days / 365.25),
                     axis='columns', result_type='expand')

    # Some composers have weird birthdate, which result in first appearance years being negative, so only
    # take positive values
    result.query('c_age_first_appearance_years > 0', inplace=True)

    if filter_outlier:
        result.query('c_age_first_appearance_years < 100', inplace=True)

    return result


def get_average_age_first_appearance_days(composer_df: pd.DataFrame) -> float:
    """
    Calculate the average age of the first appearance of a composers in a movie in days

    Parameters
    ----------
    composer_df: The dataframe containing the composer's age information

    Returns
    -------
    The average age for the first apparition of a composer in a movie
    """
    return composer_df.c_age_first_appearance_days.mean()


def get_average_age_first_appearance_years(composer_df: pd.DataFrame) -> float:
    """
    Calculate the average age of the first appearance of a composers in a movie in years

    Parameters
    ----------
    composer_df: The dataframe containing the composer's age information

    Returns
    -------
    The average age for the first apparition of a composer in a movie
    """
    return composer_df.c_age_first_appearance_years.mean()


def plot_composer_by_age_range(composer_df: pd.DataFrame, bin_nb: int = 50, kde: bool = True) -> None:
    """
    Plot the histogram of the count of composers in a certain age range

    Parameters
    ----------
    composer_df: The dataframe containing the composer's age information
    bin_nb: the number of bin to use for the histogram
    kde: Whether to plot the kernel density estimate or not

    -------

    """
    age_fst_appearance_years = composer_df.c_age_first_appearance_years

    ax = sns.histplot(age_fst_appearance_years, bins=bin_nb, kde=kde)
    ax.lines[0].set_color('black')
    plt.title('Number of composers by age range')
    plt.ylabel('Count of composers')
    plt.xlabel('Age at first appearance')
    plt.show()
