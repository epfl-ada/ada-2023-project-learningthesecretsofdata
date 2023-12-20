"""
Answer second question of project:
What is the average composer's age at their :
   - first movie appearance ?
   - biggest box office revenue ?
"""
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from question_script.question_helper import extract_composers_dataframe


def calculate_composer_age_fst_appearance(movies: pd.DataFrame, filter_outlier: bool = True) -> pd.DataFrame:
    """
    Calculate and append the composer's age at their first movie appearance
    Calculate the age in days and in years

    Parameters
    ----------
    movies: The movie dataframe
    filter_outlier: Whether to filter out the outliers or not.

    Returns
    -------
    The dataframe of the composers with the new columns [c_age_first_appearance_days, c_age_first_appearance_years]
    """

    result = movies.copy()

    # Get composers, and only keep first row as we are interested in composers attributes which have been duplicated
    # in each group, so only need first one
    result = extract_composers_dataframe(result, True).apply(lambda row: row.iloc[0])[
        ['c_birthday', 'c_date_first_appearance']]

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


def calculate_composer_age_highest_box_office(movies: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate and append the composer's age at their highest box office revenue
    Calculate the age in days and in years

    Parameters
    ----------
    movies: The movie dataframe

    Returns
    -------
    The dataframe of the composers with the new columns [c_age_highest_revenue_days, c_age_highest_revenue_years]
    """

    result = movies.copy()

    result = extract_composers_dataframe(result, True)

    result = result.apply(
        lambda df_by_id: df_by_id.sort_values(by='box_office_revenue', ascending=False).iloc[0])

    result['release_date'] = pd.to_datetime(result.release_date)

    result.dropna(subset=['c_birthday', 'release_date'], inplace=True)

    # TO exclude composers such as Vivaldi or Mozart that are too old to be meaningful
    result.query('c_birthday > 1900', inplace=True)

    result[['c_age_highest_revenue_days', 'c_age_highest_revenue_years']] = \
        result.apply(lambda row: ((row['release_date'] - row['c_birthday']).days,
                                  (row['release_date'] - row['c_birthday']).days / 365.25),
                     axis='columns', result_type='expand')

    return result


def get_average_age_first_appearance(composer_df: pd.DataFrame) -> (float, float):
    """
    Calculate the average age of the composer at his first appearance. Return the average in days and in years

    Parameters
    ----------
    composer_df: The dataframe containing the composer's age information

    Returns
    -------
    The tuple of the average age in days and in years
    """
    return composer_df.c_age_first_appearance_days.mean(), composer_df.c_age_first_appearance_years.mean()


def get_average_age_high_box_office(composer_df: pd.DataFrame) -> (float, float, float):
    """
    Calculate the average age of the composer at his highest box office revenue. Return the average in days and in years

    Parameters
    ----------
    composer_df: The dataframe containing the age information

    Returns
    -------
    The tuple of the average age in days and in years
    """
    return composer_df.c_age_highest_revenue_days.mean(), composer_df.c_age_highest_revenue_years.mean()


def plot_composer_by_age_range(composer_df: pd.DataFrame, x_data: str, bin_nb: int = 50, kde: bool = True) -> None:
    """
    Plot the histogram of the count of composers in a certain age range

    Parameters
    ----------
    composer_df: The dataframe containing the composer's age information
    x_data: The name of the column to use as the x-axis data
    bin_nb: the number of bin to use for the histogram
    kde: Whether to plot the kernel density estimate or not

    """
    age_fst_appearance_years = composer_df[x_data]

    ax = sns.histplot(age_fst_appearance_years, bins=bin_nb, kde=kde)
    ax.lines[0].set_color('black')
    plt.title('Number of composers by age range')
    plt.ylabel('Count of composers')
    plt.xlabel('Age at first appearance')
    plt.show()
