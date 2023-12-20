import pandas as pd


def extract_composers_dataframe(df: pd.DataFrame, group_by_composer_id: bool = False) -> pd.DataFrame:
    """
    Extract the composers dataframe from the movies dataframe.

    Parameters
    ----------
    df: The movies dataframe
    group_by_composer_id: Whether to return the dataframe grouped by composers id

    Returns
    -------
    The composers dataframe with required colum dropped, and each composer's attribute extracted in its own column
    as: "c_id, c_name, c_birthday, c_gender, c_homepage, c_place_of_birth, c_date_first_appearance"
    """

    # The dropna make the copy itself
    exploded_df = df.dropna(subset='composers').explode('composers')

    (exploded_df['c_id'], exploded_df['c_name'], exploded_df['c_birthday'], exploded_df['c_gender'],
     exploded_df['c_homepage'], exploded_df['c_place_of_birth'], exploded_df['c_date_first_appearance']) = \
        zip(*exploded_df.composers.apply(
            lambda c: (c.id, c.name, c.birthday, c.gender, c.homepage, c.place_of_birth, c.date_first_appearance)
        ))

    # transform date column in date type
    exploded_df['c_birthday'] = pd.to_datetime(exploded_df.c_birthday)
    exploded_df['c_date_first_appearance'] = pd.to_datetime(exploded_df.c_date_first_appearance)

    exploded_df.drop('composers', axis='columns', inplace=True)

    if group_by_composer_id:
        # Only keep first occurrences of each composer
        exploded_df = exploded_df.groupby('c_id')

    return exploded_df
