"""
Answer sixth question of project:
Does having a personal website correlate with the composers' success ?
"""

import pandas as pd
from helpers import get_composers


def get_composer_success_and_website():
    """Get a dataframe with the composers' success and website information"""
    # Get each composer's popularity and name
    composers_success = pd.read_pickle('dataset/spotify_composers_dataset.pickle')[['name', 'popularity']]

    # Get each composer's website (if they have one) and drop the release date column, since it is not relevant
    composers_website = get_composers().drop(columns=['release_date'])
    # Drop eventual duplicates
    composers_website.drop_duplicates(inplace=True)
    # Keep only the used columns
    composers_website = composers_website[['name', 'homepage']]
    # Map the homepage column to a boolean value
    composers_website['has_homepage'] = ~composers_website['homepage'].isna()
    # Drop the homepage column
    composers_website.drop(columns=['homepage'], inplace=True)

    # Merge both dataframes
    composers_success_and_website = composers_success.merge(composers_website, on='name', how='inner')
    # Drop the name column since it is now useless
    composers_success_and_website.drop(columns=['name'], inplace=True)

    return composers_success_and_website
