"""
Answer fifth question of project:
Does composer's gender matter ?
"""

import pandas as pd
from IPython.core.display_functions import display


def get_composers():
    """Get the composers from the cleaned and enriched movies data set"""
    # Load the enhanced movies data set
    enhanced_movies = pd.read_pickle('dataset/clean_enrich_movies.pickle')

    # Keep only the list of composers of each movie
    composers_list = enhanced_movies[['composers', 'release_date']]

    # Drop NaN values
    composers_list = composers_list.dropna()

    # Flatten the list of composers and rename accordingly
    composers = composers_list.explode('composers').rename(columns={'composers': 'composer'})

    # Reset the index since the shape of the dataframe has changed
    composers.reset_index(drop=True, inplace=True)

    # Map each attribute of the composer to a column in the dataframe
    composer_attributes = pd.DataFrame(composers['composer'].apply(lambda c: c.__dict__).tolist())

    # Replace the composer column with the newly created columns
    composers.drop(columns=['composer'], inplace=True)
    composers = composers.join(composer_attributes)

    return clean_composers_gender(composers)


def clean_composers_gender(composers):
    """Clean the gender attribute of the composers dataframe"""
    # Drop the rows with 'undefined' gender
    cleaned_composers = composers[composers.gender != 0].copy()
    # Map the gender values to meaningful strings
    cleaned_composers['gender'].replace(to_replace={1: 'Female', 2: 'Male'}, inplace=True)
    return cleaned_composers
