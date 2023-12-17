"""
Answer fifth question of project:
Does composer's gender matter ?
"""

from helpers import get_composers


def get_cleaned_composers():
    """Return the dataset of composers with gender attribute cleaned"""
    composers = get_composers()
    # Drop the rows with 'undefined' gender
    cleaned_composers = composers[composers.gender != 0].copy()
    # Map the gender values to meaningful strings
    cleaned_composers['gender'].replace(to_replace={1: 'Female', 2: 'Male'}, inplace=True)
    return cleaned_composers
