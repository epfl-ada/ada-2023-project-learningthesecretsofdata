"""
Answer sixth question of project:
Does having a personal website correlate with the composers' success ?
"""

import pandas as pd
from helpers import get_composers


def get_composer_success_and_website():
    """Get a dataframe with the composers' success and website information"""
    # Get each composer's website (if they have one) and drop the release date column, since it is not relevant
    composers_website = get_composers().drop(columns=['release_date'])
    # Drop eventual duplicates
    composers_website.drop_duplicates(inplace=True)
    # Keep only the used columns
    composers_website = composers_website[['id', 'box_office_revenue', 'homepage']]
    # Map the homepage column to a boolean value
    composers_website_agg = composers_website.groupby('id').agg(
        total_box_office=pd.NamedAgg(column='box_office_revenue', aggfunc='sum'),
        has_website=pd.NamedAgg(column='homepage',
                                aggfunc=lambda x: any(x.notna()))
    ).reset_index()
    composers_website_agg['website'] = (composers_website_agg['has_website']
                                        .apply(lambda x: 'Has a website' if x else 'Does not have a website'))
    return composers_website_agg
