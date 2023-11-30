from helpers import *


def prepare_data_for_q3():
    # Load the cleaned and enriched data set (created via enrich_movie_data.py script)
    enhanced_movies = pd.read_pickle('dataset/clean_enrich_movies.pickle')

    # Drop all the columns which are not release date and composer
    movies = enhanced_movies[['release_date', 'composers', 'box_office_revenue']]

    # Drop all the rows which have no composer or no release date
    movies = movies.dropna()

    display(movies.head())
    print(movies.shape)

    # Create a column composer containing only one composer and add new rows for each composer
    movie_with_composer = movies.explode('composers')

    # Rename the column composers to composer
    movie_renamed = movie_with_composer.rename(columns={'composers': 'composer', 'release_date': 'release_year'})

    movie_renamed = movie_renamed.reset_index(drop=True)

    return movie_renamed


def group_by_composer_id(df):
    df['composer'] = df['composer'].apply(lambda x: x.id)

    # group by composer
    return df.groupby('composer')


def filter_by_top_composers(df, nb_top_composers=10):
    top_composers = df['composer'].value_counts().head(nb_top_composers).index

    # Keep only the top 10 composers with their index
    return df[df['composer'].isin(top_composers)]
