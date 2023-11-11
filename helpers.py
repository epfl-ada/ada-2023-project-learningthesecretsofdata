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

    movies_df = pd.read_csv(movie_metadata_path, sep='\t', names=['wiki_movieID', 'freebase_movieID', 'name',
                                                                  'release_date', 'box_office_revenue', 'runtime',
                                                                  'languages', 'countries', 'genres'], header=None)
    return movies_df


def clean_movies(df: pd.DataFrame) -> pd.DataFrame:
    df_used_features = df[['name', 'release_date', 'box_office_revenue', 'countries', 'genres']]
    df_no_nans = df_used_features.dropna().copy()

    reg_map = lambda d: d.group(0)[:4]
    reg = r"\d{4}-\d{2}(-\d{2})?"
    df_no_nans['release_date'] = df_no_nans['release_date'].str.replace(reg, reg_map, regex=True)
    df_no_nans['release_date'].astype('int', copy=False)
    #TODO: verify if (name, release_date) is a key (no duplicates) (it is not a key, need to handle duplicates)

    return df_no_nans.copy()

def main():
    df = clean_movies(load_movies())
    df = df.set_index(['name', 'release_date'])
    print(df)
    print(df.index.is_unique)

if __name__ == "__main__":
    main()