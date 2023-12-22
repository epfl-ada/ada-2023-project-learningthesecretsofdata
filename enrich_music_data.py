import asyncio
import time

import pandas as pd

from spotify.SpotifyDataLoader import SpotifyDataLoader


async def get_music_dataset(composers_names: list) -> None:
    """
    This function is used to create the spotify_dataset.pickle file

    Parameters
    ----------
    composers_names: list ist of composers names
    """
    async with SpotifyDataLoader() as spotify:
        start_time = time.time()

        result = await spotify.create_composers_table(composers_names)

        end_time = time.time()

        print(f'Elapsed time: {end_time - start_time}')

        # Finally create a pickle file of this new dataframe, as it takes less space on disk
        result.to_pickle('dataset/spotify_composers_dataset.pickle')
        result.to_csv('dataset/spotify_composers_dataset.csv')


def create_music_composers_dataset():
    """
    Create the composer dataset
    """

    m = pd.read_pickle('dataset/clean_enrich_movies.pickle')
    list_composers = m['composers'].dropna().tolist()
    # Flatten the list
    list_composers = [item for sublist in list_composers for item in sublist]
    composers_names = [c.name for c in list_composers]
    composers_names = list(set(composers_names))
    asyncio.run(get_music_dataset(composers_names))


if __name__ == '__main__':
    create_music_composers_dataset()
