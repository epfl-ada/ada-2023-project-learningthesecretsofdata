import asyncio

import pandas as pd

from spotify.SpotifyAPI import SpotifyAPI
import time


async def get_music_dataset(composers_names: list):
    """
    This function is used to create the spotify_dataset.pickle file

    Parameters
    ----------
    composers_names: list
        List of composers names

    Returns
    -------
    None
    """
    async with SpotifyAPI() as spotify:
        start_time = time.time()

        result = await spotify.append_music(composers_names)

        end_time = time.time()

        print(f'Elapsed time: {end_time - start_time}')

        # Finally create a pickle file of this new dataframe
        # pickle, as it takes less space on disk
        result.to_csv('dataset/spotify_dataset.pickle')


if __name__ == '__main__':
    m = pd.read_pickle('dataset/clean_enrich_movies.pickle')
    list_composers = m['composers'].dropna().tolist()
    list_composers = [item for sublist in list_composers for item in sublist]
    composers_names = [c.name for c in list_composers]
    composers_names = list(set(composers_names))
    asyncio.run(get_music_dataset(composers_names))


