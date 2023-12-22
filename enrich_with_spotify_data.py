import asyncio
import os
import time

import numpy as np
import pandas as pd
from rapidfuzz import fuzz

# from question_script.question1 import create_db_to_link_composers_to_movies
from spotify import get_bearer_token
from spotify.SpotifyDataLoader import SpotifyDataLoader

# Define keywords to search for soundtrack of movies
POSITIVE_KEYWORD = ["original", "motion", "picture", "soundtrack", "music", "band", "score", "theme", "ost", "ost.",
                    "album", "composed", "conducted"]
NEGATIVE_KEYWORD = ["game", "video", "television", "series", "show", "episode", "season", "episode", "seasons",
                    "remastered", "remaster", "live", "bonus"]
NEUTRAL_KEYWORD = ["the", "of", "from", "in", "on", "at", "for", "a", "an", "and", "or", "with", "by", "to", "version",
                   "vol", "vol.", "pt", "pt.", "part", "part.", "ver", "ver.", "&"]

# Define the influence of each keyword
POSITIVE_INFLUENCE = 1.1
NEGATIVE_INFLUENCE = 0.9

BATCH_SIZE = 100


def _regenerate_token_if_needed(timer, spotify):
    """
    Regenerate token if timer is more than 3000 seconds
    """

    if time.time() - timer > 3000:
        print("Regenerate token")
        get_bearer_token.replace_token("")
        spotify.reload_config()
        return time.time()
    return timer


def count_occurrence_and_return_diff(movie_name: str, query_words: list[str], keyword_list: list[str]) -> tuple[
    int, list]:
    """
    Count the number of occurence of the keyword in the movie name and return the difference between the query words

    Parameters
    ----------
    movie_name: str
        the movie name
    query_words: list
        the query words
    keyword_list: list
        the list of keywords

    Returns
    -------
    count: int
    words: list
    """
    words = []
    count = 0
    movie_words = movie_name.lower().split()
    for word in keyword_list:
        if word not in movie_words:
            words.append(word)
            if word in query_words:
                count += 1
    return count, words


def score_best_matching_albums(albums_df: pd.DataFrame, date: int, name: str, composer: str) -> list[tuple[int, int]]:
    """
    Score the best matching albums with the movie name

    Parameters
    ----------
    albums_df: pd.DataFrame
        the dataframe of albums
    date: int
        the date of the movie
    name: str
        the name of the movie
    composer: str
        the name of the composer

    Returns
    -------
    score: list[tuple(int, int)]
    """
    score = []
    for j in range(len(albums_df.values)):
        artist_bool = False
        album = albums_df.loc[j]

        if composer:
            for artist in album["artists"]:
                if fuzz.ratio(composer, artist["name"]) > 85 or "Various Artists" == artist["name"]:
                    artist_bool = True
                    break
            if not artist_bool:
                continue

        if date:
            if not (str(date) in (str(album["release_date"])) or str(int(date) - 1) in (
                    str(album["release_date"])) or str(int(date) + 1) in (str(album["release_date"]))):
                continue

        movie_name = name.lower()
        query_name = album["name"].lower()
        if not ("(" in movie_name or ")" in movie_name):
            query_name = query_name.replace("(", "")
            query_name = query_name.replace(")", "")

        query_words = query_name.split()

        pos_count, positive_words = count_occurrence_and_return_diff(movie_name, query_words, POSITIVE_KEYWORD)
        neg_count, negative_words = count_occurrence_and_return_diff(movie_name, query_words, NEGATIVE_KEYWORD)
        neu_count, neutral_words = count_occurrence_and_return_diff(movie_name, query_words, NEUTRAL_KEYWORD)

        to_remove = positive_words + negative_words + neutral_words
        cleaned_query = [word for word in query_words if word.lower() not in to_remove]
        result = ' '.join(cleaned_query)

        if not (max(movie_name.split(), key=len) in result):
            continue

        modifiers = POSITIVE_INFLUENCE ** pos_count * NEGATIVE_INFLUENCE ** neg_count
        score += [(j, modifiers * fuzz.ratio(movie_name, result))]
    return score


async def get_album_ids_into_df(movie_names_and_date: pd.DataFrame, checkpoint: bool = False,
                                save_interval: int = 5) -> pd.DataFrame:
    """
    This function is used to create the movie_album_and_revenue.pickle file

    Parameters
    ----------
    movie_names_and_date: pd.DataFrame
        the dataframe of movies

    checkpoint: bool
        if True, load the checkpoint and save the dataframe every save_interval

    save_interval: int
        the interval to save the dataframe

    Returns
    -------
    movie_albums_df: pd.DataFrame
    """
    checkpoint_path = 'dataset/checkpoints/movie_album_and_revenue.pickle'

    movie_albums_df = movie_names_and_date
    movie_albums_df['album_id'] = None

    if checkpoint:
        # Load the checkpoint if it exists
        if os.path.isfile(checkpoint_path):
            movie_albums_df = pd.read_pickle(checkpoint_path)

    mask = movie_albums_df["album_id"].isna()
    working_index = movie_albums_df[mask].index

    start_time = time.time()
    timer = start_time

    # Get the album ids for each movie
    async with SpotifyDataLoader() as spotify:
        for i in range(0, len(working_index), BATCH_SIZE):
            # Get all the albums for the movies in the batch
            batch = movie_albums_df.loc[working_index[i:i + BATCH_SIZE]]
            date = list(batch.release_date)
            names = list(batch.movie_name)
            composer = list(batch.composer_name)

            # if timer more than 1 hour, regenerate token
            timer = _regenerate_token_if_needed(timer, spotify)

            results = await spotify.search_albums_by_name(names)
            for j, albums in enumerate(results):
                albums_df = pd.DataFrame(albums)
                scores = score_best_matching_albums(albums_df, date[j], names[j], composer[j])
                if len(scores) > 0:
                    best_score = max(scores, key=lambda x: x[1])
                    movie_albums_df.loc[working_index[i + j], "album_id"] = albums_df.loc[best_score[0]]["id"]

                    if checkpoint and j % save_interval == 0:
                        movie_albums_df.to_pickle(checkpoint_path)

    end_time = time.time()

    print(f'Elapsed time for mapping album ids to film: {end_time - start_time}')

    # Save the dataframe
    movie_albums_df.to_pickle('dataset/movie_album_and_revenue.pickle')
    movie_albums_df.to_csv('dataset/movie_album_and_revenue.csv')

    return movie_albums_df


async def get_track_ids_into_df(movie_albums_df: pd.DataFrame, checkpoint: bool = False,
                                save_interval: int = 5) -> pd.DataFrame:
    """
    This function is used to create the movie_album_and_revenue_with_track_ids.pickle file

    Parameters
    ----------
    movie_albums_df: pd.DataFrame
        the dataframe of movies

    checkpoint: bool
        if True, load the checkpoint and save the dataframe every save_interval

    save_interval: int
        the interval to save the dataframe

    Returns
    -------
    movie_albums_df: pd.DataFrame
    """

    checkpoint_path = 'dataset/checkpoints/movie_album_and_revenue_with_track_ids.pickle'
    movie_albums_df['track_ids'] = None

    if checkpoint:
        # Load the checkpoint if it exists
        if os.path.isfile(checkpoint_path):
            movie_albums_df = pd.read_pickle(checkpoint_path)

    mask = movie_albums_df["track_ids"].isna()
    working_index = movie_albums_df[mask].index

    start_time = time.time()
    timer = start_time

    async with SpotifyDataLoader() as spotify:
        for i in range(0, len(working_index), BATCH_SIZE):
            # Get all the tracks ids of the albums in the batch
            batch = list(movie_albums_df.loc[working_index[i:i + BATCH_SIZE]]["album_id"])
            results = await spotify.get_albums_tracks_async(batch)
            movie_albums_df.loc[working_index[i:i + BATCH_SIZE], "track_ids"] = np.array(results, dtype=object)
            timer = _regenerate_token_if_needed(timer, spotify)
            if checkpoint and i % save_interval == 0:
                movie_albums_df.to_pickle(checkpoint_path)

    end_time = time.time()

    print(f'Elapsed time for retrieving all track_ids from album_ids: {end_time - start_time}')

    # Save the dataframe
    movie_albums_df.to_pickle('dataset/movie_album_and_revenue_with_track_ids.pickle')
    movie_albums_df.to_csv('dataset/movie_album_and_revenue_with_track_ids.csv')

    return movie_albums_df


async def get_music_from_track_ids(albums_with_track_ids: pd.DataFrame, checkpoint: bool = False,
                                   save_interval: int = 10) -> pd.DataFrame:
    """
    This function is used to create the movie_album_and_revenue_with_track_ids.pickle file

    Parameters
    ----------
    albums_with_track_ids: pd.DataFrame
        the dataframe of albums

    checkpoint: bool
        if True, load the checkpoint and save the dataframe every save_interval

    save_interval: int
        the interval to save the dataframe

    Returns
    -------
    albums_with_track_ids: pd.DataFrame
    """
    # Create new dataframe with the same columns as movie_names_and_date and an additional column for the album id
    albums_with_track_ids['track'] = albums_with_track_ids.get('track', pd.Series(dtype='object'))
    checkpoint_path = 'dataset/checkpoints/album_id_and_musics.pickle'

    if checkpoint:
        # Load the checkpoint if it exists
        if os.path.isfile(checkpoint_path):
            albums_with_track_ids = pd.read_pickle(checkpoint_path)
            mask = albums_with_track_ids["track_ids"].str.len() != 22
            albums_with_track_ids = albums_with_track_ids[~mask]

    mask = albums_with_track_ids["track"].isna()
    working_index = albums_with_track_ids[mask].index

    start_time = time.time()
    timer = start_time

    start_time = time.time()
    async with SpotifyDataLoader() as spotify:
        # Define the batch size
        batch_size = 250  # You can change this value as needed

        # Calculate the number of batches
        unique_keys = working_index.unique()
        num_batches = int(np.ceil(len(unique_keys) / batch_size))

        # Iterate over each batch
        for batch_num in range(num_batches):
            # Get the start and end index for the current batch
            start_idx = batch_num * batch_size
            end_idx = start_idx + batch_size

            # Get the keys for the current batch
            batch_keys = unique_keys[start_idx:end_idx]
            tracks, genres = await spotify.get_tracks_from_tracks_ids(albums_with_track_ids["track_ids"][batch_keys],
                                                                      genre=False)
            timer = _regenerate_token_if_needed(timer, spotify)
            for batch in tracks:
                for track in batch["tracks"]:
                    genre = []
                    music = spotify.get_music_from_track(track, genre)
                    # Use .loc for setting the value
                    albums_with_track_ids.loc[albums_with_track_ids["track_ids"] == music.id, "track"] = music

            if checkpoint:
                albums_with_track_ids.to_pickle(checkpoint_path)

    end_time = time.time()

    print(f'Elapsed time for retrieving all music objects from track_ids: {end_time - start_time}')

    albums_with_track_ids.to_pickle('dataset/album_id_and_musics.pickle')
    albums_with_track_ids.to_csv('dataset/album_id_and_musics.csv')

    return albums_with_track_ids


def create_musics_dataset():
    # Load the data
    spotify_composers_dataset = pd.read_pickle('dataset/spotify_composers_dataset.pickle')
    clean_enrich_movies = pd.read_pickle('dataset/clean_enrich_movies.pickle')

    composers_to_movies = create_db_to_link_composers_to_movies(clean_enrich_movies)

    box_office_and_composer_popularity = pd.merge(left=spotify_composers_dataset,
                                                  right=composers_to_movies,
                                                  left_on='name',
                                                  right_on='composer_name',
                                                  how='inner')[
        ['movie_name', 'movie_revenue', 'composer_name', 'release_date', 'popularity']]

    movie_names_and_date = box_office_and_composer_popularity[
        ["movie_name", "release_date", "movie_revenue", "composer_name"]]

    if os.path.isfile("dataset/movie_album_and_revenue.pickle"):
        movie_albums_df = pd.read_pickle("dataset/movie_album_and_revenue.pickle")
    else:
        get_bearer_token.replace_token("")
        movie_albums_df = asyncio.run(get_album_ids_into_df(movie_names_and_date, checkpoint=True, save_interval=1))

    # clean the dataframe
    movie_albums_df = movie_albums_df.dropna(subset=['album_id'])
    movie_albums_df = movie_albums_df.drop_duplicates(subset=['movie_name'])

    if os.path.isfile("dataset/movie_album_and_revenue_with_track_ids.pickle"):
        movie_albums_df = pd.read_pickle("dataset/movie_album_and_revenue_with_track_ids.pickle")
    else:
        get_bearer_token.replace_token("")
        asyncio.run(get_track_ids_into_df(movie_albums_df, checkpoint=True, save_interval=1))

    # clean the dataframe
    movie_albums_df = movie_albums_df.dropna(subset=['track_ids'])
    movie_albums_df = movie_albums_df.drop_duplicates(subset=['movie_name'])

    # Create a dataframe only containing the album id and the track ids
    albums_with_tracks = movie_albums_df.explode('track_ids')
    albums_with_tracks = albums_with_tracks[["album_id", "track_ids"]]
    mask = albums_with_tracks["track_ids"].str.len() != 22
    albums_with_tracks = albums_with_tracks[~mask]

    if os.path.isfile("dataset/album_id_and_musics.pickle"):
        print("Enrichment already done!!")
    else:
        # Get the music object from track ids
        get_bearer_token.replace_token("")
        asyncio.run(get_music_from_track_ids(albums_with_tracks, checkpoint=True, save_interval=1))

    print("Enrichment done!!")


def create_db_to_link_composers_to_movies(movies: pd.DataFrame) -> pd.DataFrame:
    """Description if needed"""
    # Initialize the new database
    db_to_link_composers_to_movies = pd.DataFrame(
        columns=['tmdb_id', 'comp_id', 'movie_name', 'movie_revenue', 'composer_name', 'release_date',
                 'composer_place_of_birth']
    )
    # Set the index to be unique (pair of ids)
    db_to_link_composers_to_movies.set_index(['tmdb_id', 'comp_id'], inplace=True)

    # Description TODO
    for _, movie in movies.iterrows():
        movie_id = movie['tmdb_id']
        movie_name = movie['name']
        movie_revenue = movie['box_office_revenue']
        composers = movie['composers']
        release_date = movie['release_date']

        if type(composers) == list:  # meaning we have information about composers, otherwise float nan returned
            for composer in composers:
                comp_id = composer.id
                comp_name = composer.name
                comp_place_of_birth = composer.place_of_birth
                db_to_link_composers_to_movies.loc[(movie_id, comp_id), :] = \
                    {'movie_name': movie_name,
                     'movie_revenue': movie_revenue,
                     'composer_name': comp_name,
                     'release_date': release_date,
                     'composer_place_of_birth': comp_place_of_birth}
        else:
            pass

    return db_to_link_composers_to_movies


if __name__ == '__main__':
    create_musics_dataset()
