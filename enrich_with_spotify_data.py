import asyncio
import time

import numpy as np
import pandas as pd

from question_script.question1 import create_db_to_link_composers_to_movies
from spotify.SpotifyDataLoader import SpotifyDataLoader
from rapidfuzz import fuzz

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


def count_occurence_and_return_diff(movie_name: str, query_words: list[str], keyword_list: list[str]) -> tuple[
    int, list]:
    """
    Count the number of occurence of the keyword in the movie name and return the difference between the query words

    Parameters
    ----------
    movie_name: str
    query_words: list
    keyword_list: list

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
    date: int
    name: str
    composer: str

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

        pos_count, positive_words = count_occurence_and_return_diff(movie_name, query_words, POSITIVE_KEYWORD)
        neg_count, negative_words = count_occurence_and_return_diff(movie_name, query_words, NEGATIVE_KEYWORD)
        neu_count, neutral_words = count_occurence_and_return_diff(movie_name, query_words, NEUTRAL_KEYWORD)

        to_remove = positive_words + negative_words + neutral_words
        cleaned_query = [word for word in query_words if word.lower() not in to_remove]
        result = ' '.join(cleaned_query)

        if not (max(movie_name.split(), key=len) in result):
            continue

        modifiers = POSITIVE_INFLUENCE ** pos_count * NEGATIVE_INFLUENCE ** neg_count
        score += [(j, modifiers * fuzz.ratio(movie_name, result))]
    return score


async def get_album_ids_into_df(movie_names_and_date: pd.DataFrame) -> pd.DataFrame:
    """
    This function is used to create the movie_album_and_revenue.pickle file

    Parameters
    ----------
    movie_names_and_date: pd.DataFrame

    Returns
    -------
    movie_albums_df: pd.DataFrame
    """
    # Create new dataframe with the same columns as movie_names_and_date and an additional column for the album id
    columns = movie_names_and_date.columns.values
    columns = np.append(columns, "album_id")
    movie_albums_df = pd.DataFrame(columns=columns)

    start_time = time.time()

    # Get the album ids for each movie
    async with SpotifyDataLoader() as spotify:
        count = 0
        for i in range(0, len(movie_names_and_date), BATCH_SIZE):
            # Get all the albums for the movies in the batch
            batch = movie_names_and_date.loc[i:i + BATCH_SIZE]
            date = list(batch.release_date)
            names = list(batch.movie_name)
            composer = list(batch.composer_name)
            results = await spotify.search_albums_by_name(names)
            for j, albums in enumerate(results):
                albums_df = pd.DataFrame(albums)
                scores = score_best_matching_albums(albums_df, date[j], names[j], composer[j])
                if len(scores) > 0:
                    best_score = max(scores, key=lambda x: x[1])
                    row = movie_names_and_date.loc[i + j].copy()
                    row["album_id"] = albums_df.loc[best_score[0]]["id"]
                    movie_albums_df.loc[count] = row
                    count += 1

    end_time = time.time()

    print(f'Elapsed time for mapping album ids to film: {end_time - start_time}')

    # Save the dataframe
    movie_albums_df.to_pickle('dataset/movie_album_and_revenue.pickle')
    movie_albums_df.to_csv('dataset/movie_album_and_revenue.csv')

    return movie_albums_df


async def get_track_ids_into_df(movie_albums_df: pd.DataFrame) -> pd.DataFrame:
    """
    This function is used to create the movie_album_and_revenue_with_track_ids.pickle file

    Parameters
    ----------
    movie_albums_df: pd.DataFrame

    Returns
    -------
    movie_albums_df: pd.DataFrame
    """
    start_time = time.time()
    async with SpotifyDataLoader() as spotify:
        for i in range(0, len(movie_albums_df), BATCH_SIZE):
            # Get all the tracks ids of the albums in the batch
            batch = list(movie_albums_df.loc[i:i + BATCH_SIZE]["album_id"])
            results = await spotify.get_albums_tracks_async(batch)
            movie_albums_df.loc[i:i + BATCH_SIZE, "track_ids"] = np.array(results, dtype=object)
    end_time = time.time()

    print(f'Elapsed time for retrieving all tracks_ids from album_ids: {end_time - start_time}')

    # Save the dataframe
    movie_albums_df.to_pickle('dataset/movie_album_and_revenue_with_track_ids.pickle')
    movie_albums_df.to_csv('dataset/movie_album_and_revenue_with_track_ids.csv')

    return movie_albums_df


async def get_music_from_track_ids(albums_with_track_ids: pd.DataFrame) -> pd.DataFrame:
    """
    This function is used to create the movie_album_and_revenue_with_track_ids.pickle file

    Parameters
    ----------
    albums_with_track_ids: pd.DataFrame

    Returns
    -------
    albums_with_track_ids: pd.DataFrame
    """
    # Create new dataframe with the same columns as movie_names_and_date and an additional column for the album id
    albums_with_track_ids['track'] = albums_with_track_ids.get('track', pd.Series(dtype='object'))

    start_time = time.time()
    async with SpotifyDataLoader() as spotify:
        for key in albums_with_track_ids["track_ids"].keys().unique():
            tracks, genres = await spotify.get_tracks_from_tracks_ids(albums_with_track_ids["track_ids"][key],
                                                                      genre=True)
            for track in tracks:
                for genre in genres:
                    music = spotify.get_music_from_track(track, genre)
                    # Use .loc for setting the value
                    albums_with_track_ids.loc[albums_with_track_ids["track_ids"] == music.id, "track"] = music

    end_time = time.time()

    print(f'Elapsed time for retrieving all music objects from track_ids: {end_time - start_time}')

    albums_with_track_ids.to_pickle('dataset/album_id_and_musics.pickle')
    albums_with_track_ids.to_csv('dataset/album_id_and_musics.pickle.csv')

    return albums_with_track_ids


def main():
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

    movie_albums_df = asyncio.run(get_album_ids_into_df(movie_names_and_date))
    asyncio.run(get_track_ids_into_df(movie_albums_df))

    # Create a dataframe only containing the album id and the track ids
    albums_with_tracks = movie_albums_df.explode('track_ids')
    albums_with_tracks = albums_with_tracks[["album_id", "track_ids"]]

    # Get the music object from track ids
    asyncio.run(get_music_from_track_ids(albums_with_tracks))


if __name__ == '__main__':
    main()
