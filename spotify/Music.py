from dataclasses import dataclass


@dataclass
class Music:
    """
    Data class that represent a music track from Spotify presented in a movie
    """
    id: str
    name: str
    genre: list[str]
    composer_id: int
    popularity: int
