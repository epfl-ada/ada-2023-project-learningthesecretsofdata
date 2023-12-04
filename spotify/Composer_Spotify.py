from dataclasses import dataclass


@dataclass
class ComposerSpotify:
    """
    Data class that represent a composer from Spotify
    This class contains the composer id, name, genres and albums ids
    """
    id: str
    name: str
    genres: list[str]
    followers: int
    popularity: int
