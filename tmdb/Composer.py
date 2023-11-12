from dataclasses import dataclass


@dataclass
class Composer:
    """
    Data class that represent a composer
    """
    id: str
    name: str
    birthday: str = None
    gender: int = None
    homepage: str = None
    place_of_birth: str = None
    first_appearance_in_movie: str = None
