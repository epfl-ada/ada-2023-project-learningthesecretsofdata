from dataclasses import dataclass


@dataclass
class Composer:
    """
    Data class that represent a composer
    """
    id: str
    name: str
    birthday: str = None
    # 0 = Undefined, 1 = Female, 2 = Male
    gender: int = None
    homepage: str = None
    place_of_birth: str = None
    # First appearance of composer in movie credits
    date_first_appearance: str = None

    def __hash__(self):
        return hash(self.id) ^ hash(self.name)

    def __eq__(self, other):
        if isinstance(other, Composer):
            return self.id == other.id
        return False
