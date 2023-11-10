from os.path import join, dirname
from dotenv import dotenv_values

# Config object that store environment variables from .enf file
config = {
    **dotenv_values(join(dirname(__file__), '.env'))
}
