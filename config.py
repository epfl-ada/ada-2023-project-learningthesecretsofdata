from os.path import join, dirname
from dotenv import dotenv_values

# Config object that store environment variables from .env file
config = {
    **dotenv_values(join(dirname(__file__), '.env'))
}


def reload_env_config():
    global config
    config = {
        **dotenv_values(join(dirname(__file__), '.env'))
    }
