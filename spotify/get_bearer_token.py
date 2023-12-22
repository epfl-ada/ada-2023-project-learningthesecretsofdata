"""
This script allows you to get a bearer token from Spotify's API.
Be careful, the token expires after 1 hour.
"""

import os

import requests

from config import config


def replace_token(path="../"):
    client_id = config['SPOTIFY_CLIENT_ID']
    client_secret = config['SPOTIFY_CLIENT_SECRET']

    # Spotify URL for the Client Credentials auth flow
    auth_url = 'https://accounts.spotify.com/api/token'

    # POST to get the access token
    auth_response = requests.post(auth_url, {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
    })

    # Convert the response to JSON
    auth_response_data = auth_response.json()

    # Save the access token to .env file
    access_token = auth_response_data['access_token']
    with open(path + '.env', 'r') as old_env:
        with open(path + 'tmp', 'w') as new_env:
            for line in old_env:
                if not line.strip('\n').startswith('SPOTIFY_ACCESS_TOKEN'):
                    new_env.write(line)
            new_env.write(f'SPOTIFY_ACCESS_TOKEN="{access_token}"\n')
    os.replace(path + 'tmp', path + '.env')


if __name__ == '__main__':
    replace_token()
