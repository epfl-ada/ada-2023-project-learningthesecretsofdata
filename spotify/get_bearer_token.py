"""
This script allows you to get a bearer token from Spotify's API.
Be careful, the token expires after 1 hour.
"""

import requests

from config import config

if __name__ == '__main__':
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
    # WARNING : You need to delete the previous access token in the .env file
    access_token = auth_response_data['access_token']
    file = open('../.env', 'a')
    file.write(f'SPOTIFY_ACCESS_TOKEN="{access_token}"\n')
    file.close()