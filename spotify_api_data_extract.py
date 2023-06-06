import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import datetime

def lambda_handler(event, context):
    # pull the client id & secret from the env variable
    client_id = os.environ.get('client_id')
    client_secret = os.environ.get('client_secret')
    
    #authenticating the spotipy client
    auth_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    sp = spotipy.Spotify(auth_manager=auth_manager)
    
    #playlist link
    playlist_link ='https://open.spotify.com/playlist/37i9dQZF1DXcBWIGoYBM5M'
    playlist_uri=playlist_link.split('/')[-1]
    
    #extract data from spotify api
    spotify_data=sp.playlist_tracks(playlist_uri)
    
    #connect s3 client
    cilent = boto3.client('s3')
    
    #generate filename using the datetime.now() function
    date_string = f'{datetime.now():%Y%m%d%H%M%S%z}'
    filename = "spotify_raw_" + str(date_string) + ".json"
    
    #dump the extracted data in s3 bucket
    cilent.put_object(
        Bucket="spotify-etl-project-arungopi",
        Key="raw_data/to_processed/" + filename,
        Body=json.dumps(spotify_data)
        )