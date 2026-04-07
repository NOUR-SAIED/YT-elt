from dotenv import load_dotenv
import os
import requests 
import json

load_dotenv(dotenv_path=".env") # to load environment variables from a .env file located in the current directory.          
CHANNEL_HANDLE=os.getenv("CHANNEL_HANDLE")
API_KEY=os.getenv("API_KEY")


url= f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY}"

def get_playlist_id():
    try:
        response=requests.get(url)

        response.raise_for_status() # to check if the request was successful, if not it will raise an HTTPError exception.  

        data=response.json()

        #print(json.dumps(data,indent=4)) # to convert a python object to a json like formatted string with indentation for better readability.

        channel_items=data["items"][0]
        channel_playlist_id=channel_items["contentDetails"]["relatedPlaylists"]["uploads"]
        #print(channel_playlist_id)
        return channel_playlist_id
    except requests.exceptions.RequestException as e:
        raise e
    
if __name__=="__main__":
    get_playlist_id()
   
