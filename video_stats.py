from dotenv import load_dotenv
import os
import requests 
import json

load_dotenv(dotenv_path=".env") # to load environment variables from a .env file located in the current directory.          
CHANNEL_HANDLE=os.getenv("CHANNEL_HANDLE")
API_KEY=os.getenv("API_KEY")
MaxResults=50


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
    
    
def get_video_ids(playlist_id):
    video_ids=[]
    pagetoken= None
    url=f"https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&playlistId={playlist_id}&maxResults={MaxResults}&key={API_KEY}"
    try:
        while True:
            if pagetoken:
                url+=f"&pageToken={pagetoken}"
            response=requests.get(url)
            response.raise_for_status()
            data=response.json()
            items=data["items"]
            for item in items:
                video_id=item["contentDetails"]["videoId"]
                video_ids.append(video_id)
            pagetoken=data.get("nextPageToken")
            if not pagetoken:
                break
        return video_ids
        
    except requests.exceptions.RequestException as e:
        raise e

def extract_video_data(video_id): 
    extracted_data=[]

    def batch_list(video_id_lst,batch_size):
        for i in range(0,len(video_id_lst),batch_size):
            yield video_id_lst[i:i+batch_size]    
    
    try:
        for batch in batch_list(video_id,MaxResults):
            video_id_str=",".join(batch)
            url=f"https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails&part=snippet&part=statistics&id={video_id_str}&key={API_KEY}"
            
            response=requests.get(url)
            response.raise_for_status()
            data=response.json()
            
            
            for item in data.get("items",[]):
                video_id=item["id"]
                snippet=item["snippet"]
                statistics=item["statistics"]
                contentdetails=item["contentDetails"]
                video_data={
                    "video_id":video_id,
                    "title":snippet.get("title"),
                    "publishedAt":snippet.get("publishedAt"),
                    "viewCount":statistics.get("viewCount",None), # to handle cases where viewCount might be missing in the API response, it will return None instead of raising a KeyError.    
                    "likeCount":statistics.get("likeCount",None), # to handle cases where likeCount might be missing in the API response, it will return None instead of raising a KeyError.
                    "commentCount":statistics.get("commentCount",None),
                    "duration":contentdetails.get("duration")
                }   
                extracted_data.append(video_data)
        return extracted_data 
        
    
    except requests.exceptions.RequestException as e:
        raise e
if __name__=="__main__":
    playlist_id=get_playlist_id()
    video_ids=get_video_ids(playlist_id)
    extract_video_data(video_ids)
    
   
