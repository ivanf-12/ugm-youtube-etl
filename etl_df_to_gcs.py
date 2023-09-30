from googleapiclient.discovery import build
import pandas as pd
import warnings
import isodate
import json
from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
warnings.filterwarnings('ignore')

@task(retries=3)
def get_channel_stats(youtube, channel_id):
    request = youtube.channels().list(
                part='snippet, contentDetails, statistics',
                id=channel_id)
    response = request.execute()
    all_data = []
    
    for i in range(len(response['items'])):
        data = dict(Channel_name = response['items'][i]['snippet']['title'],
                    Creation_date = response['items'][i]['snippet']['publishedAt'],
                    Subscribers = response['items'][i]['statistics']['subscriberCount'],
                    Viewers = response['items'][i]['statistics']['viewCount'],
                    Total_videos = response['items'][i]['statistics']['videoCount'],
                    Playlist_id = response['items'][i]['contentDetails']['relatedPlaylists']['uploads'],
                    Thumbnail = response['items'][i]['snippet']['thumbnails']['high']['url'])

        all_data.append(data)
    
    return all_data 

@task(retries=3)
def get_video_ids(youtube, playlist_id):
    
    request = youtube.playlistItems().list(
                part='contentDetails',
                playlistId = playlist_id,
                maxResults = 50)
    response = request.execute()
    
    video_ids = []
    
    for i in range(len(response['items'])):
        video_ids.append(response['items'][i]['contentDetails']['videoId'])
    
    next_page_token = response.get('nextPageToken')
    more_pages = True
    
    while more_pages:
        if next_page_token is None:
            more_pages = False
        else:
            request = youtube.playlistItems().list(
                part='contentDetails',
                playlistId = playlist_id,
                maxResults = 50,
                pageToken = next_page_token)
            response = request.execute()
            
            for i in range(len(response['items'])):
                video_ids.append(response['items'][i]['contentDetails']['videoId'])
        
            next_page_token = response.get('nextPageToken')
    
    return video_ids

@task(retries=3)
def get_video_details(youtube, video_ids):
    all_video_stats = []
    for i in range(0, len(video_ids), 50):
        request = youtube.videos().list(
                    part = 'snippet, statistics, contentDetails',
                    id = ','.join(video_ids[i:i+50]))
        response = request.execute()
        for video in response['items']:
            video_stats = dict(Title = video['snippet']['title'],
                               CategoryId = video['snippet']['categoryId'],
                               Tags = video['snippet'].get('tags', 'No Tag'),
                               Published_date = video['snippet']['publishedAt'],
                               Duration = video['contentDetails']['duration'],
                               Views = video['statistics'].get('viewCount', 0),
                               Likes = video['statistics'].get('likeCount', 0),
                               Comments = video['statistics'].get('commentCount', 0),
                               Thumbnail = video['snippet']['thumbnails']['high']['url'])
            all_video_stats.append(video_stats)
    return all_video_stats

@flow(retries=3)
def fetch(youtube, channel_id):
    """Scrape data using the youtube data api"""

    channel_stats = get_channel_stats(youtube, channel_id)
    channel_df = pd.DataFrame(channel_stats)

    video_ids = get_video_ids(youtube, channel_df['Playlist_id'].iloc[0])
    video_details = get_video_details(youtube, video_ids)
    videos_df = pd.DataFrame(video_details)
    return channel_df, videos_df

@task(log_prints=True)
def clean_channel(channel_df = pd.DataFrame):
    """Fix channel stats data types"""

    channel_df['Subscribers'] = pd.to_numeric(channel_df['Subscribers'])
    channel_df['Viewers'] = pd.to_numeric(channel_df['Viewers'])
    channel_df['Total_videos'] = pd.to_numeric(channel_df['Total_videos'])
    channel_df['Creation_date'] = pd.to_datetime(channel_df['Creation_date']).dt.date
    return channel_df

@task(log_prints=True)
def clean_videos(videos_df = pd.DataFrame):
    """Fix video stats data types"""

    videos_df['Views'] = pd.to_numeric(videos_df['Views'])
    videos_df['Likes'] = pd.to_numeric(videos_df['Likes'])
    videos_df['Comments'] = pd.to_numeric(videos_df['Comments'])
    videos_df['Published_date'] = pd.to_datetime(videos_df['Published_date']).dt.date
    videos_df['DurationInSeconds'] = videos_df['Duration'].apply(lambda x: isodate.parse_duration(x).total_seconds())
    videos_df['CategoryId'] = pd.to_numeric(videos_df['CategoryId'])

    # Map each video category id to category title

    file = open('categories.json')
    category_json = json.load(file)
    category_dict = {}
    n = len(category_json['items'])
    for n in range(n):
        id = int(category_json['items'][n]['id'])
        title = category_json['items'][n]['snippet']['title']
        category_dict[id] = title

    videos_df['CategoryTitle'] = videos_df['CategoryId'].map(category_dict)
    return videos_df

@task(log_prints=True)
def write_local(df: pd.DataFrame, dataset_file:str):
    """Write dataframe out locally as csv file"""
    path = Path(f'data/{dataset_file}.csv')
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, compression='gzip', index=False)
    return path

@task()
def write_gcs(path: Path):
    """Uploading local file to gcs"""
    gcs_block = GcsBucket.load("ugm-bucket", validate=False)
    gcs_block.upload_from_path(
        from_path=f'{path}',
        to_path=path
    )
    return

@flow(log_prints=True)
def etl_web_to_gcs():
    """The main ETL function"""
    api_key = input('Enter API key: ')
    channel_id = 'UC3ItFj6jCtobDv1JgD9xcTg' # UGM's Youtube Channel

    youtube = build('youtube', 'v3', developerKey=api_key)

    channel_df, videos_df = fetch(youtube, channel_id)
    channel_df_clean = clean_channel(channel_df)
    videos_df_clean = clean_videos(videos_df)
    channel_path = write_local(channel_df_clean, 'channel_stats_data')
    videos_path = write_local(videos_df_clean, 'videos_stats_data')
    write_gcs(channel_path)
    write_gcs(videos_path)

if __name__ == '__main__':
    etl_web_to_gcs()
