import json
from googleapiclient.discovery import build  # Import necessary libraries for YouTube Data API
import os
import boto3
from datetime import datetime

# Function to fetch raw statistics data for a given YouTube channel ID
def get_channel_stats_raw(youtube, channel_id):
    # Construct and execute API request
    request = youtube.channels().list(
        part = 'snippet,contentDetails,statistics',
        id = channel_id) 
    response = request.execute()
    return response

# Function to retrieve video IDs from a given playlist ID
def get_video_ids(youtube, playlist_id):
    # Construct and execute API request to fetch playlist items
    request = youtube.playlistItems().list(
                        part = 'contentDetails',
                        playlistId = playlist_id,
                        maxResults = 50) 
    response = request.execute()

    # Extract video IDs from the response
    video_ids = []
    for i in range(len(response['items'])):
        video_ids.append(response['items'][i]['contentDetails']['videoId']) 
        
    # Check for additional pages of playlist items and retrieve video IDs
    next_page_token = response.get('nextPageToken')
    more_pages = True
    
    while more_pages:
        if next_page_token is None:
            more_pages = False
        else:
            request = youtube.playlistItems().list(
                        part = 'contentDetails',
                        playlistId = playlist_id,
                        maxResults = 50,
                        pageToken = next_page_token) 
            response = request.execute()

        for i in range(len(response['items'])):
            video_ids.append(response['items'][i]['contentDetails']['videoId']) 
        next_page_token = response.get('nextPageToken')
        
    return video_ids
    
# Function to retrieve details of videos using their IDs
def get_video_details(youtube, video_ids):
    all_data = []  # Initialize list to store video details
    
    # Iterate over video IDs in chunks of 50 (API limit)
    for i in range(0, len(video_ids), 50):
        # Make API request to fetch details for the current chunk of video IDs
        request = youtube.videos().list(
            part='snippet,statistics',
            id=','.join(video_ids[i:i+50])
        )
        response = request.execute()
        
        # Append video details to the list
        all_data.append(response)
    
    return all_data

# Function to retrieve comments data for given video IDs
def get_comments_data(youtube, video_ids):
    all_comments = []  # Initialize list to store comments
    
    # Iterate over video IDs to fetch comments
    for video_id in video_ids:
        try:
            # Initial request to retrieve first page of comments for the video
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                textFormat="plainText",
                maxResults=100
            )
            response = request.execute()
            all_comments.append(response)  # Append raw JSON response to the list

            # Check for additional pages of comments and fetch them
            while "nextPageToken" in response:
                request = youtube.commentThreads().list(
                    part="snippet",
                    videoId=video_id,
                    textFormat="plainText",
                    maxResults=100,
                    pageToken=response["nextPageToken"]
                )
                response = request.execute()
                all_comments.append(response)
        except HttpError as e:
            # Handle HTTP errors, such as when comments are disabled for a video
            if e.resp.status == 403:
                print(f"Comments are disabled for video with ID: {video_id}. Skipping comments retrieval.")
                continue
            else:
                raise e

    return all_comments

# Lambda handler function
def lambda_handler(event, context):
    # Fetch environment variable containing Google API key
    client_id = os.environ.get('client_id')
    # List of YouTube channel IDs to process
    channel_ids = ['UCChmJrVa8kDg05JfCmxpLRw', 'UCbTggJVf0NDTfWX-C_gUGSg']
    youtube = build('youtube', 'v3', developerKey=client_id)  # Build YouTube API client
    client = boto3.client('s3')  # Initialize S3 client

    # Process each channel ID in the list
    for channel_id in channel_ids:
        # Retrieve raw channel data
        channel_raw_data = get_channel_stats_raw(youtube, channel_id)
        filename_channel = "channel_raw_data_" + str(datetime.now()) + ".json"
        # Store raw channel data in S3
        client.put_object(
            Bucket="youtube-etl-njk",
            Key="raw_data/to_processed/channel_data/" + filename_channel,
            Body=json.dumps(channel_raw_data)
        )
        
        # Extract playlist ID from raw channel data
        playlist_id = channel_raw_data['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        
        # Get video IDs using the playlist ID
        video_ids_duplicate = get_video_ids(youtube, playlist_id)
        video_ids = list(set(video_ids_duplicate))
        
        # Get video details
        all_videos_raw_data = get_video_details(youtube, video_ids)
        filename_video = "video_raw_data_" + str(datetime.now()) + ".json"
        # Store raw video data in S3
        client.put_object(
            Bucket="youtube-etl-njk",
            Key="raw_data/to_processed/video_data/" + filename_video,
            Body=json.dumps(all_videos_raw_data)
        )
        
        # Get comments data
        comments_raw_data = get_comments_data(youtube, video_ids)
        filename_comments = "comments_raw_data_" + str(datetime.now()) + ".json"
        # Store raw comments data in S3
        client.put_object(
            Bucket="youtube-etl-njk",
            Key="raw_data/to_processed/comments_data/" + filename_comments,
            Body=json.dumps(comments_raw_data)
        )

    # Start AWS Glue job run
    glue = boto3.client("glue")
    gluejobname = "YT_Data_Transformation_Spark"
    try:
        runId = glue.start_job_run(JobName=gluejobname)
        status = glue.get_job_run(JobName=gluejobname, RunId=runId['JobRunId'])
        print("Job Status :", status['JobRun']['JobRunState'])
    except Exception as e:
        print(e)
