import streamlit as st
from streamlit_option_menu import option_menu
import pymongo
import pymysql
import sqlalchemy
import mysql.connector
from sqlalchemy import create_engine
import pandas as pd
import isodate
import time
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

API_KEY = 'Your API Key'
youtube = build('youtube', 'v3', developerKey=API_KEY)

# MongoDB connection
client = pymongo.MongoClient("mongodb+srv://saranya:@cluster0.z8lhc9m.mongodb.net/?retryWrites=true&w=majority")
db = client.MY_YOUTUBET_PROJECT
collection = db.YOUTUBE_channel_data

# MySQL connection 
connect = mysql.connector.connect(
    host="127.0.0.1",
    user="root",
    password="",
    database="youtube_project_harvest"
)
mycursor = connect.cursor(buffered=True)
engine = create_engine('mysql+pymysql://root:@127.0.0.1/youtube_project_harvest?charset=utf8mb4', echo=False)

# Putting Data into MongoDB
def Mongo_Data(pd_youtube):
    push_status = collection.insert_one(pd_youtube)
    return push_status

total_api_requests = 0

# Function to make API 
def make_api_request_with_rate_limit(channel_id, page_token=None):
    global total_api_requests

    
    if total_api_requests >= 10000:  
        st.error("API rate limit exceeded. Please try again later.")
        return None

    # API request
    api_request = youtube.channels().list(
        part='snippet,contentDetails,statistics,status',
        id=channel_id,
        pageToken=page_token  
    )

    try:
        
        response = api_request.execute()
        total_api_requests += 1


        remaining_requests = int(response.headers['X-RateLimit-Remaining'])

        
        if remaining_requests == 0:
            reset_time = int(response.headers['X-RateLimit-Reset'])
            sleep_time = reset_time - time.time()
            if sleep_time > 0:
                st.warning(f"Rate limit exceeded. Sleeping for {sleep_time} seconds.")
                time.sleep(sleep_time)

        return response
    except HttpError as e:
        if e.resp.status == 404:
            return {"error": f"Channel not found for ID: {channel_id}"}
        else:
            return {"error": f"API Error: {e.resp.status}"}

# Getting Channel name:
def channel_names():
    ch_name = []
    for i in collection.find():
        if 'data' in i and 'channel_name' in i['data']:
            ch_name.append(i['data']['channel_name'])
    return ch_name


def get_channel_details(channelid):
    ch_data = {}

    try:
        # Fetching channel information from the YouTube API
        ch_response = youtube.channels().list(
            part='snippet,contentDetails,statistics,status',
            id=channelid
        ).execute()

        if 'items' in ch_response and ch_response['items']:
            channel_data = ch_response['items'][0]
            channelName = channel_data['snippet']['title']
            ch_data[channelName] = {
                "channel_id": channel_data['id'],
                "channel_name": channel_data['snippet']['title'],
                "Subscribers": channel_data['statistics']['subscriberCount'],
                "video_count": channel_data['statistics']['videoCount'],
                "channel_views": channel_data['statistics']['viewCount'],
                "channel_description": channel_data["snippet"]["description"],
                "channel_status": channel_data['status']['privacyStatus'],
                "Playlist": {},
                "Videos": {},
                "Comments": {}
            }

            # Fetching playlists associated with the channel
            playlist_Response = youtube.playlists().list(
                part="snippet,contentDetails",
                channelId=channelid,
                maxResults=25
            ).execute()

            for playlist in playlist_Response['items']:
                playlist_id = playlist['id']
                ch_data[channelName]["Playlist"][playlist_id] = {
                    "playlist_id": playlist_id,
                    "channel_id": playlist['snippet']['channelId'],
                    "playlist_title": playlist['snippet']['title'],
                    "videos": []  # Initialize an empty list to store video IDs
                }

            # Fetching videos for each playlist
            for playlist_id in ch_data[channelName]["Playlist"]:
                video_list = youtube.playlistItems().list(
                    part="contentDetails",
                    playlistId=playlist_id,
                    maxResults=5
                ).execute()

                for video_item in video_list['items']:
                    video_id = video_item['contentDetails']['videoId']
                    ch_data[channelName]["Playlist"][playlist_id]["videos"].append(video_id)

            # Fetching video details for each video
            for playlist_id in ch_data[channelName]["Playlist"]:
                for video_id in ch_data[channelName]["Playlist"][playlist_id]["videos"]:
                    video_details = youtube.videos().list(
                        part="snippet,contentDetails,statistics",
                        id=video_id
                    ).execute()

                    if 'items' in video_details and video_details['items']:
                        video_data = video_details['items'][0]
                        vid_dict = {
                            "video_id": video_id,
                            "channel_id": channelid,
                            "video_name": video_data['snippet']['title'],
                            "video_description": video_data['snippet']['description'],
                            "published_at": video_data['snippet']['publishedAt'].replace("T", " ").replace("Z", ""),
                            "view_count": video_data['statistics']['viewCount'],
                            "like_count": video_data['statistics']['likeCount'],
                            "channel_name": channelName,
                            "comment_count": video_data['statistics']['commentCount'],
                            "duration": isodate.parse_duration(video_data['contentDetails']['duration']).total_seconds(),
                            "thumbnail": video_data['snippet']['thumbnails']['default']['url'],
                            "caption_status": video_data['contentDetails']['caption'],
                            "comments": []  # Initialize an empty list to store comment data
                        }
                        ch_data[channelName]["Videos"][video_id] = vid_dict

                        # Fetching comments for each video
                        comment_dict = youtube.commentThreads().list(
                            part="snippet,replies",
                            videoId=video_id,
                            maxResults=15
                        ).execute()

                        for comment_item in comment_dict['items']:
                            comment_id = comment_item['snippet']['topLevelComment']['id']
                            com_dict = {
                                "channel_id": channelid,
                                "Video_id": video_id,
                                "Comment_Id": comment_id,
                                "Comment_Text": comment_item['snippet']['topLevelComment']['snippet']['textOriginal'],
                                "Comment_Author": comment_item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                                "Comment_PublishedAt": comment_item['snippet']['topLevelComment']['snippet']['publishedAt'].replace("T", " ").replace("Z", "")
                            }
                            ch_data[channelName]["Videos"][video_id]["comments"].append(com_dict)

    except HttpError as e:
        if e.resp.status == 404:
            return {"error": f"Channel not found for ID: {channelid}"}
        else:
            return {"error": f"API Error: {e.resp.status}"}

    return {"Channel_Name": ch_data[channelName]['channel_name'], 'data': ch_data[channelName]}




# migrate data to SQL
def migrate_to_sql(channel_name):
    channel_data = collection.find({"Channel_Name": channel_name})[0]

    # Migrate channel data to MySQL
    channel_df = pd.DataFrame([[channel_data["data"]["channel_name"], channel_data["data"]["channel_id"],
                                channel_data["data"]["channel_views"], channel_data["data"]["channel_description"],
                                channel_data["data"]["video_count"], channel_data["data"]["channel_status"]]],
                              columns=["Channel_Name", "Channel_Id", "Channel_Views", "Channel_Description",
                                       "Total_videos", "Channel_status"])

    channel_df.to_sql('channel', engine, if_exists='append', index=False,
                      dtype={"Channel_Name": sqlalchemy.types.VARCHAR(length=225),
                             "Channel_Id": sqlalchemy.types.VARCHAR(length=225),
                             "Channel_Views": sqlalchemy.types.BigInteger,
                             "Channel_Description": sqlalchemy.types.TEXT})

    # Migrate playlist data to MySQL
    playlist_data = []
    for key, val in channel_data["data"]["Playlist"].items():
        playlist_data.append([key, val["channel_id"], val["playlist_title"]])

    playlist_df = pd.DataFrame(playlist_data, columns=["Playlist_Id", "Channel_Id", "Playlist_name"])
    playlist_df.to_sql('playlist', engine, if_exists='append', index=False,
                       dtype={"Playlist_Id": sqlalchemy.types.VARCHAR(length=225),
                              "Channel_Id": sqlalchemy.types.VARCHAR(length=225),
                              "Playlist_name": sqlalchemy.types.VARCHAR(length=225)})

    # Migrate video data to MySQL
    video_data = []
    for key, val in channel_data["data"]["Videos"].items():
        video_data.append([key, val['channel_id'], val["video_name"], val["channel_name"], val["video_description"],
                           val["published_at"], val["view_count"], val["like_count"], val["comment_count"],
                           val["duration"], val["caption_status"]])

    video_df = pd.DataFrame(video_data, columns=["video_id", "channel_id", "video_name", "channel_name",
                                                 "video_description", 'published_date', 'view_count', 'like_count',
                                                 'comment_count', 'duration', 'caption_status'])
    video_df.to_sql('video', engine, if_exists='append', index=False,
                    dtype={'video_id': sqlalchemy.types.VARCHAR(length=225),
                           'channel_id': sqlalchemy.types.VARCHAR(length=225),
                           'channel_name': sqlalchemy.types.VARCHAR(length=225),
                           'video_name': sqlalchemy.types.VARCHAR(length=225),
                           'video_description': sqlalchemy.types.TEXT,
                           'published_date': sqlalchemy.types.String(length=50),
                           'view_count': sqlalchemy.types.BigInteger,
                           'like_count': sqlalchemy.types.BigInteger,
                            'comment_count': sqlalchemy.types.Integer,
                           'duration': sqlalchemy.types.VARCHAR(length=1024),
                           'caption_status': sqlalchemy.types.VARCHAR(length=225)})

    # Migrate comment data to MySQL
    comment_data = []
    for key, val in channel_data["data"]["Comments"].items():
        comment_data.append([val["Video_id"], key, val["Comment_Text"], val["Comment_Author"],
                             val["Comment_PublishedAt"]])

    comment_df = pd.DataFrame(comment_data, columns=['Video_id', 'Comment_Id', 'Comment_Text', 'Comment_Author',
                                                     'Comment_Published_date'])
    comment_df.to_sql('comments', engine, if_exists='append', index=False,
                      dtype={'Video_Id': sqlalchemy.types.VARCHAR(length=225),
                             'Comment_Id': sqlalchemy.types.VARCHAR(length=225),
                             'Comment_Text': sqlalchemy.types.TEXT,
                             'Comment_Author': sqlalchemy.types.VARCHAR(length=225),
                             'Comment_Published_date': sqlalchemy.types.String(length=50)})

    return 0

#SLIDEBAR
with st.sidebar:
    selected = option_menu(None,
                           options=["DATA EXTRACTION AND MIGRATION", "SQL QUERIES"],
                           default_index=0)


if selected == "DATA EXTRACTION AND MIGRATION":
    st.title('YouTube Data Harvesting and Warehousing')
    extract, transfer = st.tabs(["EXTRACT", "MIGRATE"])

    with extract:
        st.title("Enter the Channel IDs")
        ch_id = st.text_input("Enter one or more YouTube Channel IDs separated by commas")
        srch_btn = st.button("Search")
        upld_button = st.button("Upload to MongoDB")

        if ch_id and srch_btn:
            channel_ids = [channel.strip() for channel in ch_id.split(',')]
            for channel_id in channel_ids:
                with st.spinner(f'Please wait while retrieving channel details for {channel_id}...'):
                    retrieved_data = get_channel_details(channel_id)
                    
                    st.write(retrieved_data)

        if upld_button:
            # Split the input channel IDs 
            channel_ids = [channel.strip() for channel in ch_id.split(',')]
            for channel_id in channel_ids:
                cursor = collection.find()
                ds = [item["data"]["channel_id"] for item in cursor if "data" in item and "channel_id" in item["data"]]

                # Check the channel IDs already exist in MongoDB
                

                if channel_id in ds:
                    st.error(f"The channel with ID {channel_id} already exists in MongoDB.")
                else:
                    channel_info = get_channel_details(channel_id.strip())
                    pushed_to_mongo = Mongo_Data(channel_info)
                    if pushed_to_mongo.acknowledged:
                        st.success(f'Data for Channel ID {channel_id.strip()} has been inserted into MongoDB!')
                    else:
                        st.error(f'Data for Channel ID {channel_id.strip()} was not inserted into MongoDB.')

    with transfer:
        st.title("Select a Channel to Transfer to MySQL")
        ch_name = channel_names()
        channel_name = st.selectbox("Select a channel", ch_name)
        migrate_button = st.button("Migrate to MySQL")

        if migrate_button:
            # Check if the selected channel exists in MongoDB
            cursor = collection.find({"data.channel_name": channel_name})
            cursor_count = sum(1 for _ in cursor)
            if cursor_count == 0:
                st.error(f"Channel '{channel_name}' not found in MongoDB.")
            else:
                migrate_to_sql(channel_name)



elif selected == "SQL QUERIES":
    st.write("Select a question from the list below to provide insights:")
    questions = st.selectbox(label='Questions', options=[
        '1. What are the names of all the videos and their corresponding channels?',
        '2. Which channels have the most number of videos, and how many videos do they have?',
        '3. What are the top 10 most viewed videos and their respective channels?',
        '4. How many comments were made on each video, and what are their corresponding video names?',
        '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
        '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
        '7. What is the total number of views for each channel, and what are their corresponding channel names?',
        '8. What are the names of all the channels that have published videos in the year 2022?',
        '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
        '10. Which videos have the highest number of comments, and what are their corresponding channel names?'])

    if questions == '1. What are the names of all the videos and their corresponding channels?':
        mycursor.execute("""SELECT video_name AS Video_Title, channel_name AS Channel_Name
                            FROM video""")
        df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
        df.index += 1
        st.write(df)
    elif questions == '2. Which channels have the most number of videos, and how many videos do they have?':
        mycursor.execute("""SELECT channel_name AS Channel_Name, COUNT(*) AS Total_Videos
                            FROM video
                            GROUP BY channel_name
                            ORDER BY Total_Videos DESC""")
        df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
        df.index += 1
        st.write(df)

    elif questions == '3. What are the top 10 most viewed videos and their respective channels?':
        mycursor.execute("""SELECT channel_name AS Channel_Name, video_name AS Video_Title, view_count AS Views 
                            FROM video
                            ORDER BY view_count DESC
                            LIMIT 10""")
        df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
        df.index += 1
        st.write(df)

    elif questions == '4. How many comments were made on each video, and what are their corresponding video names?':
        mycursor.execute("""SELECT video_name AS Video_Name, comment_count AS Total_Comments
                            FROM video""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        df.index+=1
        st.write(df)

    elif questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name AS Channel_Name, video_name AS Video_Name, like_count AS Total_Likes
                            FROM video
                            ORDER BY like_count DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        df.index+=1
        st.write(df)

    elif questions == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        mycursor.execute("""SELECT video_name AS Video_Name,like_count As Total_likes FROM video order by view_count ASC
                            """)
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        df.index+=1
        st.write(df)

    elif questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name AS Channel_Name, SUM(view_count) AS Total_Views
                            FROM video
                            GROUP BY channel_name""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        df.index+=1
        st.write(df)

    elif questions == '8. What are the names of all the channels that have published videos in the year 2022?':
        mycursor.execute("""SELECT DISTINCT channel_name AS Channel_Name
                            FROM video
                            WHERE YEAR(published_date) = 2022""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        df.index+=1
        st.write(df)

    elif questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name AS Channel_Name, AVG(duration) AS Average_Duration_In_Seconds
                            FROM video
                            GROUP BY channel_name""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        df.index+=1
        st.write(df)

    elif questions == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name AS Channel_Name, video_name AS Video_Name, comment_count AS Total_No_Of_Comments
                            FROM video
                            ORDER BY comment_count DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        df.index+=1
        st.write(df)


