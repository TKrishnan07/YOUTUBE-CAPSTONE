from googleapiclient.discovery import build
import psycopg2
import pandas as pd
import streamlit as st

# Connecting API-Key
def Api_connect():
    Api_Id = "AIzaSyAMdOMt5mwlsVBc8PQx0n3KDqg-BZpXjo0"
    api_service_name = "youtube"
    api_version = "v3"
    youtube = build(api_service_name, api_version, developerKey=Api_Id)
    return youtube

youtube = Api_connect()

# Getting Channel Info
def fetching_channel_info(channel_Id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_Id
    )
    response = request.execute()
    if 'items' in response and len(response['items']) > 0:
        i = response['items'][0]
        data = dict(
            channel_Name=i["snippet"].get("title", "N/A"),
            channel_Id=i.get("id", "N/A"),
            subscribers=i["statistics"].get("subscriberCount", "N/A"),
            views=i["statistics"].get("viewCount", "N/A"),
            Total_videos_count=i["statistics"].get("videoCount", "N/A"),
            Channel_Description=i["snippet"].get("description", "N/A"),
            playlist_Id=i["contentDetails"]["relatedPlaylists"].get("uploads", "N/A")
        )
        return data
    else:
        return {"error": "Channel not found or API response is invalid"}

# Getting Video ids
def get_videos_id(channel_id):
    Video_Ids = []
    response = youtube.channels().list(
        id=channel_id,
        part='contentDetails'
    ).execute()
    try:
        playlist_Id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    except (KeyError, IndexError):
        return {"error": "Invalid channel ID or unable to retrieve playlist ID"}

    next_page_token = None
    while True:
        response1 = youtube.playlistItems().list(
            part='snippet',
            playlistId=playlist_Id, 
            maxResults=50,
            pageToken=next_page_token
        ).execute()
        for item in response1.get('items', []):
            video_id = item['snippet'].get('resourceId', {}).get('videoId')
            if video_id:
                Video_Ids.append(video_id)
        next_page_token = response1.get('nextPageToken')
        if next_page_token is None:
            break
    return Video_Ids

# Get video information
def get_video_info(video_ids):
    Video_data = []
    for video_id in video_ids:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        response = request.execute()
        for item in response["items"]:
            data = dict(
                channel_Name=item['snippet'].get('channelTitle', 'N/A'),
                channel_Id=item['snippet'].get('channelId', 'N/A'),
                video_Id=item.get('id', 'N/A'),
                Title=item['snippet'].get('title', 'N/A'),
                Tags=item['snippet'].get('tags', []),
                Thumbnail=item['snippet']['thumbnails']['default'].get('url'),
                Description=item['snippet'].get('description', 'N/A'),
                Published=item['snippet'].get('publishedAt', 'N/A'),
                Duration=item['contentDetails'].get('duration', 'N/A'),
                Views=item['statistics'].get('viewCount', 'N/A'),
                Likes=item['statistics'].get('likeCount','N/A'),
                Comments=item['statistics'].get('commentCount', '0'),
                Favourite=item['statistics'].get('favoriteCount', 'N/A'),
                Definition=item['contentDetails'].get('definition', 'N/A'),
                Caption_Status=item['contentDetails'].get('caption', 'N/A')
            )
            Video_data.append(data)
    return Video_data

# Getting Comment info
def get_comment_info(video_ids):
    comment_data = []
    try:
        for video_id in video_ids:
            request = youtube.commentThreads().list(
                part='snippet',
                videoId=video_id,
                maxResults=50
            )
            response = request.execute()
            for item in response['items']:
                data = dict(
                    comment_id=item['snippet']['topLevelComment']['id'],
                    video_id=item['snippet']['topLevelComment']['snippet']['videoId'],
                    comment_text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                    comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt']
                )
                comment_data.append(data)
    except:
        pass
    return comment_data

# Getting Playlist Details
def get_playlist_info(channel_id):
    next_page_token = None
    playlist_data = []
    while True:
        request = youtube.playlists().list(
            part='snippet,contentDetails',
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token 
        )
        response = request.execute()
        for item in response['items']:
            data = dict(
                Playlist_Id=item['id'],
                Title=item['snippet']['title'],
                channel_Id=item['snippet']['channelId'],
                channel_name=item['snippet']['channelTitle'],
                Published_At=item['snippet']['publishedAt'],
                Video_Count=item['contentDetails']['itemCount']
            )
            playlist_data.append(data)
        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            break
    return playlist_data

# PostgreSQL connection details
def get_postgres_connection():
    return psycopg2.connect(
        host="localhost",
        user="postgres",
        password="Tk0407",
        database="youtube_data",
        port="5432"
    )

# Inserting data into PostgreSQL
def insert_channel_details(cursor, ch_details):
    for channel in ch_details:
        query = '''INSERT INTO channels (
                        channel_Name,
                        channel_Id,
                        subscribers,
                        views,
                        Total_videos,
                        Channel_Description,
                        playlist_Id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (channel_Id) DO NOTHING'''
        values = (
            ch_details['channel_Name'],
            ch_details['channel_Id'],
            ch_details['subscribers'],
            ch_details['views'],
            ch_details['Total_videos_count'],
            ch_details['Channel_Description'],
            ch_details['playlist_Id']
        )
        cursor.execute(query, values)


def insert_playlist_details(cursor, pl_details):
    for playlist in pl_details:
        query = '''INSERT INTO playlists (
                        Playlist_Id,
                        Title,
                        channel_Id,
                        channel_name,
                        Published_At,
                        Video_Count
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (Playlist_Id) DO NOTHING'''
        values = (
            playlist['Playlist_Id'],
            playlist['Title'],
            playlist['channel_Id'],
            playlist['channel_name'],
            playlist['Published_At'],
            playlist['Video_Count']
        )
        cursor.execute(query, values)


def insert_video_details(cursor, vi_details):
    for video in vi_details:
        query = '''INSERT INTO videos (
                        channel_Name,
                        channel_Id,
                        video_Id,
                        Title,
                        Tags,
                        Thumbnail,
                        Description,
                        Published,
                        Duration,
                        Views,
                        Likes,
                        Comments,
                        Favourite,
                        Definition,
                        Caption_Status
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (video_Id) DO NOTHING'''
        values = (
            video['channel_Name'],
            video['channel_Id'],
            video['video_Id'],
            video['Title'],
            video['Tags'],
            video['Thumbnail'],
            video['Description'],
            video['Published'],
            video['Duration'],
            video['Views'],
            video['Likes'],
            video['Comments'],
            video['Favourite'],
            video['Definition'],
            video['Caption_Status']
        )
        cursor.execute(query, values)

def insert_comment_details(cursor, com_details):
    for comment in com_details:
        query = '''INSERT INTO comments (
                        comment_id,
                        video_id,
                        comment_text,
                        comment_Author,
                        comment_Published
                    ) VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (comment_id) DO NOTHING'''
        values = (
            comment['comment_id'],
            comment['video_id'],
            comment['comment_text'],
            comment['comment_Author'],
            comment['comment_Published']
        )
        cursor.execute(query, values)


# Check if channel ID exists in the database
def channel_id_exists(channel_id):
    conn = get_postgres_connection()
    cursor = conn.cursor()
    query = "SELECT 1 FROM channels WHERE channel_Id = %s"
    cursor.execute(query, (channel_id,))
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    return result is not None


# Channel details function
def Channel_details(channel_id):
    ch_details = fetching_channel_info(channel_id)
    vi_id = get_videos_id(channel_id)
    pl_details = get_playlist_info(channel_id)
    vi_details = get_video_info(vi_id)
    com_details = get_comment_info(vi_id)
    
    conn = get_postgres_connection()
    cursor = conn.cursor()
    
    insert_channel_details(cursor, ch_details)
    insert_playlist_details(cursor, pl_details)
    insert_video_details(cursor, vi_details)
    insert_comment_details(cursor, com_details)
    
    conn.commit()
    cursor.close()
    conn.close()
    
    return "Upload Completed Successfully!!!"

# Creating tables in PostgreSQL
def create_tables():
    conn = get_postgres_connection()
    cursor = conn.cursor()

    create_channel_table = '''CREATE TABLE IF NOT EXISTS channels (
        channel_Name TEXT,
        channel_Id TEXT PRIMARY KEY,
        subscribers BIGINT,
        views BIGINT,
        Total_videos BIGINT,
        Channel_Description TEXT,
        playlist_Id TEXT
    )'''
    
    create_playlist_table = '''CREATE TABLE IF NOT EXISTS playlists (
        Playlist_Id TEXT PRIMARY KEY,
        Title TEXT,
        channel_Id TEXT,
        channel_name TEXT,
        Published_At TIMESTAMP,
        Video_Count BIGINT
    )'''
    
    create_video_table = '''CREATE TABLE IF NOT EXISTS videos (
        channel_Name TEXT,
        channel_Id TEXT,
        video_Id TEXT PRIMARY KEY,
        Title TEXT,
        Tags TEXT[],
        Thumbnail TEXT,
        Description TEXT,
        Published TIMESTAMP,
        Duration TEXT,
        Views BIGINT,
        Likes BIGINT,
        Comments BIGINT,
        Favourite BIGINT,
        Definition TEXT,
        Caption_Status TEXT
    )'''
    
    create_comment_table = '''CREATE TABLE IF NOT EXISTS comments (
        comment_id TEXT PRIMARY KEY,
        video_id TEXT,
        comment_text TEXT,
        comment_Author TEXT,
        comment_Published TIMESTAMP
    )'''
    
    cursor.execute(create_channel_table)
    cursor.execute(create_playlist_table)
    cursor.execute(create_video_table)
    cursor.execute(create_comment_table)
    
    conn.commit()
    cursor.close()
    conn.close()

# Streamlit Application
st.sidebar.title(":red[YouTube Data Harvesting and Warehousing]")
st.sidebar.header("Home")
st.markdown(
    """
    <style>
    .stApp {
        background-image: url("https://i.gifer.com/H0uT.gif");
        background-size: cover;
    }
    </style>
    """,
    unsafe_allow_html=True
)

def display_table(query):
    conn = get_postgres_connection()
    df = pd.read_sql(query, conn)
    st.dataframe(df)
    conn.close()


app_mode = st.sidebar.selectbox("Choose the app mode", ["Data Harvesting","Query Data"])
if app_mode == "Data Harvesting":
    st.title(":red[YouTube Data Harvesting]")
    channel_id = st.text_input("Enter the YouTube channel ID")
    if st.button("Scrap and Store Data"):
        if channel_id_exists(channel_id):
            st.success("Channel ID already exists")
        else:
            create_tables()
            insert = Channel_details(channel_id)
            st.success(insert)

    Display_table = st.radio("Select the Table to View", ("Channels", "Playlists", "Videos", "Comments"))

    if Display_table == "Channels":
        display_table("SELECT * FROM channels")
    elif Display_table == "Playlists":
        display_table("SELECT * FROM playlists")
    elif Display_table == "Videos":
        display_table("SELECT * FROM videos")
    elif Display_table == "Comments":
        display_table("SELECT * FROM comments")

# SQL Queries
elif app_mode == "Query Data":
    st.title(":red[Query Data]")
    st.header("SQL Queries")

    question = st.selectbox("Select your question", (
        "1. What are the names of all the videos and their corresponding channels?",
        "2. Which channels have the most number of videos, and how many videos do they have?",
        "3. What are the top 10 most viewed videos and their respective channels?",
        "4. How many comments were made on each video, and what are their corresponding video names?",
        "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
        "6. What is the total number of likes for each video, and what are their corresponding video names?",
        "7. What is the total number of views for each channel, and what are their corresponding channel names?",
        "8. What are the names of all the channels that have published videos in the year 2022?",
        "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
        "10. Which videos have the highest number of comments, and what are their corresponding channel names?"
    ))
    if question == "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?":
        query9 = '''
            SELECT channel_Name AS channelname, 
                    AVG(iso8601_to_seconds(Duration)) AS averageduration 
            FROM videos
            GROUP BY channel_Name
        '''
        conn = get_postgres_connection()
        cursor = conn.cursor()
        cursor.execute(query9)
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        
        df9 = pd.DataFrame(results, columns=["channelname", "averageduration"])
        df9['averageduration'] = pd.to_datetime(df9['averageduration'], unit='s').dt.strftime('%H:%M:%S')
        st.write(df9)

    else:
        query_dict = {
            "1. What are the names of all the videos and their corresponding channels?": "SELECT Title, channel_Name FROM videos",
            "2. Which channels have the most number of videos, and how many videos do they have?": "SELECT channel_Name, COUNT(*) FROM videos GROUP BY channel_Name ORDER BY COUNT(*) DESC",
            "3. What are the top 10 most viewed videos and their respective channels?": "SELECT Title, channel_Name, Views FROM videos ORDER BY Views DESC LIMIT 10",
            "4. How many comments were made on each video, and what are their corresponding video names?": "SELECT Title, Comments FROM videos order by Comments Desc",
            "5. Which videos have the highest number of likes, and what are their corresponding channel names?": "SELECT Title, channel_Name, Likes FROM videos ",
            "6. What is the total number of likes for each video, and what are their corresponding video names?": "SELECT Title, Likes FROM videos order by Likes Desc",
            "7. What is the total number of views for each channel, and what are their corresponding channel names?": "SELECT channel_Name, SUM(Views) FROM videos GROUP BY channel_Name ORDER BY SUM(Views) DESC",
            "8. What are the names of all the channels that have published videos in the year 2022?": "SELECT DISTINCT channel_Name,Published FROM videos WHERE EXTRACT(YEAR FROM Published) = 2022",
            "10. Which videos have the highest number of comments, and what are their corresponding channel names?": "SELECT Title, channel_Name, Comments FROM videos ORDER BY Comments DESC"
        }

        if st.button("Execute"):
            if question in query_dict:
                query = query_dict[question]
                display_table(query)
            else:
                st.error("Invalid query selection")
