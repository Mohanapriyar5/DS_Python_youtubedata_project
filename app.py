from googleapiclient.discovery import build
import pymongo
import mysql.connector
import pandas as pd
from datetime import datetime
from datetime import datetime
import streamlit as st
import pytz
import re


#API key connection
def api_con():
    api_service_name = "youtube"
    api_version = "v3"
    api_key = 'AIzaSyCKvoyDdRAZK6eB_qQiyEyNZR5Fqqzz8Sw'

    youtube = build(api_service_name, api_version, developerKey = api_key)
    return youtube
youtube = api_con()

#Getting channel details "UCEN7gaERUErJ1OCH5iFCBEQ""UCuI5XcJYynHa5k_lqDzAgwQ"
def get_channel_details(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    ) 
    response =request.execute()
    for i in response['items']:
        data = dict(Channel_name = i['snippet']['title'],
               Channel_id = i["id"],
               View_count = i['statistics']['viewCount'],
               Video_count = i['statistics']['videoCount'],
               Channel_des = i['snippet'].get('description'),
               Playlist_id = i['contentDetails']['relatedPlaylists']['uploads'],
               Sub_count = i['statistics']['subscriberCount'])
    return data

#Getting video id's
def get_video_ids(channel_id):
    video_ids = []
    response = youtube.channels().list(id = channel_id,
                                       part = "contentDetails").execute()
    Playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
    while True:
        request1 = youtube.playlistItems().list(
                                        part = "snippet",
                                        playlistId = Playlist_id,
                                        maxResults=50,
                                        pageToken=next_page_token).execute()
        for i in range(len(request1['items'])):
            video_ids.append(request1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = request1.get('nextPageToken')
        if next_page_token is None:
            break
    return video_ids

#Get video informations
def get_video_info(video_ids):
    video_info = []
    for video_id in video_ids:
        request = youtube.videos().list(
                                        part="snippet,contentDetails,statistics",
                                        id = video_id)
        response = request.execute()
        for item in response['items']:
            data = dict(channel_name = item['snippet']['channelTitle'],
                        channel_id = item['snippet']['channelId'],
                        video_id = item['id'],
                        video_name = item['snippet']['title'],
                        video_des = item['snippet'].get('description'),
                        published_date = item['snippet']['publishedAt'],
                        view_count = item['statistics'].get('viewCount'),
                        like_count = item['statistics'].get('likeCount'),
                        favorite_count = item['statistics']['favoriteCount'],
                        comment_count = item['statistics'].get('commentCount'),
                        duration = item['contentDetails']['duration'],
                        thumbnail = item['snippet']['thumbnails']['default']['url'],
                        caption_status = item['contentDetails']['caption'])
            video_info.append(data)
    return video_info
                            

#Getting comment details
def get_comment_details(video_ids):
    comment_data = []
    try:
        for video_id in video_ids:
            request = youtube.commentThreads().list(part="snippet",
                                                    videoId = video_id,
                                                    maxResults = 50)
            response = request.execute()
            for item in response['items']:
                data = dict(comment_id = item['snippet']['topLevelComment']['id'],
                            video_id = item['snippet']['topLevelComment']['snippet']['videoId'],
                            comment_txt = item['snippet']['topLevelComment']['snippet']['textDisplay'],
                            comment_author = item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            comment_published_date = item['snippet']['topLevelComment']['snippet']['publishedAt'])
                comment_data.append(data)
    except:
        pass
    return comment_data

#Mongodb connection and creating database
monconn = pymongo.MongoClient("mongodb://localhost:27017/")
db = monconn["youtube_data"]

#creating collection and inserting values into mongodb
def channel_details(channel_id):
    channel_info = get_channel_details(channel_id)
    video_ids_info = get_video_ids(channel_id)
    video_info = get_video_info(video_ids_info)
    comment_info = get_comment_details(video_ids_info)

    mycoll = db["channel_details"]
    mycoll.insert_one({"channel_details":channel_info,
                       "video_details":video_info,
                       "comment_details":comment_info
                      })
    return "Uploaded successfully"

#sql connection and creating table in sql
def channel_table():
    sqlconn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="root",
        database = "youtube_data")
    cursor = sqlconn.cursor()
    drop_query = 'DROP TABLE IF EXISTS channel;'
    cursor.execute(drop_query)
    sqlconn.commit()
    
    try:
        create_query = '''create table if not exists channel(Channel_id varchar(255) primary key,
                                                         Channel_name varchar(255),
                                                         Channel_des varchar(5000),
                                                         View_count bigint,
                                                         Video_count bigint,
                                                         Sub_count int,
                                                         Playlist_id varchar(255))'''
        cursor.execute(create_query)
        sqlconn.commit()
    except:
        print("channel table already created")
    
    #Extracting data from mongodb and converting it to dataframe i.e.,table format
    channel_lst = []
    db = monconn["youtube_data"]
    mycoll = db["channel_details"]
    for channel_data in mycoll.find({},{"_id":0,"channel_details":1}):
        channel_lst.append(channel_data["channel_details"])
    df = pd.DataFrame(channel_lst)
    
    #Getting values from dataframe and inserting the values into the table in mysql
    for index,row in df.iterrows():
        insert_query = '''insert into channel(Channel_name,
                                              Channel_id,
                                              View_count,
                                              Video_count,
                                              Channel_des,
                                              Playlist_id,
                                              Sub_count)values(%s,%s,%s,%s,%s,%s,%s)'''
        values = (row['Channel_name'],
                  row['Channel_id'],
                  row['View_count'],
                  row['Video_count'],
                  row['Channel_des'],
                  row['Playlist_id'],
                  row['Sub_count'])
        try:
            cursor.execute(insert_query,values)
            sqlconn.commit()
        except:
            print("channel values already instered")


def video_table():
    def duration_to_seconds(duration_str):
        match = re.match(r'PT(\d+H)?(\d+M)?(\d+S)?', duration_str)
        if match:
            hours = int(match.group(1)[:-1]) if match.group(1) else 0
            minutes = int(match.group(2)[:-1]) if match.group(2) else 0
            seconds = int(match.group(3)[:-1]) if match.group(3) else 0
            return hours * 3600 + minutes * 60 + seconds
        return 0

    def convert_published_date(published_date_str):
        published_date_utc = datetime.strptime(published_date_str, '%Y-%m-%dT%H:%M:%SZ')
        published_date_utc = published_date_utc.replace(tzinfo=pytz.utc)
    
        target_time_zone = pytz.timezone('America/New_York')
        published_date = published_date_utc.astimezone(target_time_zone).strftime('%Y-%m-%d %H:%M:%S')
        
        return published_date
    sqlconn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="root",
        database="youtube_data"
    )
    cursor = sqlconn.cursor()
    drop_query = 'DROP TABLE IF EXISTS video;'
    cursor.execute(drop_query)
    sqlconn.commit()

    try:
        create_query = '''CREATE TABLE IF NOT EXISTS video(
            channel_name VARCHAR(255),
            video_id VARCHAR(50) PRIMARY KEY,
            video_name VARCHAR(255),
            video_des VARCHAR(5000),
            published_date DATETIME,
            view_count BIGINT,
            like_count BIGINT,
            favorite_count INT,
            comment_count BIGINT,
            duration INT,
            thumbnail VARCHAR(150),
            caption_status VARCHAR(25)
        )'''
        cursor.execute(create_query)
        sqlconn.commit()
    except Exception as e:
        print("Error creating table:", e)

    video_lst = []
    db = monconn["youtube_data"]
    mycoll = db["channel_details"]
    for video_data in mycoll.find({}, {"_id": 0, "video_details": 1}):
        for i in range(len(video_data["video_details"])):
            video_lst.append(video_data["video_details"][i])
    df1 = pd.DataFrame(video_lst)

    # Inserting the DataFrame into the MySQL table
    for index, row in df1.iterrows():
        check_query = "SELECT * FROM video WHERE video_id = %s"
        cursor.execute(check_query, (row['video_id'],))
        existing_row = cursor.fetchone()

        if not existing_row:
            # If the row doesn't exist, insert it
            insert_query = '''INSERT INTO video (
                channel_name,
                video_id,
                video_name,
                video_des, 
                published_date,
                view_count,
                like_count,
                favorite_count,
                comment_count, 
                duration,
                thumbnail,
                caption_status
            ) VALUES (
                %(channel_name)s, %(video_id)s, %(video_name)s, %(video_des)s, 
                %(published_date)s, 
                %(view_count)s, %(like_count)s, %(favorite_count)s, %(comment_count)s, 
                %(duration)s, %(thumbnail)s, %(caption_status)s
            )'''

            try:
                row['published_date'] = convert_published_date(row['published_date'])
                row['duration'] = duration_to_seconds(row['duration'])
                cursor.execute(insert_query, row.to_dict())
                sqlconn.commit()
            except Exception as e:
                print("Error inserting row:", e)
       

def comment_table():
    print("Be4 mysql connection")
    sqlconn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="root",
            database = "youtube_data")
    cursor = sqlconn.cursor()
    drop_query = 'DROP TABLE IF EXISTS comment;'
    cursor.execute(drop_query)
    sqlconn.commit()
    
    try:
        create_query = '''create table if not exists comment(comment_id varchar(100) primary key,
                                                    video_id varchar(100),
                                                    comment_txt varchar(3000),
                                                    comment_author varchar(50),
                                                    comment_published_date TIMESTAMP)'''
        cursor.execute(create_query)
        sqlconn.commit()
    except:
        print("comment Table already created")
    print("Table created")
    
    comment_lst = []
    db = monconn["youtube_data"]
    mycoll = db["channel_details"]
    for comment_data in mycoll.find({}, {"_id": 0, "comment_details": 1}):
        for i in range(len(comment_data["comment_details"])):
            comment_lst.append(comment_data["comment_details"][i])
    df2 = pd.DataFrame(comment_lst)
    
    for index,row in df2.iterrows():
        # Parse the datetime string and convert it to the required format
        comment_published_date = datetime.strptime(row['comment_published_date'], '%Y-%m-%dT%H:%M:%SZ')
        insert_query = '''INSERT INTO comment (comment_id,
                                               video_id,
                                               comment_txt,
                                               comment_author,
                                               comment_published_date) VALUES(%s, %s, %s, %s, %s)'''

        values = (row['comment_id'],
                row['video_id'],
                row['comment_txt'],
                row['comment_author'],
                comment_published_date)

        #print("Query:", insert_query)
        #print("Values:", values)
        print("be4 insertion")
        try:
            cursor.execute(insert_query, values)
            sqlconn.commit()
            print("After instertion")
        except:
            print("This comment already exists")

def tables():
    channel_table()
    video_table()
    comment_table()

    return "Tables created"

def view_channel_table():
    channel_lst = []
    db = monconn["youtube_data"]
    mycoll = db["channel_details"]
    for channel_data in mycoll.find({},{"_id":0,"channel_details":1}):
        channel_lst.append(channel_data["channel_details"])
    df = st.dataframe(channel_lst)

    return df

def view_video_table():
    video_lst = []
    db = monconn["youtube_data"]
    mycoll = db["channel_details"]
    for video_data in mycoll.find({}, {"_id": 0, "video_details": 1}):
        for i in range(len(video_data["video_details"])):
            video_lst.append(video_data["video_details"][i])
    df1 = st.dataframe(video_lst)

    return df1

def view_comment_table():
    comment_lst = []
    db = monconn["youtube_data"]
    mycoll = db["channel_details"]
    for comment_data in mycoll.find({}, {"_id": 0, "comment_details": 1}):
        for i in range(len(comment_data["comment_details"])):
            comment_lst.append(comment_data["comment_details"][i])
    df2 = st.dataframe(comment_lst)

    return df2

#streamlit part
st.header(":blue[YouTube Data Harvesting and Warehousing]")
with st.sidebar:
    st.subheader(":red[A Quick Summary]")
    st.caption("Problem statement: Creating a Streamlit application for YouTube data analysis.")
    st.caption("Using the Google API, users can enter a YouTube channel ID to retrieve pertinent data.")
    st.caption("This data can be stored in MongoDB for numerous channels by using the app.")
    st.caption("After that, users can move data from the data lake to a SQL database, giving them access to a variety of search and retrieval options, such as table joins for in-depth channel information.")


channel_id = st.text_input("Enter channel id")
channels = channel_id.split(',')
channels = [ch.strip() for ch in channels if ch]

if st.button("Collect & Store data"):
    for channel in channels:
        channel_ids = []
        db = monconn["youtube_data"]
        mycoll = db["channel_details"]
        for channel_data in mycoll.find({},{"_id":0,"channel_details":1}):
           channel_ids.append(channel_data["channel_details"]["Channel_id"])
        if channel in channel_ids:
            st.success('The details of given channel id already exists')
        else:
            insert = channel_details(channel_id)
            st.success(insert)
if st.button("Migrate to SQL"):
    Table = st.success(tables)

show_table = st.radio("Select the table you want to view",("CHANNEL","VIDEO","COMMENT"))
if show_table == "CHANNEL":
    view_channel_table()
elif show_table == "VIDEO":
    view_video_table()
elif show_table =="COMMENT":
    view_comment_table()

#Mysql connection
sqlconn = mysql.connector.connect(
                            host="localhost",
                            user="root",
                            password="root",
                            database = "youtube_data")
cursor = sqlconn.cursor()

question = st.selectbox(
    'Please Select Your Question',
    ('1. All the videos and the Channel Name',
     '2. Channels with most number of videos and the video count',
     '3. Top 10 videos mostly viewed ',
     '4. No. of Comments in each video and their name',
     '5. Videos with highest likes and the channel names',
     '6. Total no. of likes of all videos with their names',
     '7. Total no. of views of each channel with their names',
     '8. Name of the channel which published their video in the year 2022',
     '9. Average duration of all videos in each channel and their names',
     '10. Videos with highest number of comments and their channel names'))

if question == '1. All the videos and the Channel Name':
    # first video_name --> table column name
    query1 = "select video_name as Video_name, channel_name as channel_name from video"
    cursor.execute(query1)
    t1 = cursor.fetchall()
    st.write(pd.DataFrame(t1,columns = ["Video Name","Channel Name"]))

elif question == '2. Channels with most number of videos and the video count':
    query2 = "select Channel_name as channel_name, Video_count as video_count from channel order by Video_count desc"
    cursor.execute(query2)
    t2 = cursor.fetchall()
    st.write(pd.DataFrame(t2,columns = ["Channel Name","No. of Videos"]))

elif question == '3. Top 10 videos mostly viewed ':
    query3 = '''select view_count as views, channel_name as channel_name, video_name as video_name from video
    where view_count is not null order by view_count desc limit 10'''
    cursor.execute(query3)
    t3 = cursor.fetchall()
    st.write(pd.DataFrame(t3,columns = ["View count","Channel Name","Video Name"]))

elif question == '4. No. of Comments in each video and their name':
    query4 = '''select comment_count as comments, video_name as video_name from video
    where comment_count is not null'''
    cursor.execute(query4)
    t4 = cursor.fetchall()
    st.write(pd.DataFrame(t4,columns = ["Comment count","Video Name"]))

elif question == '5. Videos with highest likes and the channel names':
    query5 = '''select video_name as video_name,channel_name as channel_name, like_count as like_count from video
    where like_count is not null order by like_count desc'''
    cursor.execute(query5)
    t5 = cursor.fetchall()
    st.write(pd.DataFrame(t5,columns = ["Video Name","Channel Name","Likes count"]))

elif question == '6. Total no. of likes of all videos with their names':
    query6 = '''select like_count as like_count,video_name as video_name
    from video'''
    cursor.execute(query6)
    t6 = cursor.fetchall()
    st.write(pd.DataFrame(t6,columns = ["Likes count","Video Name"]))

elif question == '7. Total no. of views of each channel with their names':
    query7 = '''select View_count as view_count,Channel_name as channel_name
    from channel'''
    cursor.execute(query7)
    t7 = cursor.fetchall()
    st.write(pd.DataFrame(t7,columns = ["View count","Channel Name"]))

elif question == '8. Name of the channel which published their video in the year 2022':
    query8 = '''select video_name as video_name,published_date as published_date,channel_name as channel_name from video
    where extract(year from published_date) = 2022'''
    cursor.execute(query8)
    t8 = cursor.fetchall()
    st.write(pd.DataFrame(t8,columns = ["Video Name","Published Date","Channel Name"]))

elif question == '9. Average duration of all videos in each channel and their names':
    query9 = '''select channel_name as channel_name, AVG(duration) as avg_duration from video
    group by channel_name'''
    cursor.execute(query9)
    t9 = cursor.fetchall()
    st.write(pd.DataFrame(t9,columns = ["Channel Name","Average Duration"]))

elif question == '10. Videos with highest number of comments and their channel names':
    query10 = '''select video_name as video_name,channel_name as channel_name, comment_count as comment_count from video
    where comment_count is not null order by comment_count desc'''
    cursor.execute(query10)
    t10 = cursor.fetchall()
    st.write(pd.DataFrame(t10,columns = ["Video Name","Channel Name","Comment Count"])) 
