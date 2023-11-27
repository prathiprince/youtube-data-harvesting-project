# packages and libraries
from googleapiclient.discovery import build
import pymongo
import mysql.connector
import pandas as pd
import streamlit as st
import re
import seaborn as sns
import matplotlib.pyplot as plt



# API key connection
def Api_connect():
    Api_Id="AIzaSyDbOWb6XOjCIDnzo5b9toZp29srOx8N1ek"
    api_service_name='youtube'
    api_version='v3'
    youtube=build(api_service_name,api_version,developerKey=Api_Id)
    return youtube
youtube=Api_connect()



# get channel information
def get_channelinfo(channel_id):
    request=youtube.channels().list(
                part='snippet,contentDetails,statistics',
                id=channel_id
    )
    response=request.execute()

    for i in response['items']:
        data=dict(Channel_Name=i['snippet']['title'],
                Channel_Id=i['id'],
                Subs_count=i['statistics']['subscriberCount'],
                view_count=i['statistics']['viewCount'],
                Total_videos=i['statistics']['videoCount'],
                Channel_description=i['snippet']['description'],
                Playlist_Id=i['contentDetails']['relatedPlaylists']['uploads']
                )
    return data




def get_video_ids(channel_id):
    video_ids=[]
    response=youtube.channels().list(id=channel_id,
                                    part='contentDetails').execute()
                                    
    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token=None

    while True:
        response1=youtube.playlistItems().list(
                                            part='snippet',
                                            playlistId=Playlist_Id,
                                            maxResults=50,
                                            pageToken=next_page_token).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids




# trial run for duration conversion in video information
import re
def convert_duration(original_duration):
    regex = r'PT(\d+H)?(\d+M)?(\d+S)?'
    match = re.match(regex, original_duration)
    if not match:
        return '00:00:00'
    hours, minutes, seconds = match.groups()
    hours = int(hours[:-1]) if hours else 0
    minutes = int(minutes[:-1]) if minutes else 0
    seconds = int(seconds[:-1]) if seconds else 0
    total_seconds = hours * 3600 + minutes * 60 + seconds
    return '{:02d}:{:02d}:{:02d}'.format(int(total_seconds / 3600), int((total_seconds % 3600) / 60), int(total_seconds % 60))

def get_video_info(videoids):
    video_data=[]
    for v_id in videoids:
        request=youtube.videos().list(part='snippet,contentDetails,statistics',
                                id=v_id
                                )
        response=request.execute()
        
        for item in response['items']:
            original_duration=item['contentDetails']['duration']
            duration=convert_duration(original_duration)
            data=dict(channel_Name=item['snippet']['channelTitle'],
                        channel_id=item['snippet']['channelId'],
                        id=item['id'],
                        title=item['snippet']['title'],
                        tags=item['snippet'].get('tags'),
                        thumbnail=item['snippet']['thumbnails']['default']['url'],
                        descript=item['snippet'].get('description'),
                        published_at=item['snippet']['publishedAt'],
                        viewcount=item['statistics']['viewCount'],
                        likes=item['statistics'].get('likeCount'),
                        dislikes=item['statistics'].get('dislikeCount'),
                        comments=item['statistics'].get('commentCount'),
                        video_duration=duration
                        )
            video_data.append(data)
            
    return video_data





# get comment information
def get_commentdetails(videoids):
    comment_data = []
    try:
        for vid_ids in videoids:
            request = youtube.commentThreads().list(
                                                    part = 'snippet', 
                                                    videoId = vid_ids,
                                                    maxResults = 50
                                                    )
            response = request.execute()
            for item in response['items']:
                data = dict(
                            comment_id = item['snippet']['topLevelComment']['id'],
                            videoidsofcomments = item['snippet']['topLevelComment']['snippet']['videoId'],
                            comment_text = item['snippet']['topLevelComment']['snippet']['textDisplay'],
                            author = item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            published_at=item['snippet']['topLevelComment']['snippet']['publishedAt']
                            )
                comment_data.append(data)
    except:
        pass
    return comment_data



# get playlist details
def get_playlistdetails(channel_id):
    next_page_token = None
    all_data = []

    while True:
        request = youtube.playlists().list(
            part='snippet,contentDetails',
            channelId=channel_id,  # Fix: use the variable channel_id
            maxResults=50,
            pageToken=next_page_token
        )
        response = request.execute()

        for item in response['items']:
            data = dict(
                Playlist_Id=item['id'],
                Title=item['snippet']['title'],
                channel_Id=item['snippet']['channelId'],
                channel_Name=item['snippet']['channelTitle'],
                PublishedAt=item['snippet']['publishedAt'],
                Video_count=item['contentDetails']['itemCount']
            )
            all_data.append(data)

        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            break

    return all_data



#upload to mongodb
client = pymongo.MongoClient("mongodb://localhost:27017/") 
db = client['projectutube']
coll1=db['project_info']



# data transfer to momgodb
def project_info(channel_id):
    ch_details=get_channelinfo(channel_id)
    playlists_details=get_playlistdetails(channel_id)
    videoidentity=get_video_ids(channel_id)
    videoinfo=get_video_info(videoidentity)
    commentinfo=get_commentdetails(videoidentity)

    coll1=db['project_info']
    coll1.insert_one(
                    {'channel_information':ch_details,
                    'video_information' : videoinfo,
                    'comment_information' : commentinfo,
                    'playlist_information':playlists_details
                    }
                    )
    return 'upload_completed'



mydb = mysql.connector.connect(host='localhost',user='root',password='123456',database=' projectutubemys')
mycursor = mydb.cursor()


# working code for channel table creation
def channeltable_details():
    mydb = mysql.connector.connect(host='localhost',user='root',password='123456',database=' projectutubemys')
    mycursor = mydb.cursor()

    create_query = '''create table if not exists channels(Channel_Name varchar(100),
                                                        Channel_Id varchar(80) primary key,                         
                                                        Subs_count bigint,
                                                        view_count bigint,
                                                        Total_videos int,
                                                        Channel_descrition text,
                                                        Playlist_Id varchar(80)
                                                        )'''
    mycursor.execute(create_query)
    mydb.commit()
    
    ch_list = []
    db=client['projectutube']
    coll1=db['project_info']
    for ch_data in coll1.find({},{'_id':0,'channel_information':1}):
        ch_list.append(ch_data['channel_information'])
    df=pd.DataFrame(ch_list)

    for index, row in df.iterrows():
        insert_query = ''' insert into channels(Channel_Name,
                                                Channel_Id,
                                                Subs_count,
                                                view_count,
                                                Total_videos,
                                                Channel_descrition,
                                                Playlist_Id
                                                )
                                                values(%s,%s,%s,%s,%s,%s,%s)'''
        values = (row['Channel_Name'],
                row['Channel_Id'],
                row['Subs_count'],
                row['view_count'],
                row['Total_videos'],
                row['Channel_description'],
                row['Playlist_Id'])
        
        mycursor.execute(insert_query,values)
        mydb.commit()




def playlisttable_details():
    mydb = mysql.connector.connect(host='localhost',user='root',password='123456',database='projectutubemys')
    mycursor = mydb.cursor()

    drop_query = '''drop table if exists playlists'''
    mycursor.execute(drop_query)
    mydb.commit()

    create_query = '''create table if not exists playlists(Playlist_Id varchar(100) primary key,
                                                        Title varchar(80),                         
                                                        channel_Id varchar(100),
                                                        channel_Name varchar(100),
                                                        PublishedAt timestamp,
                                                        Video_count int
                                                        )'''

    mycursor.execute(create_query)
    mydb.commit()

    pl_list = []
    db=client['projectutube']
    coll1=db['project_info']
    for pl_data in coll1.find({},{'_id':0,'playlist_information':1}):
        for i in range(len(pl_data['playlist_information'])):
            pl_list.append(pl_data['playlist_information'][i])
    df1=pd.DataFrame(pl_list)

    from datetime import datetime
    for index, row in df1.iterrows():
        insert_query = ''' insert into playlists(Playlist_Id,
                                                Title,                         
                                                channel_Id,
                                                channel_Name,
                                                PublishedAt,
                                                Video_count
                                                )
                                                values(%s,%s,%s,%s,%s,%s)'''
        
        PublishedAt = datetime.strptime(row['PublishedAt'], '%Y-%m-%dT%H:%M:%SZ')
        
        values = (row['Playlist_Id'],
                row['Title'],
                row['channel_Id'],
                row['channel_Name'],
                PublishedAt,
                row['Video_count']
                )
        mycursor.execute(insert_query,values)
        mydb.commit()



def get_videodetails():
    mydb = mysql.connector.connect(host='localhost',user='root',password='123456',database='projectutubemys')
    mycursor = mydb.cursor()

    drop_query = '''drop table if exists videos'''
    mycursor.execute(drop_query)
    mydb.commit()

    create_query = '''create table if not exists videos(channel_Name varchar(100),
                                                        channel_id varchar(100),
                                                        id varchar(100) primary key,
                                                        title varchar(100),
                                                        tags text,
                                                        thumbnail varchar(200),
                                                        descript text,
                                                        published_at timestamp,
                                                        viewcount bigint,
                                                        likes bigint,
                                                        dislikes bigint,
                                                        comments int,
                                                        duration time
                                                        )'''

    mycursor.execute(create_query)
    mydb.commit()

    video_list = []
    db=client['projectutube']
    coll1=db['project_info']
    for video_data in coll1.find({},{'_id':0,'video_information':1}):
        for i in range(len(video_data['video_information'])):
            video_list.append(video_data['video_information'][i])
    df2=pd.DataFrame(video_list)

    from datetime import datetime
    for index, row in df2.iterrows():
        insert_query = ''' insert into videos(channel_Name,
                                                channel_id,
                                                id,
                                                title,
                                                tags,
                                                thumbnail,
                                                descript,
                                                published_at,
                                                viewcount,
                                                likes,
                                                dislikes,
                                                comments,
                                                duration
                                                )
                                                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        
        published_at = datetime.strptime(row['published_at'], '%Y-%m-%dT%H:%M:%SZ')
        values = (row['channel_Name'],
                row['channel_id'],
                row['id'],
                row['title'],
                row['tags'],
                row['thumbnail'],
                row['descript'],
                published_at,
                row['viewcount'],
                row['likes'],
                row['dislikes'],
                row['comments'],
                row['video_duration']
                )
        try:
            mycursor.execute(insert_query, values)
            mydb.commit()
        except Exception as e:
            print(f"Error inserting row {index}: {e}")


def comment_tabledetails():
    mydb = mysql.connector.connect(host='localhost',user='root',password='123456',database='projectutubemys')
    mycursor = mydb.cursor()

    drop_query = '''drop table if exists comment'''
    mycursor.execute(drop_query)
    mydb.commit()

    create_query = '''create table if not exists comment(comment_id varchar(100) primary key,
                                                        videoidsofcomments varchar(100),
                                                        comment_text text,
                                                        author varchar(100),
                                                        published_at timestamp
                                                        )'''

    mycursor.execute(create_query)
    mydb.commit()

    comment_list = []
    db=client['projectutube']
    coll1=db['project_info']
    for comment_data in coll1.find({},{'_id':0,'comment_information':1}):
        for i in range (len(comment_data['comment_information'])):
            comment_list.append(comment_data['comment_information'][i])
    df3=pd.DataFrame(comment_list)

    from datetime import datetime
    for index, row in df3.iterrows():
        insert_query = '''INSERT INTO comment (comment_id, videoidsofcomments, comment_text, author, published_at) 
                        VALUES (%s, %s, %s, %s, %s)'''
        
        published_at = datetime.strptime(row['published_at'], '%Y-%m-%dT%H:%M:%SZ')
        
        values = (row['comment_id'],
                row['videoidsofcomments'],
                row['comment_text'],
                row['author'],
                published_at)
        
        try:
            mycursor.execute(insert_query, values)
            mydb.commit()
        except Exception as e:
            print(f"Error inserting row {index}: {e}")



def tables():
    channeltable_details()
    playlisttable_details()
    get_videodetails()
    comment_tabledetails()
    
    return 'tables created successfully'


def show_channeltabledetails():
    ch_list = []
    db=client['projectutube']
    coll1=db['project_info']
    for ch_data in coll1.find({},{'_id':0,'channel_information':1}):
        ch_list.append(ch_data['channel_information'])
    df=st.dataframe(ch_list)

    return df


def show_playslisttabledetails():
    pl_list = []
    db=client['projectutube']
    coll1=db['project_info']
    for pl_data in coll1.find({},{'_id':0,'playlist_information':1}):
        for i in range(len(pl_data['playlist_information'])):
            pl_list.append(pl_data['playlist_information'][i])
    df1=st.dataframe(pl_list)

    return df1


def show_videotabledetails():
    video_list = []
    db=client['projectutube']
    coll1=db['project_info']
    for video_data in coll1.find({},{'_id':0,'video_information':1}):
        for i in range(len(video_data['video_information'])):
            video_list.append(video_data['video_information'][i])
    df2=st.dataframe(video_list)
    
    return df2


def show_commenttabledetails():
    comment_list = []
    db=client['projectutube']
    coll1=db['project_info']
    for comment_data in coll1.find({},{'_id':0,'comment_information':1}):
        for i in range (len(comment_data['comment_information'])):
            comment_list.append(comment_data['comment_information'][i])
    df3=st.dataframe(comment_list)


# streamlit 
st.header(':red[YOUTUBE DATA HARVESTING]')
with st.sidebar:
    st.title(":blue[YOUTUBE DATA HARVESTING AND WAREHOUSING PROJECT]")
    

channel_id = st.text_input('PLEASE INPUT THE CHANNEL ID')

if st.button("EXTRACT AND TRANSFER DATA TO MONGODB"):
    ch_ids=[]
    db=client['projectutube']
    coll1=db['project_info']
    for ch_data in coll1.find({},{'_id':0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"]["Channel_Id"])

    if channel_id in ch_ids:
        st.success("channel details already exists")
    
    else:
        insert = project_info(channel_id)
        st.success(insert)

if st.button("Migrate to sql"):
    Table=tables()
    st.success(Table)

show_table = st.radio("select the table for view",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))

if show_table == 'CHANNELS':
    show_channeltabledetails()

elif show_table == 'PLAYLISTS':
    show_playslisttabledetails()

elif show_table == 'VIDEOS':
    show_videotabledetails()

elif show_table == 'COMMENTS':
    show_commenttabledetails()

# SQL Connection

mydb = mysql.connector.connect(host='localhost',user='root',password='123456',database='projectutubemys')
mycursor = mydb.cursor()

questions = st.selectbox("select the question",("1. All the videos and the channel name",
                                                "2. channels with most number of videos",
                                                "3. Ten most viewed videos",
                                                "4. comments in each videos",
                                                "5. Videos with highest likes",
                                                "6. likes of all vidoes",
                                                "7. views of each channel",
                                                "8. Videos published in the year of 2022",
                                                "9. Average duration of all videos in each channel",
                                                "10. Videos with highest number of comments"))

if questions=="1. All the videos and the channel name":
    query1='''select title as videos,channel_Name as channelname from videos'''
    mycursor.execute(query1)
    t1=mycursor.fetchall()
    mydb.commit()
    df=pd.DataFrame(t1,columns=['video_title','channel_name'])
    st.write(df)


elif questions == '2. channels with most number of videos':
    query2 = '''select Channel_Name as channelname, Total_videos as numbervideos from channels'''
    mycursor.execute(query2)
    t2 = mycursor.fetchall()
    mydb.commit()
    df2 = pd.DataFrame(t2, columns=["channel_name", "numbervideos"])
    st.write(df2)
    
    fig, ax = plt.subplots()
    sns.barplot(data=df2, y='channel_name', x='numbervideos', color='#03fcf0', ax=ax)
    ax.set_title('Channels with Most Number of Videos')
    st.pyplot(fig)


elif questions== '3. Ten most viewed videos':
    query3='''select viewcount as views, channel_Name as channelname, title as videotitle from videos where viewcount is not null order by views desc limit 10'''
    mycursor.execute(query3)
    t3=mycursor.fetchall()
    mydb.commit()
    df3=pd.DataFrame(t3,columns=["views","channel_name","videotitle"])
    st.write(df3)

    fig1, ax = plt.subplots()
    sns.barplot(data=df3, y='videotitle', x='views', color='#fc6f03', ax=ax)
    ax.set_title('Ten most viewed videos')
    st.pyplot(fig1)


elif questions== '4. comments in each videos':
    query4='''select comments as number_of_comments, title as videotitle from videos where comments is not null'''
    mycursor.execute(query4)
    t4=mycursor.fetchall()
    mydb.commit()
    df4=pd.DataFrame(t4,columns=["number_of_comments","videotitle"])
    st.write(df4)


elif questions== '5. Videos with highest likes':
    query5='''select title as videotitle, channel_name as channelname, likes as likecount from videos where likes is not null order by likes desc'''
    mycursor.execute(query5)
    t5=mycursor.fetchall()
    mydb.commit()
    df5=pd.DataFrame(t5,columns=["videotitle","channelname","likecount"])
    st.write(df5)


elif questions== '6. likes of all vidoes':
    query6='''select likes as likecount,title as videotitle from videos'''
    mycursor.execute(query6)
    t6=mycursor.fetchall()
    mydb.commit()
    df6=pd.DataFrame(t6,columns=["likecount","videotitle"])
    st.write(df6)


elif questions== '7. views of each channel':
    query7='''select Channel_Name as channelname, view_count as totalviews from channels'''
    mycursor.execute(query7)
    t7=mycursor.fetchall()
    mydb.commit()
    df7=pd.DataFrame(t7,columns=["channelname","totalviews"])
    st.write(df7)

    fig2, ax = plt.subplots()
    sns.barplot(data=df7, x='totalviews', y='channelname', color='#03fc5e', ax=ax)
    ax.set_title('Views of each channel')
    st.pyplot(fig2)

elif questions== '8. Videos published in the year of 2022':
    query8='''select title as video_title, published_at as videorelease, channel_Name as channelname 
                from videos where extract(year from published_at)=2022'''
    mycursor.execute(query8)
    t8=mycursor.fetchall()
    mydb.commit()
    df8=pd.DataFrame(t8,columns=["videotitle","videorelease","channelname"])
    st.write(df8)


elif questions== '9. Average duration of all videos in each channel':
    query9='''select channel_Name as channelname, AVG(duration) as averageduration from videos group by channel_Name'''
    mycursor.execute(query9)
    t9=mycursor.fetchall()
    mydb.commit()
    df9=pd.DataFrame(t9,columns=["channelname","averageduration"])
    
    T9=[]
    for index, row in df9.iterrows():
        channel_title=row["channelname"]
        average_duration=row["averageduration"]
        avg_durationstr=str(average_duration)
        T9.append(dict(channeltitle=channel_title,avgduration=avg_durationstr))
    df1=pd.DataFrame(T9)
    st.write(df1)


elif questions== '10. Videos with highest number of comments':
    query10='''select channel_Name as channelname, title as videos, comments as highestcomments from videos 
                where comments is not null order by comments desc'''
    mycursor.execute(query10)
    t10=mycursor.fetchall()
    mydb.commit()
    df10=pd.DataFrame(t10,columns=["channelname","videos","highestcomments"])
    st.write(df10)

    


