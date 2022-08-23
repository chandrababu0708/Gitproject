import psycopg2
import os
import glob
import pandas as pd
import sql_queries

sparkify_db_details={
    'host':'localhost',
    'dbname':'sparkifydb',
    'user':'postgres',
    'pwd':'Sai@12300'
}

def get_dbconnection(db_details):
    con=psycopg2.connect(f"host={db_details.get('host')} dbname={db_details.get('dbname')} user={db_details.get('user')} password={db_details.get('pwd')}")
    return con

def get_files(filepath):
    all_files=[]
    for root,dirs,files in os.walk(filepath):
        files=glob.glob(os.path.join(root,'*json'))
        for file in files:
            all_files.append(file)
    return all_files

def process_song_data():
    files=get_files(r'E:\dataengineer\projects\Udacity-Data-Engineering-Projects-master\Data_Modeling_with_Postgres\data\song_data')
    cur = con.cursor()
    result = cur.execute(sql_queries.song_table_insert, song_data)
    for file in files:
        song_file_df=pd.read_json("file",lines=True)
        song_data=song_file_df[['song_id','title','artist_id','year','duration']].values[0]
        con=get_dbconnection(sparkify_db_details)
        print(f"number of records inserted into the songs table :{result}")
    con.commit()

def process_artists_data():
    files=get_files(r'E:\dataengineer\projects\Udacity-Data-Engineering-Projects-master\Data_Modeling_with_Postgres\data\song_data')
    con = get_dbconnection(sparkify_db_details)
    cur = con.cursor()
    for file in files:
        song_file_df=pd.read_json("file",lines=True)
        artist_data=song_file_df[['song_id','title','artist_id','year','duration']].values[0]
        result=cur.execute(sql_queries.artist_table_create,artist_data)
        print(f"number of records inserted into the artist table :{result}")

    con.commit()


def process_song_data2():
    files=get_files(r'E:\dataengineer\projects\Udacity-Data-Engineering-Projects-master\Data_Modeling_with_Postgres\data\song_data')
    con = get_dbconnection(sparkify_db_details)
    cur = con.cursor()
    for file in files:
        song_file_df=pd.read_json("file",lines=True)
        song_data=song_file_df[['song_id','title','artist_id','year','duration']].values[0]
        result=cur.execute(sql_queries.song_table_insert)
        print(f"number of records inserted into the songs table :{result}")
    con.commit()


def process_song_data3():
    files=get_files(r'E:\dataengineer\projects\Udacity-Data-Engineering-Projects-master\Data_Modeling_with_Postgres\data\song_data')
    con = get_dbconnection(sparkify_db_details)
    cur = con.cursor()
    for file in files:
        song_file_df=pd.read_json("file",lines=True)
        song_data=song_file_df[['song_id','title','artist_id','year','duration']].values[0]
        result=cur.execute(sql_queries.song_table_insert)
        print(f"number of records inserted into the songs table :{result}")
    con.commit()








