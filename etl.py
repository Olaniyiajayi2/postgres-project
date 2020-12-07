import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    
    '''
    process the song files, create a connection and a cursor.
    
    Parameters:
    -------------------
        cur: connection cursor
        
            create a connection cursor used for creating the database 
            
        filepath: String
            
            path to where the song file resides
            
    Returns:
    --------
        None
        
    '''
    
    
    # open song file
    df = pd.read_json(filepath, lines = True)
    cols_songs = ["song_id", "title", "artist_id", "year", "duration"]
    df_song = df[cols_songs].values
    df_song = df_song[0]
    
    # insert song record
    song_data = list(df_song)
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    cols_artist = ["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]
    df_artist = df[cols_artist].values
    df_artist = df_artist[0]
    artist_data = list(df_artist)
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    
    '''
    process the song files, create a connection and a cursor.
    
    Parameters:
    -------------------
        cur: connection cursor
        
            create a connection cursor used for creating the database 
            
        filepath: String
            
            path to where the log file resides
            
    Returns:
    --------
        None
        
    '''
    # open log file
    df = pd.read_json(filepath, lines = True)

    # filter by NextSong action
    df = df[df['page'] == "NextSong"]

    # convert timestamp column to datetime and extract month, year, day, week, weekday, and hour
    
    df["ts"] = pd.to_datetime(df["ts"], unit = 'ms')
    df['month'] = df['ts'].dt.month
    df['year'] = df['ts'].dt.year
    df['day'] = df['ts'].dt.day
    df['week'] = df['ts'].dt.week
    df['weekday'] = df['ts'].dt.weekday
    df['hour'] = df['ts'].dt.hour
    
    # insert time data records
    column_labels = ["ts", "year", "month", "week", "weekday", "day", "hour"]
    time_df = df[column_labels]

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_col = ["userId", "firstName", "lastName", "gender", "level"]
    user_df = df[user_col]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data =  (index, row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        try:
            cur.execute(songplay_table_insert, songplay_data)
        except psycopg2.Error as e:
            print("Error: Inserting Row")
            print(e)
        


def process_data(cur, conn, filepath, func):
    
    '''
    process the song files, create a connection and a cursor.
    
    Parameters:
    -------------------
        cur: connection cursor
        
            create a connection cursor used for creating the database 
            
        conn: database connection
        
            Creates a connection for the database
            
        filepath: String
            
            path to where the song file resides
            
        func: Function
            
            custom function for processing either the song or log files
            
            
    Returns:
    --------
        None
        
    '''
    
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()