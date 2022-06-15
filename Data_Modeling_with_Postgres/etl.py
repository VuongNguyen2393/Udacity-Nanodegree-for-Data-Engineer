import os
import glob
import psycopg2
import pandas as pd
from datetime import datetime
from sql_queries import *


def process_song_file(cur, filepath):
    """ 
    - Read data from filepath
    - Insert columns song_id, title, artist_id, year, duration into songs table
    - Insert columns artist_id, artist_name, artist_location, artist_latitude, artist_longitude into artists table.
    """
    # open song file
    df = pd.read_json(filepath,typ="series")

    # insert song record
    song_data = df[['song_id','title','artist_id','year','duration']].values.tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].values.tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """ 
    - Read data from filepath
    - Filter the records which have page NextSong
    - Convert timestamp into datetime, extract the needed time information and insert into time table
    - Insert user's information into users table
    - Insert needed information into songplays table
    """
    # open log file
    df = pd.read_json(filepath,lines=True)

    # filter by NextSong action
    df = df[df['page'] == "NextSong"]

    # convert timestamp column to datetime
    t = pd.Series([datetime.fromtimestamp(date/1000) for date in df['ts']])
    
    # insert time data records
    time_data = {'timestamp': t, 
             'hour': t.dt.hour.values.tolist(), 
             'day': t.dt.day.values.tolist(),
             'weekofyear': t.dt.weekofyear.values.tolist(),
             'month': t.dt.month.values.tolist(),
             'year': t.dt.year.values.tolist(),
             'weekday': t.dt.weekday.values.tolist()}
    column_labels = ('timestamp', 'hour', 'day', 'weekofyear', 'month', 'year', 'weekday')
    time_df = pd.DataFrame(time_data, columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']]

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
        songplay_data = (datetime.fromtimestamp(row.ts/1000), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """ Process get the files path from variable filepath and process function."""
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
    """ Execute the whole program with defined connection and cursor to Postgre."""
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()