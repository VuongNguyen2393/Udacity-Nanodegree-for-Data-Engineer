import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA = config.get('S3','LOG_DATA')
LOG_JSONPATH = config.get('S3','LOG_JSONPATH')
SONG_DATA = config.get('S3','SONG_DATA')
DWH_ROLE_ARN = config.get('IAM_ROLE','ARN')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_songs_table_create= (""" CREATE TABLE IF NOT EXISTS staging_songs (\
                                           num_songs  int NOT NULL,\
                                           artist_id text,\
                                           artist_latitude double precision,\
                                           artist_longitude double precision,\
                                           artist_location text,\
                                           artist_name text NOT NULL,\
                                           song_id text,\
                                           title text,\
                                           duration numeric,\
                                           year int)

""")

staging_events_table_create = (""" CREATE TABLE IF NOT EXISTS staging_events (\
                                            artist text,\
                                            auth text,\
                                            firstName text,\
                                            gender char(1),\
                                            itemInSession int,\
                                            lastName text,\
                                            length numeric,\
                                            level text,\
                                            location text,\
                                            method text,\
                                            page text,\
                                            registration numeric,\
                                            sessionId int,\
                                            song text,\
                                            status int,\
                                            ts bigint,\
                                            userAgent text,\
                                            userId int
                                            )
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (\
                                songplay_id int PRIMARY KEY IDENTITY,\
                                start_time timestamp NOT NULL sortkey,\
                                user_id int NOT NULL default 0,\
                                level text,\
                                song_id text distkey,\
                                artist_id text,\
                                session_id int ,\
                                location text,\
                                user_agent text)
                            
                            
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS  users (\
                                user_id int,\
                                first_name text sortkey,\
                                last_name text,\
                                gender char(1),\
                                level text)
                        diststyle all;
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (\
                                song_id text PRIMARY KEY distkey,\
                                title text NOT NULL,\
                                artist_id text,\
                                year int sortkey,\
                                duration numeric NOT NULL)
                       
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (\
                                artist_id text PRIMARY KEY,\
                                name text NOT NULL sortkey,\
                                location text,\
                                latitude double precision,\
                                longitude double precision)
                        diststyle all;
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (\
                                start_time date PRIMARY KEY sortkey,\
                                hour int,\
                                day int,\
                                week int,\
                                month int,\
                                year int,\
                                weekday int)
                         diststyle all;
                       
""")

# STAGING TABLES

staging_events_copy = ("""
copy {} from {} 
iam_role {}
json {};
""").format("staging_events",LOG_DATA, DWH_ROLE_ARN, LOG_JSONPATH)

staging_songs_copy = ("""
copy {} from {} 
iam_role {}
json 'auto'
""").format("staging_songs",SONG_DATA, DWH_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id,\
                                                    artist_id, session_id, location, user_agent)\
                             SELECT  to_timestamp(e.ts::text, 'YYYYMMDDHH24MISS'),\
                                     e.userId,\
                                     e.level,\
                                     s.song_id,\
                                     s.artist_id,\
                                     e.sessionId,\
                                     s.artist_location,\
                                     e.userAgent
                             FROM staging_songs s JOIN staging_events e ON s.title = e.song
                             
""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
                             SELECT DISTINCT(userId), firstName, lastName, gender, level
                             FROM staging_events
                             
""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)\
                             SELECT DISTINCT(song_id), title, artist_id, year, duration
                             FROM staging_songs
                            
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude)\
                             SELECT DISTINCT(artist_id), artist_name, artist_location, artist_latitude, artist_longitude
                             FROM staging_songs
                             
""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)\
                             SELECT DISTINCT(to_date(ts/1000::text,'YYYYMMDD')) as stime,\
                                    EXTRACT(HOUR from stime),\
                                    EXTRACT(DAY from stime),\
                                    EXTRACT(WEEK from stime),\
                                    EXTRACT(MONTH from stime),\
                                    EXTRACT(YEAR from stime),\
                                    EXTRACT(DOW FROM stime)     
                             FROM staging_events
                             
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
