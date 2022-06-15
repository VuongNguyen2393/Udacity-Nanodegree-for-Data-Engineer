## Purpose:

This database is built to help Sparkify's analytic team can easily get the information about their users, songs, artists and time. 
The raw data is save in S3 storage folders log_data and song_data. We will need to have staging tables to store those data and then we can denormalize into suitable dimensional tables.

## ETL Pipeline Staging tables:

1. **staging_events**: store data copy from folder log_data on S3.
- artist, auth, firstName, gender, itemInSession, lastName, length, level, location, method, page, registration, sessionId, song, status, ts, userAgent, userId

2. **staging_songs**: store raw data copy from folder song_data on S3
- num_songs, artist_id, artist_latitude, artist_longitude, artist_location, artist_name, song_id, title, duration, year

## Database schema design:


The database is design with Star Schema which includes below tables:

#### Fact Table

1. **songplays**: records in log data associated with song plays i.e. records with page NextSong
- columns: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
- distkey: song_id
- sortkey: start_time
- diststyle: even

#### Dimension Tables

2. **users**: users in the app
- columns: user_id, first_name, last_name, gender, level
- sortkey: first_name
- diststyle: all

3. **songs**: songs in music database
- columns: song_id, title, artist_id, year, duration
- distkey: song_id
- sortkey: year
- diststyle: even

3. **artists**: artists in music database
- columns: artist_id, name, location, latitude, longitude
- sortkey: name
- diststyle: all

4. **time**: timestamps of records in songplays broken down into specific units
- columns: start_time, hour, day, week, month, year, weekday
- sortkey: start_time
- diststyle: all