## Purpose:

This database is built to help Sparkify's analytic team can easily get the information about their users, songs, artists and time. 
The raw data is save in S3 storage folders log_data and song_data. We will need to have staging tables to store those data and then we can denormalize into suitable dimensional tables.

## ETL Pipeline:

The pipeline will have 3 functions:

**create_spark_session()** : will create the spark session to transform and loading data
**process_song_data(spark, input_data, output_data)**: will use the spark session to extract in-needed data from song data into `songs_table`, `artists_table`
**process_log_data(spark, input_data, output_data)**: will use the spark session to extract in-needed data from log data into `users_table`, `time_table`, `songplays_table`


## Database schema design:


The database is design with Star Schema which includes below tables:

#### Fact Table

1. **songplays_table**: records in log data associated with song plays i.e. records with page NextSong
- columns: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent


#### Dimension Tables

2. **users_table**: users in the app
- columns: user_id, first_name, last_name, gender, level


3. **songs_table**: songs in music database
- columns: song_id, title, artist_id, year, duration


3. **artists_table**: artists in music database
- columns: artist_id, name, location, latitude, longitude


4. **time_table**: timestamps of records in songplays broken down into specific units
- columns: start_time, hour, day, week, month, year, weekday
