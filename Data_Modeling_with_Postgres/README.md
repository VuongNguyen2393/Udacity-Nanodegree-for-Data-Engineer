## Introduction:

This database is built to help Sparkify's analytic team can easily get the information about their users, songs, artists and time. 
From those information, the analytic team can analize the behavior of user and the music trending also. 

## Prequisites:

Before running the program, please check if you had installed below libraries:

- **os**
- **glob**
- **psycopg2**
- **pandas**
- **datetime**

## How to run the Python scrypt:

- To drops and creates table use this command with terminal: `python create_tables.py`
- To process ETL use this command with termianl: `python etl.py`

## Files in the repository:

1. **test.ipynb**: displays the first few rows of each table to let you check the database.
2. **create_tables.py**: drops and creates your tables. You run this file to reset your tables before each time you run your ETL scripts.
3. **etl.ipynb**: reads and processes a single file from song_data and log_data and loads the data into database. This notebook contains detailed instructions on the ETL process for each of the tables.
4. **etl.py**: reads and processes files from song_data and log_data and loads them into database.
5. **sql_queries.py**: contains all the sql queries.
6. **README.md**: provides detail information about prequisites and how to execute the program.

## Database schema design and ETL pipeline.

To optimize for queries on song play analysis. The database is design with Star Schema which includes below tables:

#### Fact Table

1. **songplays**: records in log data associated with song plays i.e. records with page NextSong
- songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

#### Dimension Tables

2. **users**: users in the app
    user_id, first_name, last_name, gender, level

3. **songs**: songs in music database
    song_id, title, artist_id, year, duration

3. **artists**: artists in music database
    artist_id, name, location, latitude, longitude

4. **time**: timestamps of records in songplays broken down into specific units
    start_time, hour, day, week, month, year, weekday
