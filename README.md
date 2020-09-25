In this project, and ETL process is being used to process and transform data stored in an AWS S3 bucket for a mocked up song streaming app.  The raw data files for the app are json log files of events for users streaming music and JSON song files with song information.  Python scripts are used to extract tables from the raw data into an S3 data lake using pyspark.

Then, an ETL pipeline is created to process the raw data into a star schema for analytical purposes.  The final tables are:

1) songplays - information about song streaming
    songplay_id 
    start_time 
    user_id 
    level 
    song_id 
    artist_id 
    session_id 
    location 
    user_agent 
        
2) users - user profile information
    user_id 
    first_name 
    last_name 
    gender 
    level 

3) songs - song information
    song_id 
    title 
    artist_id 
    year 
    duration 

4) artist - artist information
    artist_id
    name
    location
    lattitude
    longitude
    
5) time - information on when songs were streamed
    start_time
    hour
    day
    week
    month
    year
    weekday


These tables were created to for analytical purposes to be served to anaylsts, data scientists, and ML engineers.