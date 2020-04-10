# Project Overview
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.  

# Schema Structure
Schema for Song Play Analysis
Using the song and log datasets, you'll need to create a star schema optimized for queries on song play analysis. This includes the following tables.

## Fact Table  
- songplays - records in log data associated with song plays i.e. records with page NextSong  
> songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent   
 
## Dimension Tables  
- users - users in the app  
> user_id, first_name, last_name, gender, level  
- songs - songs in music database  
> song_id, title, artist_id, year, duration  
- artists - artists in music database  
> artist_id, name, location, lattitude, longitude  
- time - timestamps of records in songplays broken down into specific units  
> start_time, hour, day, week, month, year, weekday  

## How To:
1. Extract zip files under data folder, song-data.zip to song_data folder and log-data-zip to log_data folder.  
2. Add your AWS Access and Secret Key in *dl.cfg* file.  
3. Create S3 bucket *udacity-output* and make sure your AWS user has Read/Write permission.
4. Run the script:  
`python etl.py`

> Note:: a parquet files should created under your bucket inside different folders,  

> - song: for songs table. 
> - artist: for artists table. 
> - user: for users data. 
> - time: for time table. 
> - songplays: for songplays table
