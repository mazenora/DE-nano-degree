# DataWarehouse Project
## Introduction
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

## How to Run

1. set the configuration needed to connect to Redshift and S3 in dwh.cfg

```
[CLUSTER]
HOST=
DB_NAME=
DB_USER=
DB_PASSWORD=
DB_PORT=

[IAM_ROLE]
ARN=''

[S3]
LOG_DATA='s3://udacity-dend/log_data'
LOG_JSONPATH='s3://udacity-dend/log_json_path.json'
SONG_DATA='s3://udacity-dend/song_data'
```

2. run create tables python file. 
`python create_tables.py`  
  
3. Run the ETL process: get data from S3, into stage, then into the deminsional tables:  
`python etl.py`

## Databse Schema Design

### Staging Tables
- staging_events
- staging_songs


### Fact Table
- songplays - records in event data associated with song plays i.e. records with page NextSong
> songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension Tables
- users - users in the app
> user_id, first_name, last_name, gender, level

- songs - songs in music database
> song_id, title, artist_id, year, duration

- artists - artists in music database
> artist_id, name, location, lattitude, longitude

- time - timestamps of records in songplays broken down into specific units
> start_time, hour, day, week, month, year, weekday