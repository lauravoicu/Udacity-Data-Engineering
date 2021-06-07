# Project: Data Lake

## Introduction

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

In this project we will build an ETL pipeline that extracts their data from the data lake hosted on S3, processes them using Spark which will be deployed on an EMR cluster using AWS, and load the data back into S3 as a set of dimensional tables in parquet format. 

From this tables we will be able to find insights in what songs their users are listening to.

## How to run

Fill in the details of the `dl.cfg` file in the root of the project with the following data:

```
KEY=YOUR_AWS_ACCESS_KEY
SECRET=YOUR_AWS_SECRET_KEY
```

Run the command inside the Spark master machine:

`python etl.py`

## Project structure

- dl.cfg: File with AWS credentials.
- etl.py: Program that extracts songs and log data from S3, transforms it using Spark, and loads the dimensional tables created in parquet format back to S3.
- README.md: Current file, contains detailed information about the project.

## ETL pipeline

1. Load credentials
2. Read data from S3
    - Song data: `s3://udacity-dend/song_data`
    - Log data: `s3://udacity-dend/log_data`

    The script reads song_data and load_data from S3.

3. Process data using spark

    Transforms them to create five different dimensions and facts tables.
    Each table includes the right columns and data types. Duplicates are addressed where appropriate.

4. Load it back to S3

    Writes them to partitioned parquet files in table directories on S3.

    Each of the five tables are written to parquet files in a separate analytics directory on S3. Each table has its own folder within the directory. Songs table files are partitioned by year and then artist. Time table files are partitioned by year and month. Songplays table files are partitioned by year and month.

### Source Data
- **Song datasets**: all json files are nested in subdirectories under *s3a://udacity-dend/song_data*. 

- **Log datasets**: all json files are nested in subdirectories under *s3a://udacity-dend/log_data*. 

### Dimension Tables and Fact Table

**songplays** - Fact table - records in log data associated with song plays i.e. records with page NextSong
- *Location:* `s3a://udacity-dend-sparkify-datalake/songplays.parquet`
- *Type:* Fact table

- songplay_id (INT) PRIMARY KEY: ID of each user song play 
- start_time (DATE) NOT NULL: Timestamp of beggining of user activity
- user_id (INT) NOT NULL: ID of user
- level (TEXT): User level {free | paid}
- song_id (TEXT) NOT NULL: ID of Song played
- artist_id (TEXT) NOT NULL: ID of Artist of the song played
- session_id (INT): ID of the user Session 
- location (TEXT): User location 
- user_agent (TEXT): Agent used by user to access Sparkify platform

**users** - users in the app
- *Location:* `s3a://udacity-dend-sparkify-datalake/users.parquet`
- *Type:* Dimension table

- user_id (INT) PRIMARY KEY: ID of user
- first_name (TEXT) NOT NULL: Name of user
- last_name (TEXT) NOT NULL: Last Name of user
- gender (TEXT): Gender of user {M | F}
- level (TEXT): User level {free | paid}

**songs** - songs in music database
- *Location:* `s3a://udacity-dend-sparkify-datalake/songs.parquet`
- *Type:* Dimension table

- song_id (TEXT) PRIMARY KEY: ID of Song
- title (TEXT) NOT NULL: Title of Song
- artist_id (TEXT) NOT NULL: ID of song Artist
- year (INT): Year of song release
- duration (FLOAT) NOT NULL: Song duration in milliseconds

**artists** - artists in music database
- *Location:* `s3a://udacity-dend-sparkify-datalake/artists.parquet`
- *Type:* Dimension table

- artist_id (TEXT) PRIMARY KEY: ID of Artist
- name (TEXT) NOT NULL: Name of Artist
- location (TEXT): Name of Artist city
- lattitude (FLOAT): Lattitude location of artist
- longitude (FLOAT): Longitude location of artist

**time** - timestamps of records in songplays broken down into specific units
- *Name:* `s3a://udacity-dend-sparkify-datalake/time.parquet`
- *Type:* Dimension table

- start_time (DATE) PRIMARY KEY: Timestamp of row
- hour (INT): Hour associated to start_time
- day (INT): Day associated to start_time
- week (INT): Week of year associated to start_time
- month (INT): Month associated to start_time 
- year (INT): Year associated to start_time
- weekday (TEXT): Name of week day associated to start_time
