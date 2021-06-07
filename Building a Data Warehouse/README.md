# Sparkify's Data Warehouse ETL process

## Project description

Sparkify is a music streaming startup with a growing user base and song database.

Their user activity and songs metadata data resides in json files in S3. The goal of the current project is to build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. 

## Project Structure

The project contains the following scripts (the order here implies the logical ordering of how to run them)

1. Fill all configuration parameters in the config file (`dwh.cfg`), but leave the following two fields empty (these will be written by the `create_cluster.py` script) 
 - `HOST` (inside the `DB` configuration section) 
 - And the `ARN` (inside the `IAM_ROLE` configuration section) 

2. Run the *create_cluster* script to set up the infrastructure for this project.

3. Run the *create_tables* script to set up the database staging and analytical tables

4. Run the *etl* script to extract data from the files in S3, stage it in redshift and store it in the dimensional tables.

5. Run the *etl* script to extract data from the files in S3, stage it in redshift and store it in the dimensional tables.

6. Run the *validate_project* script to verify that the project was completed successfully. The script performs a simple count of each analytical table.

7. Run the *destroy_cluster* script to delete the created AWS Redshift cluster.

## Database Design

**Staging Tables**
- staging_events
- staging_songs

**Fact Table**
- songplays - records in event data associated with song plays i.e. records with page NextSong 

**Dimension Tables**
- users - users in the app 
- songs - songs in music database 
- artists - artists in music database 
- time - timestamps of records in songplays broken down into specific units 

## Tables Specification

### Events table

- *Name:* `staging_events`
- *Type:* Staging table

| Column | Type | Description |
| ------ | ---- | ----------- |
| `artist` | `VARCHAR(500)` | The artist name |
| `auth` | `VARCHAR(20)` | The authentication status |
| `firstName` | `VARCHAR(500)` | The first name of the user |
| `gender` | `CHAR(1)` | The gender of the user |
| `itemInSession` | `INTEGER` | The sequence number of the item inside a given session |
| `lastName` | `VARCHAR(500)` | The last name of the user |
| `length` | `DECIMAL(12, 5)` | The duration of the song |
| `level` | `VARCHAR(10)` | The level of the user´s plan (free or premium) |
| `location` | `VARCHAR(500)` | The location of the user |
| `method` | `VARCHAR(20)` | The method of the http request |
| `page` | `VARCHAR(500)` | The page that the event occurred |
| `registration` | `FLOAT` | The time that the user registered |
| `sessionId` | `INTEGER` | The session id |
| `song` | `VARCHAR(500)` | The song name |
| `status` | `INTEGER` | The status |
| `ts` | `VARCHAR(50)` | The timestamp that this event occurred |
| `userAgent` | `VARCHAR(500)` | The user agent he was using |
| `userId` | `INTEGER` | The user id |

### Songs table

- *Name:* `staging_songs`
- *Type:* Staging table

| Column | Type | Description |
| ------ | ---- | ----------- |
| `num_songs` | `INTEGER` | The number of songs of this artist |
| `artist_id` | `VARCHAR(20)` | The artist id |
| `artist_latitude` | `DECIMAL(12, 5)` | The artist latitude location |
| `artist_longitude` | `DECIMAL(12, 5)` | The artist longitude location |
| `artist_location` | `VARCHAR(500)` | The artist descriptive location |
| `artist_name` | `VARCHAR(500)` | The artist name |
| `song_id` | `VARCHAR(20)` | The song id |
| `title` | `VARCHAR(500)` | The title |
| `duration` | `DECIMAL(15, 5)` | The duration of the song |
| `year` | `INTEGER` | The year of the song |

### Song Plays table

- *Name:* `songplays`
- *Type:* Fact table

| Column | Type | Description |
| ------ | ---- | ----------- |
| `songplay_id` | `INTEGER IDENTITY(0,1) SORTKEY` | The main identification of the table | 
| `start_time` | `TIMESTAMP NOT NULL` | The timestamp that this song play log happened |
| `user_id` | `INTEGER NOT NULL REFERENCES users (user_id)` | The user id that triggered this song play log. It cannot be null, as we don't have song play logs without being triggered by an user.  |
| `level` | `VARCHAR(10)` | The level of the user that triggered this song play log |
| `song_id` | `VARCHAR(20) REFERENCES songs (song_id)` | The identification of the song that was played. It can be null.  |
| `artist_id` | `VARCHAR(20) REFERENCES artists (artist_id)` | The identification of the artist of the song that was played. |
| `session_id` | `INTEGER NOT NULL` | The session_id of the user on the app |
| `location` | `VARCHAR(500)` | The location where this song play log was triggered  |
| `user_agent` | `VARCHAR(500)` | The user agent of our app |

### Users table

- *Name:* `users`
- *Type:* Dimension table

| Column | Type | Description |
| ------ | ---- | ----------- |
| `user_id` | `INTEGER PRIMARY KEY` | The main identification of an user |
| `first_name` | `VARCHAR(500) NOT NULL` | First name of the user, can not be null. It is the basic information we have from the user |
| `last_name` | `VARCHAR(500) NOT NULL` | Last name of the user. |
| `gender` | `CHAR(1)` | The gender is stated with just one character `M` (male) or `F` (female). Otherwise it can be stated as `NULL` |
| `level` | `VARCHAR(10) NOT NULL` | The level stands for the user app plans (`premium` or `free`) |


### Songs table

- *Name:* `songs`
- *Type:* Dimension table

| Column | Type | Description |
| ------ | ---- | ----------- |
| `song_id` | `VARCHAR(20) PRIMARY KEY` | The main identification of a song | 
| `title` | `VARCHAR(500) NOT NULL SORTKEY` | The title of the song. It can not be null, as it is the basic information we have about a song. |
| `artist_id` | `VARCHAR NOT NULL DISTKEY REFERENCES artists (artist_id)` | The artist id, it can not be null as we don't have songs without an artist, and this field also references the artists table. |
| `year` | `INTEGER NOT NULL` | The year that this song was made |
| `duration` | `NUMERIC (15, 5) NOT NULL` | The duration of the song |


### Artists table

- *Name:* `artists`
- *Type:* Dimension table

| Column | Type | Description |
| ------ | ---- | ----------- |
| `artist_id` | `VARCHAR(20) PRIMARY KEY` | The main identification of an artist |
| `name` | `VARCHAR(500) NOT NULL` | The name of the artist |
| `location` | `VARCHAR(500)` | The location where the artist are from |
| `latitude` | `DECIMAL(12,6)` | The latitude of the location that the artist are from |
| `longitude` | `DECIMAL(12,6)` | The longitude of the location that the artist are from |

### Time table

- *Name:* `time`
- *Type:* Dimension table

| Column | Type | Description |
| ------ | ---- | ----------- |
| `start_time` | `TIMESTAMP NOT NULL PRIMARY KEY` | The timestamp itself, serves as the main identification of this table |
| `hour` | `NUMERIC NOT NULL` | The hour from the timestamp  |
| `day` | `NUMERIC NOT NULL` | The day of the month from the timestamp |
| `week` | `NUMERIC NOT NULL` | The week of the year from the timestamp |
| `month` | `NUMERIC NOT NULL` | The month of the year from the timestamp |
| `year` | `NUMERIC NOT NULL` | The year from the timestamp |
| `weekday` | `NUMERIC NOT NULL` | The week day from the timestamp |


## Queries and Results

Number of rows in each table:

| Table            | rows  |
|---               | ---   |
| staging_events   | 8056  |
| staging_songs    | 14896 |
| artists          | 10025 |
| songplays        | 333   |
| songs            | 14896 |
| time             |  8023 |
| users            |  104  |
