import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE staging_events(
                    event_id bigint,
                    artist varchar,
                    auth varchar,
                    fistName varchar,
                    gender varchar
                    itemInSession integer,
                    lastName varchar,
                    length double precision,
                    level varchar,
                    location varchar,
                    method varchar,
                    page varchar,
                    registration varchar,
                    sessionId integer,
                    song varchar,
                    status integer,
                    ts bigint,
                    userAgent text,
                    user_id integer
                    
)
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs(
                    song_id varchar,
                    num_songs integer,
                    artist_id varchar, 
                    artist_latitude double precision,
                    artist_longitude double precision,
                    artist_location varchar,
                    artist_name varchar,
                    title varchar,
                    duration double precision,
                    year integer
                    
)
""")

songplay_table_create = ("""
    CREATE TABLE songplay(
                    songplay_id integer IDENTITY(0,1) NOT NULL,
                    start_time timestamp NOT NULL,
                    user_id varchar NOT NULL,
                    level varchar NOT NULL,
                    song_id varchar NOT NULL,
                    artist_id varchar NOT NULL,
                    session_id integer NOT NULL,
                    location varchar,
                    user_agent varchar,
                    PRIMARY KEY (songplay_id),
)
""")

user_table_create = ("""
    CREATE TABLE users(
                    user_id integer NOT NULL,
                    first_name varchar,
                    last_name varchar,
                    gender varchar,
                    level varchar,
                    PRIMARY KEY (user_id)
)
""")

song_table_create = ("""
    CREATE TABLE songs(
                    song_id varchar NOT NULL,
                    title varchar NOT NULL,
                    artist_id varchar NOT NULL,
                    year integer NOT NULL,
                    duration double precision NOT NULL,
                    PRIMARY KEY (song_id),
)
""")

artist_table_create = ("""
    CREATE TABLE artists(
                    artist_id varchar NOT NULL,
                    name varchar NOT NULL,
                    location varchar,
                    latitude double precision,
                    longitude double precision,
                    PRIMARY KEY (artist_id)
)
""")

time_table_create = ("""
    CREATE TABLE time(
                    start_time bigint NOT NULL,
                    hour integer,
                    day integer,
                    week integer,
                    month integer,
                    year integer,
                    weekday integer,
                    PRIMARY KEY (start_time)
)
""")

# STAGING TABLES

staging_events_copy = ("""copy staging_events 
                          from {}
                          iam_role {}
                          json {};
""").format(config.get('S3','LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""copy staging_songs 
                          from {} 
                          iam_role {}
                          json 'auto';
""").format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
SELECT  
            TIMESTAMP 'epoch' + e.ts/1000 * interval '1 second' as start_time, 
            e.user_id, 
            e.level, 
            s.song_id,
            s.artist_id, 
            e.session_id,
            e.location, 
            e.user_agent
FROM staging_events e, staging_songs s
WHERE e.page = 'NextSong' 
AND e.song_title = s.title 
AND e.artist_name = s.artist_name 
AND e.song_length = s.duration
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT  
            user_id, 
            user_first_name, 
            user_last_name, 
            user_gender, 
            user_level
FROM staging_events
WHERE page = 'NextSong'
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration) 
SELECT DISTINCT 
            song_id, 
            title,
            artist_id,
            year,
            duration
FROM staging_songs
WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude) 
SELECT DISTINCT 
            artist_id,
            artist_name,
            artist_location,
            artist_latitude,
            artist_longitude
FROM staging_songs
WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
INSERT INTO time(start_time, hour, day, week, month, year, weekday)
SELECT start_time, 
            extract(hour from timestamp 'epoch' + start_time * interval '0.001 seconds') as hour,
            extract(day from timestamp 'epoch' + start_time * interval '0.001 seconds') as day,
            extract(week from timestamp 'epoch' + start_time * interval '0.001 seconds') as week,
            extract(year from timestamp 'epoch' + start_time * interval '0.001 seconds') as year,
            extract(weekday from timestamp 'epoch' + start_time * interval '0.001 seconds') as weekday
FROM songplays
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
