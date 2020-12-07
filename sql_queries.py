# DROP TABLES

songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"

# CREATE TABLES

songplay_table_create = (""" CREATE TABLE songplays (songplay_id INT PRIMARY KEY, start_time TIMESTAMP NOT NULL, user_id int NOT NULL, level varchar, song_id varchar, artist_id varchar, session_id INT, location varchar, user_agent varchar) """)

user_table_create = (""" CREATE TABLE users (user_id INT PRIMARY KEY, first_name varchar, last_name varchar, gender varchar, level varchar) """)

song_table_create = (""" CREATE TABLE songs (song_id varchar PRIMARY KEY NOT NULL, title varchar, artist_id varchar, year int, duration NUMERIC) """)

artist_table_create = (""" CREATE TABLE artists (artist_id VARCHAR PRIMARY KEY, artist_name varchar, artist_location varchar, artist_latitude varchar, artist_longitude varchar) """)

time_table_create = (""" CREATE TABLE time (start_time TIMESTAMP, hour NUMERIC, day NUMERIC, week NUMERIC, month NUMERIC, year NUMERIC, weekday NUMERIC)""")

# INSERT RECORDS

songplay_table_insert = (""" INSERT INTO songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) \
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (songplay_id) DO NOTHING
""")

user_table_insert = ("""INSERT INTO users ("user_id", "first_name", "last_name", "gender", "level") \
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (user_id) DO NOTHING
""")

song_table_insert = ("""INSERT INTO songs ("song_id", "title", "artist_id", "year", "duration") \
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (song_id) DO NOTHING
""")

artist_table_insert = (""" INSERT INTO artists ("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude") \
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (artist_id) DO NOTHING
""")


time_table_insert = (""" INSERT INTO time ("start_time", "year", "month", "week", "weekday", "day", "hour") \
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
""")

# FIND SONGS

song_select = ("""SELECT songs.song_id, artists.artist_id FROM 
                songs
                JOIN artists ON (songs.artist_id = artists.artist_id)
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]