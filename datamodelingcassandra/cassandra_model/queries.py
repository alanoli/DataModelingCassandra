# 1. Give me the artist, song title and song's length in the music app history
# that was heard during sessionId = 338, and itemInSession = 4

QUERY_SELECT_1 = """
    SELECT
        artist
        ,song_title
        ,song_length
    FROM
        music_history_by_artist_and_song_data
    WHERE
        session_id = %s
        AND item_in_session = %s
"""

QUERY_CREATE_1 = """
    CREATE TABLE IF NOT EXISTS music_history_by_artist_and_song_data
    (
        artist text
        ,song_title text
        ,song_length decimal
        ,session_id int
        ,item_in_session int
        ,PRIMARY KEY ((session_id, item_in_session), artist, song_title)
    )
"""

QUERY_INSERT_1 = """
    INSERT INTO music_history_by_artist_and_song_data
    (
        artist
        ,song_title
        ,song_length
        ,session_id
        ,item_in_session
    )
    VALUES (%s, %s, %s, %s, %s)
"""

# -----------------------------------------------------------------------------------------------
# 2. Give me only the following: name of artist, song (sorted by itemInSession)
# and user (first and last name) for userid = 10, sessionid = 182
QUERY_SELECT_2 = """
    SELECT
        artist
        ,song_title
    FROM
        music_history_by_user_and_session
    WHERE
        user_id = %s
        AND session_id = %s
"""

QUERY_CREATE_2 = """
    CREATE TABLE IF NOT EXISTS music_history_by_user_and_session
    (
        artist text
        ,song_title text
        ,session_id int
        ,user_id int
        ,first_name text
        ,last_name text
        ,item_in_session int
        ,PRIMARY KEY ((user_id, session_id))
    )
"""

QUERY_INSERT_2 = """
    INSERT INTO music_history_by_user_and_session
    (
        artist
        ,song_title
        ,session_id
        ,user_id
        ,first_name
        ,last_name
        ,item_in_session
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s)
"""


# DROPING TABLES
QUERY_DROP_ALL_TABLES = """
    DROP TABLE music_history_by_artist_and_song_data;
    DROP TABLE music_history_by_user_and_session;
"""