import sqlalchemy
from sqlalchemy.orm import sessionmaker
import sqlite3
import pandas as pd
import requests
import json
from datetime import datetime
import datetime

DATABASE_NAME = 'music_data.sqlite'
DATABASE_LOCATION = f'sqlite:///{DATABASE_NAME}'
USER_ID = 'nelisw1'
token = 'BQClHiH8d9HoZ8POid4w8-YxPpdTgb_9mS6-fppA_Zg3xCjptxLrHmwiB51wSizFzb184XrT43n' \
        'wLM1EtA6VcmtpgRyZbvVfO7gndYN_ZKDVfUT3BntKLo8jxyI3_8g3cJ2iYCsiCD_U'
source = 'https://api.spotify.com/v1/me/player/recently-played'


def check_if_valid_data(df: pd.DataFrame):

    # check for empty Frame
    if df.empty:
        print("No Songs downloaded. Finished Execution")
        return False

    # check for duplicates, primary key check
    if not df['played_at'].is_unique:
        raise Exception("Primary key check violated.")

    # check for nulls,empty cells
    if df.isnull().values.any():
        raise Exception("Null values found.")

    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

    # only download from the last 24 hours, check timestamp
    timestamps = df['timestamp'].tolist()
    for stamp in timestamps:
        if datetime.datetime.strptime(stamp, "%Y-%m-%d") < yesterday:
            raise Exception("At least one song is not from the last 24 hours.")

    return True


if __name__ == "__main__":

    # creating request URL
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}'
    }

    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000    # in unix milliseconds

    endpoint = f'{source}?after={yesterday_unix_timestamp}'

    # extracting the data
    with requests.get(endpoint, headers=headers) as r:
        if r.status_code not in range(200, 300):
            Exception("Error, check if key expired.")
        data = r.json()

    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []

    for item in data['items']:
        song_names.append(item['track']['name'])
        artist_names.append(item['track']['artists'][0]['name'])
        played_at_list.append(item['played_at'])
        timestamps.append(item['played_at'][:10])

    songs_dict = {
        'song_name': song_names,
        'artist_name': artist_names,
        'played_at': played_at_list,
        'timestamp': timestamps
    }

    df_1 = pd.DataFrame(songs_dict)

    # validating data
    if check_if_valid_data(df_1):
        print("Data valid, proceed to Load.")

        # Load
        engine = sqlalchemy.create_engine(DATABASE_LOCATION)
        con = sqlite3.connect(DATABASE_NAME)
        cursor = con.cursor()

        sql_query = """
        CREATE TABLE IF NOT EXISTS my_played_tracks(
            song_name VARCHAR(200),
            artist_name VARCHAR(200),
            played_at VARCHAR(200),
            timestamp VARCHAR(200),
            CONSTRAINT primary_key_constraint PRIMARY KEY(played_at)
        )
        """
        cursor.execute(sql_query)
        print("Opened database successfully")

        try:
            # insert with dataframe to_sql, combined with SQLAlchemy
            df_1.to_sql('my_played_tracks', engine, index=False, if_exists='append')
        except:
            print("Data already exists in the database.")

        cursor.close()
        con.close()
