import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# Extract data from CSV
file_input = "spotify_songs.csv"
df = pd.read_csv(file_input)

# cleaning missing values
df["track_name"].fillna("Unknown_track", inplace=True)
df["track_artist"].fillna("Unknown_artist", inplace=True)
df["track_album_name"].fillna("Unknown_album", inplace=True)

# Extract only the first 4 characters of the 'track_album_release_date' string
df['track_album_release_date'] = df['track_album_release_date'].astype(str).str[:4]

# Convert 'duration_ms' to minutes and cast to int64
df['duration_min'] = (df['duration_ms'] / (1000 * 60)).astype('int64')
# Drop the original 'duration_ms' column
df.drop('duration_ms', axis=1, inplace=True)

# Snowflake account credentials and connection details
user = "MARYAN"
password = "Hejsan123"  # Replace with your actual password
account = "DRXDGZU-CS03614"
database = "assesment_python"
schema = "assesment_python"

# Create a connection to Snowflake
conn = snowflake.connector.connect(
    user=user,
    password=password,
    account=account,
    database=database,
    schema=schema
)



write_pandas(conn, df, "spotify_song_1", auto_create_table=True)

# Close the connection
conn.close()


