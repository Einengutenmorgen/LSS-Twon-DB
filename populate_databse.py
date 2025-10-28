import pandas as pd
import psycopg2
from psycopg2 import extras
import numpy as np

def get_db_connection_details():
    """Gets PostgreSQL connection details from the user."""
    # print("--- Enter PostgreSQL Connection Details ---")
    # dbname = input("Database Name: ")
    # user = input("User: ")
    # password = input("Password: ")
    # host = input("Host (default: localhost): ") or "localhost"
    # port = input("Port (default: 5432): ") or "5432"
    DB_CONFIG = {
    "user": "christophhau",
    "password": "",
    "host": "localhost",
    "port": "5432",
    "dbname": "LSS_twon"}

    return DB_CONFIG

def get_file_paths():
    """Gets the paths to the three CSV files from the user."""
    print("\n--- Enter CSV File Paths ---")
    path_follows = '/Users/christophhau/Downloads/UserFollowees1.csv'
    path_likes = '/Users/christophhau/Downloads/users1_likes_df_JulyScrape.csv'
    path_tweets = '/Users/christophhau/Downloads/FolloweeIDs1_tweets_df_JulyPull.csv'

    return path_follows, path_likes, path_tweets

def populate_database(conn_details, file_paths):
    """
    Reads data from CSV files and populates the PostgreSQL database tables.
    """
    path_follows, path_likes, path_tweets = file_paths
    conn = None
    try:
        # Load data into pandas DataFrames
        print("\nLoading CSV files into memory...")
        df_follows = pd.read_csv(path_follows)
        df_likes = pd.read_csv(path_likes)
        df_tweets = pd.read_csv(path_tweets)
        print("CSV files loaded successfully.")

        # Establish the connection
        conn = psycopg2.connect(**conn_details)
        cur = conn.cursor()
        print("\nSuccessfully connected to the database for data population.")

        # --- 1. Populate Users Table ---
        print("\nStep 1: Preparing and inserting user data...")
        users = {}
        # From follows file
        for _, row in df_follows.iterrows():
            users[row['id']] = users.get(row['id'], row['username'])
            users[row['from_id']] = users.get(row['from_id'], None)
        # From likes file (CORRECTED LOGIC)
        for _, row in df_likes.iterrows():
            # original_user_id is the one who performed the like
            users[row['original_user_id']] = users.get(row['original_user_id'], row['screen_name']) 
            # liked_user_id is the author of the tweet
            users[row['liked_user_id']] = users.get(row['liked_user_id'], None) 
        # From tweets file
        for _, row in df_tweets.iterrows():
            users[row['original_user_id']] = users.get(row['original_user_id'], row['screen_name'])
            if pd.notna(row['retweeted_user_ID']):
                users[row['retweeted_user_ID']] = users.get(row['retweeted_user_ID'], None)

        user_list = [
            (int(uid), uname) for uid, uname in users.items() if pd.notna(uid)
        ]

        insert_query = """
        INSERT INTO Users (user_id, username) 
        VALUES %s 
        ON CONFLICT (user_id) 
        DO UPDATE SET username = COALESCE(Users.username, EXCLUDED.username);
        """
        extras.execute_values(cur, insert_query, user_list, template=None, page_size=1000)
        print(f"-> Processed {len(user_list)} unique user IDs. Users table is up to date.")

        # --- 2. Populate Tweets Table ---
        print("\nStep 2: Preparing and inserting tweet data...")
        all_tweets = {}
        # From tweets file
        for _, row in df_tweets.iterrows():
            tweet_id = row['tweet_id']
            if pd.notna(tweet_id) and tweet_id not in all_tweets:
                 all_tweets[tweet_id] = (
                    int(tweet_id),
                    int(row['original_user_id']),
                    row['full_text'],
                    pd.to_datetime(row['created_at'], errors='coerce'),
                    pd.to_numeric(row.get('retweeted_user_ID'), errors='coerce'),
                    pd.to_datetime(row['collected_at'], errors='coerce')
                )
        # From likes file (CORRECTED LOGIC - author is liked_user_id)
        for _, row in df_likes.iterrows():
            tweet_id = row['tweet_id']
            if pd.notna(tweet_id) and tweet_id not in all_tweets:
                all_tweets[tweet_id] = (
                    int(tweet_id),
                    int(row['liked_user_id']), # Author of the liked tweet
                    row['full_text'],
                    pd.to_datetime(row['created_at'], errors='coerce'),
                    np.nan, # No retweet info in likes file
                    pd.to_datetime(row['collected_at'], errors='coerce')
                )

        tweet_list = [
            (t[0], t[1], t[2], t[3], None if pd.isna(t[4]) else int(t[4]), t[5])
            for t in all_tweets.values()
            if pd.notna(t[0]) and pd.notna(t[1])
        ]

        insert_query = """
        INSERT INTO Tweets (tweet_id, author_id, full_text, created_at, retweet_of_user_id, collected_at) 
        VALUES %s ON CONFLICT (tweet_id) DO NOTHING;
        """
        extras.execute_values(cur, insert_query, tweet_list, template=None, page_size=1000)
        print(f"-> Processed {len(tweet_list)} unique tweets. Tweets table is up to date.")


        # --- 3. Populate Follows Table ---
        print("\nStep 3: Preparing and inserting follows data...")
        follows_list = df_follows[['from_id', 'id']].dropna().astype(int).values.tolist()
        insert_query = "INSERT INTO Follows (follower_id, followee_id) VALUES %s ON CONFLICT (follower_id, followee_id) DO NOTHING;"
        extras.execute_values(cur, insert_query, follows_list, template=None, page_size=1000)
        print(f"-> Processed {len(follows_list)} follow relationships. Follows table is up to date.")

        # --- 4. Populate Likes Table ---
        print("\nStep 4: Preparing and inserting likes data...")
        # (CORRECTED LOGIC - user who likes is original_user_id)
        df_likes_clean = df_likes[['original_user_id', 'tweet_id', 'collected_at']].dropna()
        df_likes_clean.rename(columns={'original_user_id': 'user_id'}, inplace=True)
        df_likes_clean['user_id'] = df_likes_clean['user_id'].astype(int)
        df_likes_clean['tweet_id'] = df_likes_clean['tweet_id'].astype(int)
        df_likes_clean['collected_at'] = pd.to_datetime(df_likes_clean['collected_at'], errors='coerce')
        
        likes_list = [tuple(x) for x in df_likes_clean.to_numpy()]

        insert_query = "INSERT INTO Likes (user_id, tweet_id, collected_at) VALUES %s ON CONFLICT (user_id, tweet_id) DO NOTHING;"
        extras.execute_values(cur, insert_query, likes_list, template=None, page_size=1000)
        print(f"-> Processed {len(likes_list)} like relationships. Likes table is up to date.")

        # Commit all transactions
        conn.commit()
        print("\nAll data has been successfully populated.")

    except (psycopg2.Error, FileNotFoundError) as e:
        print(f"\nAn error occurred: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cur.close()
            conn.close()
            print("Database connection closed.")


if __name__ == "__main__":
    conn_details = get_db_connection_details()
    file_paths = get_file_paths()
    populate_database(conn_details, file_paths)

