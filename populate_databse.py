# populate_databse.py
import pandas as pd
import sqlite3
import numpy as np

def get_db_path():
    """Returns the path to the SQLite database file."""
    return "LSS_twon.db"

def get_file_paths():
    """Returns the paths to the CSV files."""
    print("\n--- CSV File Paths ---")
    path_follows = '/Users/christophhau/Downloads/UserFollowees1.csv'
    path_likes = '/Users/christophhau/Downloads/users1_likes_df_JulyScrape.csv'
    path_tweets = '/Users/christophhau/Downloads/FolloweeIDs1_tweets_df_JulyPull.csv'
    return path_follows, path_likes, path_tweets

def populate_database(db_path, file_paths):
    """
    Reads data from CSV files and populates the SQLite database tables.
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
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("PRAGMA journal_mode = WAL;")
        cur.execute("PRAGMA synchronous = OFF;")
        cur.execute("PRAGMA temp_store = MEMORY;")
        cur.execute("PRAGMA cache_size = 100000;")

        print(f"\nConnected to database: {db_path}")

        # --- 1. Populate Users Table ---
        print("\nStep 1: Preparing and inserting user data...")
        users = {}
        
        # From follows file
        for _, row in df_follows.iterrows():
            users[row['id']] = users.get(row['id'], row['username'])
            users[row['from_id']] = users.get(row['from_id'], None)
        
        # From likes file
        for _, row in df_likes.iterrows():
            users[row['original_user_id']] = users.get(row['original_user_id'], row['screen_name'])
            users[row['liked_user_id']] = users.get(row['liked_user_id'], None)
        
        # From tweets file
        for _, row in df_tweets.iterrows():
            users[row['original_user_id']] = users.get(row['original_user_id'], row['screen_name'])
            if pd.notna(row['retweeted_user_ID']):
                users[row['retweeted_user_ID']] = users.get(row['retweeted_user_ID'], None)

        user_list = [
            (int(uid), uname) for uid, uname in users.items() if pd.notna(uid)
        ]

        cur.executemany("""
            INSERT OR REPLACE INTO Users (user_id, username) 
            VALUES (?, ?)
        """, user_list)
        print(f"-> Inserted {len(user_list)} unique users.")

        # --- 2. Populate Tweets Table ---
        print("\nStep 2: Preparing and inserting tweet data...")
        all_tweets = {}
        
        # From tweets file
        for _, row in df_tweets.iterrows():
            tweet_id = row['tweet_id']
            if pd.notna(tweet_id) and tweet_id not in all_tweets:
                retweet_id = row.get('retweeted_user_ID')
                all_tweets[tweet_id] = (
                    int(tweet_id),
                    int(row['original_user_id']),
                    row['full_text'],
                    pd.to_datetime(row['created_at'], errors='coerce').isoformat() if pd.notna(row['created_at']) else None,
                    int(retweet_id) if pd.notna(retweet_id) else None,
                    pd.to_datetime(row['collected_at'], errors='coerce').isoformat() if pd.notna(row['collected_at']) else None
                )
        
        # From likes file
        for _, row in df_likes.iterrows():
            tweet_id = row['tweet_id']
            if pd.notna(tweet_id) and tweet_id not in all_tweets:
                all_tweets[tweet_id] = (
                    int(tweet_id),
                    int(row['liked_user_id']),
                    row['full_text'],
                    pd.to_datetime(row['created_at'], errors='coerce').isoformat() if pd.notna(row['created_at']) else None,
                    None,
                    pd.to_datetime(row['collected_at'], errors='coerce').isoformat() if pd.notna(row['collected_at']) else None
                )

        tweet_list = [t for t in all_tweets.values() if t[0] and t[1]]

        cur.executemany("""
            INSERT OR IGNORE INTO Tweets 
            (tweet_id, author_id, full_text, created_at, retweet_of_user_id, collected_at) 
            VALUES (?, ?, ?, ?, ?, ?)
        """, tweet_list)
        print(f"-> Inserted {len(tweet_list)} unique tweets.")

        # --- 3. Populate Follows Table ---
        print("\nStep 3: Preparing and inserting follows data...")
        follows_list = df_follows[['from_id', 'id']].dropna().astype(int).values.tolist()
        
        cur.executemany("""
            INSERT OR IGNORE INTO Follows (follower_id, followee_id) 
            VALUES (?, ?)
        """, follows_list)
        print(f"-> Inserted {len(follows_list)} follow relationships.")

        # --- 4. Populate Likes Table ---
        print("\nStep 4: Preparing and inserting likes data...")
        df_likes_clean = df_likes[['original_user_id', 'tweet_id', 'collected_at']].dropna()
        df_likes_clean['original_user_id'] = df_likes_clean['original_user_id'].astype(int)
        df_likes_clean['tweet_id'] = df_likes_clean['tweet_id'].astype(int)
        df_likes_clean['collected_at'] = pd.to_datetime(df_likes_clean['collected_at'], errors='coerce').apply(
            lambda x: x.isoformat() if pd.notna(x) else None
        )
        
        likes_list = df_likes_clean.values.tolist()

        cur.executemany("""
            INSERT OR IGNORE INTO Likes (user_id, tweet_id, collected_at) 
            VALUES (?, ?, ?)
        """, likes_list)
        print(f"-> Inserted {len(likes_list)} like relationships.")

        # Commit all transactions
        conn.commit()
        print("\nAll data successfully populated.")

    except (sqlite3.Error, FileNotFoundError) as e:
        print(f"\nAn error occurred: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    start_TIME = pd.Timestamp.now()
    print(f"\n--- Database Population Started at {start_TIME} ---")
    db_path = get_db_path()
    file_paths = get_file_paths()
    populate_database(db_path, file_paths)
    print(f"\n--- Database Population Ended at {pd.Timestamp.now()} ---")
    print(f"Total Duration: {pd.Timestamp.now() - start_TIME}")
