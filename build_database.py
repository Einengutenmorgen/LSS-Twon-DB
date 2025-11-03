#build_database.py
import sqlite3

def get_db_connection_details():
    """Returns the path to the SQLite database file."""
    DB_FILE = "lss_twon.db"
    return DB_FILE


def create_tables(db_path):
    """
    Connects to the SQLite database and creates the necessary tables
    for the social media relational model.
    """
    conn = None
    try:
        # Establish the connection to the target database file
        # This will create the file if it doesn't exist
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        
        # Enable Foreign Key support in SQLite
        cur.execute("PRAGMA foreign_keys = ON;")
        
        print("\nSuccessfully connected to the database to create tables.")

        # --- SQL Commands to Create Tables ---
        # Note: BIGINT -> INTEGER, TIMESTAMPTZ -> TIMESTAMP
        # Using TIMESTAMP allows sqlite3 to detect and convert to/from datetime objects

        # 1. Users Table
        # user_id is the primary key. Duplicates in username are allowed.
        create_users_table = """
        CREATE TABLE IF NOT EXISTS Users (
            user_id INTEGER PRIMARY KEY,
            username TEXT
        );
        """

        # 2. Tweets Table
        create_tweets_table = """
        CREATE TABLE IF NOT EXISTS Tweets (
            tweet_id INTEGER PRIMARY KEY,
            author_id INTEGER NOT NULL REFERENCES Users(user_id) ON DELETE CASCADE,
            full_text TEXT,
            created_at TIMESTAMP,
            retweet_of_user_id INTEGER REFERENCES Users(user_id) ON DELETE SET NULL,
            collected_at TIMESTAMP
        );
        """

        # 3. Follows Table
        create_follows_table = """
        CREATE TABLE IF NOT EXISTS Follows (
            follower_id INTEGER NOT NULL REFERENCES Users(user_id) ON DELETE CASCADE,
            followee_id INTEGER NOT NULL REFERENCES Users(user_id) ON DELETE CASCADE,
            PRIMARY KEY (follower_id, followee_id)
        );
        """

        # 4. Likes Table
        create_likes_table = """
        CREATE TABLE IF NOT EXISTS Likes (
            user_id INTEGER NOT NULL REFERENCES Users(user_id) ON DELETE CASCADE,
            tweet_id INTEGER NOT NULL REFERENCES Tweets(tweet_id) ON DELETE CASCADE,
            collected_at TIMESTAMP,
            PRIMARY KEY (user_id, tweet_id)
        );
        """

        # Execute the commands
        print("Creating tables if they do not exist...")
        # Drop tables in reverse order of creation to respect foreign keys
        cur.execute("DROP TABLE IF EXISTS Likes;")
        cur.execute("DROP TABLE IF EXISTS Follows;")
        cur.execute("DROP TABLE IF EXISTS Tweets;")
        cur.execute("DROP TABLE IF EXISTS Users;")
        
        cur.execute(create_users_table)
        print("- Users table created or already exists.")
        cur.execute(create_tweets_table)
        print("- Tweets table created or already exists.")
        cur.execute(create_follows_table)
        print("- Follows table created or already exists.")
        cur.execute(create_likes_table)
        print("- Likes table created or already exists.")

        conn.commit()
        print("\nAll tables have been successfully created.")

    except sqlite3.Error as e:
        print(f"\nDatabase error during table creation: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    db_file_path = get_db_connection_details()
    create_tables(db_file_path)