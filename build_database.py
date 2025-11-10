#!/usr/bin/env python3
#build_database.py
import sqlite3
import sys

def get_db_connection_details():
    """Returns the path to the SQLite database file."""
    DB_FILE = "lss_twon.db"
    return DB_FILE


def create_tables(db_path):
    """
    Connects to the SQLite database and creates the necessary tables.
    Checks if tables already exist and prompts the user for action.
    """
    conn = None
    try:
        # Establish the connection to the target database file
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        
        # Enable Foreign Key support in SQLite
        cur.execute("PRAGMA foreign_keys = ON;")
        
        print(f"\nSuccessfully connected to the database: {db_path}")

        # --- Define SQL Commands to Create Tables ---
        create_users_table = """
        CREATE TABLE IF NOT EXISTS Users (
            user_id INTEGER PRIMARY KEY,
            username TEXT
        );
        """
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
        create_follows_table = """
        CREATE TABLE IF NOT EXISTS Follows (
            follower_id INTEGER NOT NULL REFERENCES Users(user_id) ON DELETE CASCADE,
            followee_id INTEGER NOT NULL REFERENCES Users(user_id) ON DELETE CASCADE,
            PRIMARY KEY (follower_id, followee_id)
        );
        """
        create_likes_table = """
        CREATE TABLE IF NOT EXISTS Likes (
            user_id INTEGER NOT NULL REFERENCES Users(user_id) ON DELETE CASCADE,
            tweet_id INTEGER NOT NULL REFERENCES Tweets(tweet_id) ON DELETE CASCADE,
            collected_at TIMESTAMP,
            PRIMARY KEY (user_id, tweet_id)
        );
        """
        
        # --- Check for existing tables ---
        required_tables = {'Users', 'Tweets', 'Follows', 'Likes'}
        cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
        # Fetch all table names and put them in a set for easy comparison
        existing_tables = {row[0] for row in cur.fetchall()}
        
        # Find which of our required tables already exist
        found_tables = required_tables.intersection(existing_tables)
        
        do_create_tables = True # Flag to control execution

        if found_tables:
            print("\n--- Existing Tables Found ---")
            print(f"Found the following tables: {', '.join(found_tables)}")
            print("What would you like to do?")
            print("  [D]elete ALL existing tables and recreate them (WARNING: Data will be lost!)")
            print("  [K]eep existing tables and only create missing ones.")
            print("  [A]bort operation.")
            
            choice = ""
            while choice not in ['d', 'k', 'a']:
                choice = input("Enter your choice (d/k/a): ").strip().lower()

            if choice == 'd':
                print("\nDeleting existing tables...")
                # Drop tables in reverse order of creation to respect foreign keys
                cur.execute("DROP TABLE IF EXISTS Likes;")
                cur.execute("DROP TABLE IF EXISTS Follows;")
                cur.execute("DROP TABLE IF EXISTS Tweets;")
                cur.execute("DROP TABLE IF EXISTS Users;")
                print("All tables deleted. Proceeding to recreate...")
            
            elif choice == 'k':
                print("\nKeeping existing tables. Will only create missing ones...")
                # Do nothing, just proceed to the create step
                pass

            elif choice == 'a':
                print("\nOperation aborted by user.")
                do_create_tables = False
        
        else:
            print("\nNo existing tables found. Creating new tables...")

        # --- Execute the create commands if not aborted ---
        if do_create_tables:
            print("Creating tables if they do not exist...")
            
            cur.execute(create_users_table)
            print("- Users table created or already exists.")
            cur.execute(create_tweets_table)
            print("- Tweets table created or already exists.")
            cur.execute(create_follows_table)
            print("- Follows table created or already exists.")
            cur.execute(create_likes_table)
            print("- Likes table created or already exists.")

            conn.commit()
            print("\nAll tables have been successfully processed.")

    except sqlite3.Error as e:
        print(f"\nDatabase error during table creation: {e}")
        if conn:
            conn.rollback() # Rollback changes on error
    except KeyboardInterrupt:
        print("\nOperation cancelled by user (Ctrl+C).")
        if conn:
            conn.rollback()
        sys.exit(0)
    finally:
        if conn:
            cur.close()
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    db_file_path = get_db_connection_details()
    create_tables(db_file_path)