import psycopg2
from psycopg2 import sql

def get_db_connection_details():
    """Gets PostgreSQL connection details from the user."""
    # print("--- Enter PostgreSQL Connection Details ---")
    # dbname = input("Database Name (will be created if it doesn't exist): ")
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


def create_database(conn_details):
    """
    Connects to the PostgreSQL server and creates the target database if it does not exist.
    """
    # Connect to the default 'postgres' database to run admin commands
    maintenance_conn_details = conn_details.copy()
    target_dbname = maintenance_conn_details.pop('dbname')
    maintenance_conn_details['dbname'] = 'postgres'

    conn = None
    try:
        conn = psycopg2.connect(**maintenance_conn_details)
        conn.autocommit = True  # CREATE DATABASE cannot run inside a transaction block
        cur = conn.cursor()

        # Check if the target database already exists
        cur.execute(sql.SQL("SELECT 1 FROM pg_database WHERE datname = %s"), [target_dbname])
        exists = cur.fetchone()

        if not exists:
            print(f"Database '{target_dbname}' does not exist. Creating now...")
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(target_dbname)))
            print(f"Database '{target_dbname}' created successfully.")
        else:
            print(f"Database '{target_dbname}' already exists. Proceeding to create tables.")
        
        cur.close()
        return True

    except psycopg2.Error as e:
        print(f"\nDatabase error during creation: {e}")
        return False
    finally:
        if conn:
            conn.close()


def create_tables(conn_details):
    """
    Connects to the PostgreSQL database and creates the necessary tables
    for the social media relational model.
    """
    conn = None
    try:
        # Establish the connection to the target database
        conn = psycopg2.connect(**conn_details)
        cur = conn.cursor()
        print("\nSuccessfully connected to the database to create tables.")

        # --- SQL Commands to Create Tables ---

        # 1. Users Table
        # user_id is the primary key. Duplicates in username are allowed.
        create_users_table = """
        CREATE TABLE IF NOT EXISTS Users (
            user_id BIGINT PRIMARY KEY,
            username TEXT
        );
        """

        # 2. Tweets Table
        create_tweets_table = """
        CREATE TABLE IF NOT EXISTS Tweets (
            tweet_id BIGINT PRIMARY KEY,
            author_id BIGINT NOT NULL REFERENCES Users(user_id) ON DELETE CASCADE,
            full_text TEXT,
            created_at TIMESTAMPTZ,
            retweet_of_user_id BIGINT REFERENCES Users(user_id) ON DELETE SET NULL,
            collected_at TIMESTAMPTZ
        );
        """

        # 3. Follows Table
        create_follows_table = """
        CREATE TABLE IF NOT EXISTS Follows (
            follower_id BIGINT NOT NULL REFERENCES Users(user_id) ON DELETE CASCADE,
            followee_id BIGINT NOT NULL REFERENCES Users(user_id) ON DELETE CASCADE,
            PRIMARY KEY (follower_id, followee_id)
        );
        """

        # 4. Likes Table
        create_likes_table = """
        CREATE TABLE IF NOT EXISTS Likes (
            user_id BIGINT NOT NULL REFERENCES Users(user_id) ON DELETE CASCADE,
            tweet_id BIGINT NOT NULL REFERENCES Tweets(tweet_id) ON DELETE CASCADE,
            collected_at TIMESTAMPTZ,
            PRIMARY KEY (user_id, tweet_id)
        );
        """

        # Execute the commands
        print("Creating tables if they do not exist...")
        # Drop tables in reverse order of creation to respect foreign keys
        cur.execute("DROP TABLE IF EXISTS Likes, Follows, Tweets, Users CASCADE;")
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

    except psycopg2.Error as e:
        print(f"\nDatabase error during table creation: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    connection_details = get_db_connection_details()
    if create_database(connection_details):
        create_tables(connection_details)

