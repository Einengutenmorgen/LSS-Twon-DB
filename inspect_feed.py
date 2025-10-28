import sys
import textwrap
from contextlib import contextmanager

# --- Import Core Logic ---
# We import the query class from your existing script to avoid duplicating code.
try:
    from query_database import TwitterDBQuery, get_db_connection_details
except ImportError:
    print("Error: Could not find 'query_database.py'.")
    print("Make sure this script is in the same directory as query_database.py")
    sys.exit(1)
except Exception as e:
    print(f"An unexpected error occurred during import: {e}")
    sys.exit(1)


def pretty_print_tweet(tweet):
    """
    Formats and prints a single tweet record in a readable way.
    A tweet record is a dictionary-like object from RealDictCursor.
    """
    
    print("-" * 80)
    
    # Handle if it's a Retweet
    if tweet.get('retweet_of_user_id'):
        print(f"🔄 Retweeted by: @{tweet['author_username']} (ID: {tweet['author_id']})")
        print(f"  Original Author: @{tweet.get('retweet_of_username', 'N/A')} (ID: {tweet['retweet_of_user_id']})")
    else:
        # It's an Original Tweet
        print(f"👤 Tweet by: @{tweet['author_username']} (ID: {tweet['author_id']})")
    
    print(f"  Date: {tweet['created_at']}")
    print(f"  Tweet ID: {tweet['tweet_id']}")
    print("\n")
    
    # Wrap the text for readability
    wrapped_text = textwrap.fill(
        tweet['full_text'],
        width=76,  # 80 - 4 spaces for indent
        initial_indent="    ",
        subsequent_indent="    "
    )
    print(wrapped_text)
    print("-" * 80)


def main():
    """
    Main function to run the feed inspector.
    """
    print("--- Social Media Feed Inspector ---")
    
    # 1. Get connection details
    try:
        conn_details = get_db_connection_details()
    except EOFError:
        print("\nOperation cancelled.")
        sys.exit(0)
        
    # 2. Create the query object
    try:
        tester = TwitterDBQuery(conn_details)
        # Test the connection immediately
        with tester._get_cursor() as cur:
            cur.execute("SELECT 1;")
        print("Database connection successful.\n")
    except Exception as e:
        print(f"Failed to connect to the database: {e}")
        sys.exit(1)

    # 3. Start the inspection loop
    while True:
        try:
            print()
            user_input = input("Enter a username, user_id, or 'q' to quit: ").strip()

            if user_input.lower() == 'q':
                break

            user_id = None
            
            # Try to convert to int. If it fails, treat it as a username.
            try:
                user_id = int(user_input)
                print(f"Searching for user_id: {user_id}")
            except ValueError:
                print(f"Searching for username: @{user_input}")
                user_record = tester.get_user_by_username(user_input)
                if user_record:
                    user_id = user_record['user_id']
                    print(f"Found user_id: {user_id}")
                else:
                    print(f"Error: Username '@{user_input}' not found in the database.")
                    continue
            
            if not user_id:
                print("Could not determine user.")
                continue

            # Get the feed
            limit_input = input("How many tweets to fetch? (default: 25): ").strip()
            limit = int(limit_input) if limit_input.isdigit() else 25
            
            feed = tester.get_user_feed(user_id, limit=limit)
            
            if not feed:
                print(f"\n--- Feed for {user_id} is empty ---")
                print("(This user may not exist, or no tweets from them/their followees have been collected).")
                continue
            
            # Print the feed
            print(f"\n--- Showing last {len(feed)} tweets for {user_id} ---")
            for tweet in feed:
                pretty_print_tweet(tweet)
            print(f"--- End of feed for {user_id} ---")

        except EOFError:
            break # Exit on Ctrl+D
        except Exception as e:
            print(f"\nAn error occurred: {e}")
            
    print("\nExiting feed inspector. Goodbye!")


if __name__ == "__main__":
    main()
