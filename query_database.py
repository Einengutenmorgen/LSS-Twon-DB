import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
import numpy as np
from contextlib import contextmanager
import textwrap # Added for formatting
import datetime # Added for date testing

# --- Connection Helper ---

def get_db_connection_details():
    """Gets PostgreSQL connection details from the user."""
    print("--- Enter PostgreSQL Connection Details for Testing ---")
    dbname = input("Database Name: ") or "LSS_twon"
    user = input("User: ") or "christophhau"
    password = input("Password: ")
    host = input("Host (default: localhost): ") or "localhost"
    port = input("Port (default: 5432): ") or "5432"
    return {
        "dbname": dbname,
        "user": user,
        "password": password,
        "host": host,
        "port": port,
    }

class TwitterDBQuery:
    """
    A class to handle querying the Twitter relational database.
    """
    def __init__(self, db_params):
        self.db_params = db_params
        self.conn = None

    @contextmanager
    def _get_cursor(self):
        """A context manager for database connections and cursors."""
        try:
            self.conn = psycopg2.connect(**self.db_params)
            cur = self.conn.cursor(cursor_factory=RealDictCursor)
            yield cur
        except psycopg2.Error as e:
            print(f"Database connection error: {e}")
            raise
        finally:
            if self.conn:
                self.conn.close()

    def get_user_likes(self, user_id):
        """Gets a list of all tweet IDs liked by a specific user."""
        query = "SELECT tweet_id FROM Likes WHERE user_id = %s;"
        with self._get_cursor() as cur:
            cur.execute(query, (user_id,))
            return [row['tweet_id'] for row in cur.fetchall()]

    def get_user_by_username(self, username):
        """Gets a user's data by their username."""
        query = "SELECT user_id FROM Users WHERE username = %s LIMIT 1;"
        with self._get_cursor() as cur:
            cur.execute(query, (username,))
            return cur.fetchone()

    def get_followers(self, user_id):
        """Gets a list of user_ids for a given user's followers (people who follow them)."""
        query = "SELECT follower_id FROM Follows WHERE followee_id = %s;"
        with self._get_cursor() as cur:
            cur.execute(query, (user_id,))
            return [row['follower_id'] for row in cur.fetchall()]

    def get_tweet_by_id(self, tweet_id):
        """Gets a single tweet by its ID."""
        query = "SELECT * FROM Tweets WHERE tweet_id = %s;"
        with self._get_cursor() as cur:
            cur.execute(query, (tweet_id,))
            return cur.fetchone()

    def get_followees(self, user_id):
        """Gets a list of user_ids for a given user's followees (people they follow)."""
        query = "SELECT followee_id FROM Follows WHERE follower_id = %s;"
        with self._get_cursor() as cur:
            cur.execute(query, (user_id,))
            return [row['followee_id'] for row in cur.fetchall()]

    def get_user_feed(self, user_id, limit=100):
        """
        Reconstructs a user's feed, composed of tweets from their followees
        and their own tweets, in reverse chronological order.
        
        Joins with the Users table to also return usernames.
        """
        query = """
        SELECT
            t.tweet_id,
            t.author_id,
            u.username AS author_username,
            t.full_text,
            t.created_at,
            t.retweet_of_user_id,
            ru.username AS retweet_of_username
        FROM
            Tweets t
        JOIN  -- Join to get the author's username
            Users u ON t.author_id = u.user_id
        LEFT JOIN -- Left Join to get the retweeted user's name (if it's a retweet)
            Users ru ON t.retweet_of_user_id = ru.user_id
        WHERE
            t.author_id IN (
                -- 1. People the user follows (followees)
                SELECT followee_id
                FROM Follows
                WHERE follower_id = %s  -- Param 1: target_user_id
                
                UNION
                
                -- 2. The user themselves
                SELECT %s  -- Param 2: target_user_id
            )
        ORDER BY
            t.created_at DESC
        LIMIT %s; -- Param 3: limit
        """
        with self._get_cursor() as cur:
            # Pass the user_id twice (for the subquery and the UNION)
            cur.execute(query, (user_id, user_id, limit))
            return cur.fetchall()

    # --- NEW METHOD 1 ---
    
    def get_user_feed_until(self, user_id, enddate, limit=100):
        """
        Reconstructs a user's feed, composed of tweets from their followees
        and their own tweets, created on or before a specific end date.
        
        :param user_id: The ID of the user whose feed to reconstruct.
        :param enddate: A date or timestamp. Only tweets created at or before
                        this time will be included.
        :param limit: The maximum number of tweets to return.
        :return: A list of post dictionaries.
        """
        query = """
        SELECT
            t.tweet_id,
            t.author_id,
            u.username AS author_username,
            t.full_text,
            t.created_at,
            t.retweet_of_user_id,
            ru.username AS retweet_of_username
        FROM
            Tweets t
        JOIN
            Users u ON t.author_id = u.user_id
        LEFT JOIN
            Users ru ON t.retweet_of_user_id = ru.user_id
        WHERE
            t.author_id IN (
                -- 1. People the user follows (followees)
                SELECT followee_id
                FROM Follows
                WHERE follower_id = %s  -- Param 1: target_user_id
                
                UNION
                
                -- 2. The user themselves
                SELECT %s  -- Param 2: target_user_id
            )
        AND -- Filter by the end date
            t.created_at <= %s  -- Param 3: enddate
        ORDER BY
            t.created_at DESC
        LIMIT %s; -- Param 4: limit
        """
        with self._get_cursor() as cur:
            cur.execute(query, (user_id, user_id, enddate, limit))
            return cur.fetchall()

    # --- NEW METHOD 2 (and its helpers) ---

    def get_user_posts(self, user_id):
        """
        Gets all posts (tweets and retweets) authored by a specific user.
        
        :param user_id: The ID of the author.
        :return: A list of post dictionaries.
        """
        query = """
        SELECT
            t.tweet_id,
            t.author_id,
            u.username AS author_username,
            t.full_text,
            t.created_at,
            t.retweet_of_user_id,
            ru.username AS retweet_of_username
        FROM
            Tweets t
        JOIN
            Users u ON t.author_id = u.user_id
        LEFT JOIN
            Users ru ON t.retweet_of_user_id = ru.user_id
        WHERE
            t.author_id = %s  -- Filter for only this author
        ORDER BY
            t.created_at DESC;
        """
        with self._get_cursor() as cur:
            cur.execute(query, (user_id,))
            return cur.fetchall()

    @staticmethod
    def _format_post(post):
        """
        Formats a single post dictionary into a human-readable string.
        
        :param post: A post dictionary (from RealDictCursor).
        :return: A formatted, multi-line string.
        """
        lines = []
        separator = "-" * 80
        lines.append(separator)

        # Handle Retweet vs. Original Tweet
        if post.get('retweet_of_user_id'):
            lines.append(f"🔄 Retweeted by: @{post['author_username']} (ID: {post['author_id']})")
            lines.append(f"  Original Author: @{post.get('retweet_of_username', 'N/A')} (ID: {post['retweet_of_user_id']})")
        else:
            lines.append(f"👤 Tweet by: @{post['author_username']} (ID: {post['author_id']})")

        lines.append(f"  Date: {post['created_at']}")
        lines.append(f"  Tweet ID: {post['tweet_id']}")
        lines.append("\n")

        # Wrap text for readability
        wrapped_text = textwrap.fill(
            post['full_text'],
            width=76,  # 80 - 4 spaces for indent
            initial_indent="    ",
            subsequent_indent="    "
        )
        lines.append(wrapped_text)
        lines.append(separator)
        
        return "\n".join(lines)

    def get_user_posts_formatted(self, user_id):
       """
       Returns a list of formatted strings representing all posts
       authored by a given user.
       
       :param user_id: The ID of the author.
       :return: A list of formatted strings.
       """
       posts = self.get_user_posts(user_id)
       # Use the static helper method to format each post
       formatted_posts = [self._format_post(post) for post in posts]
       return formatted_posts

# --- Testing Function (Now includes tests for new methods) ---

def run_tests(conn_details):
    """
    Runs a suite of tests against the populated database.
    """
    print("\n--- Running Database Tests ---")
    tester = TwitterDBQuery(conn_details)
    test_results = {'passed': 0, 'failed': 0}

    # Known user ID for testing
    USER_ID_TO_CHECK = 818934188

    def run_a_test(name, test_func):
        """Helper to run a single test and record the result."""
        try:
            print(f"\n[RUNNING] {name}")
            test_func()
            print(f"[PASS] {name}")
            test_results['passed'] += 1
        except AssertionError as e:
            print(f"[FAIL] {name}\n   -> Assertion Error: {e}")
            test_results['failed'] += 1
        except Exception as e:
            print(f"[FAIL] {name}\n   -> An unexpected error occurred: {e}")
            test_results['failed'] += 1

    # --- Test 1: Check Specific User's Like Count ---
    def test_user_likes_count():
        liked_tweets = tester.get_user_likes(USER_ID_TO_CHECK)
        expected_likes = 3155
        print(f"   -> Found {len(liked_tweets)} likes for user {USER_ID_TO_CHECK}.")
        assert len(liked_tweets) == expected_likes, f"Expected {expected_likes} likes, but found {len(liked_tweets)}."
    run_a_test("Check Specific User's Like Count", test_user_likes_count)

    # --- Test 2: Verify a Known Follower ---
    def test_known_follower():
        username_to_check = 'DeSantisJet'
        known_follower_id = 1088163738950295553
        user = tester.get_user_by_username(username_to_check)
        assert user is not None, f"User '{username_to_check}' not found in the database."
        user_id = user['user_id']
        followers = tester.get_followers(user_id)
        assert known_follower_id in followers, f"Known follower {known_follower_id} not in the follower list."
    run_a_test("Verify a Known Follower", test_known_follower)

    # --- Test 3: Data Integrity - No Orphaned Likes ---
    def test_no_orphaned_likes():
        liked_tweets = tester.get_user_likes(USER_ID_TO_CHECK)
        assert len(liked_tweets) > 0, "Cannot perform test; user has no liked tweets."
        tweet = tester.get_tweet_by_id(liked_tweets[0])
        assert tweet is not None, f"Liked tweet_id {liked_tweets[0]} does not exist (orphaned record)."
    run_a_test("Data Integrity - No Orphaned Likes", test_no_orphaned_likes)
    
    # --- Test 4: Feed Reconstruction Logic ---
    def test_feed_reconstruction_logic():
        feed = tester.get_user_feed(USER_ID_TO_CHECK)
        assert feed is not None, "get_user_feed returned None."
        if len(feed) == 0:
            print("   -> [SKIP] Feed is empty. Cannot validate logic.")
            return
        followees = tester.get_followees(USER_ID_TO_CHECK)
        valid_author_ids = set(followees)
        valid_author_ids.add(USER_ID_TO_CHECK)
        for tweet in feed:
            assert tweet['author_id'] in valid_author_ids, "Feed contains tweet from non-followed user."
        print(f"   -> All {len(feed)} tweets in feed are from valid authors.")
    run_a_test("Feed Reconstruction Logic", test_feed_reconstruction_logic)

    # --- Test 5: get_user_posts (NEW) ---
    def test_get_user_posts():
        # Dynamically find an author known to have posts
        feed = tester.get_user_feed(USER_ID_TO_CHECK, limit=1)
        assert len(feed) > 0, "Cannot run test: initial user feed is empty (no authors found)."
        
        author_id_to_check = feed[0]['author_id']
        author_username = feed[0]['author_username']
        print(f"   -> Testing with known author from feed: @{author_username} ({author_id_to_check})")

        posts = tester.get_user_posts(author_id_to_check)
        assert posts is not None, "get_user_posts returned None."
        assert len(posts) > 0, "Expected user to have posts, but found none."
        for post in posts:
            assert post['author_id'] == author_id_to_check, "Found post with incorrect author_id."
        print(f"   -> Found {len(posts)} posts, all correctly authored by {author_id_to_check}.")
    run_a_test("Query: get_user_posts", test_get_user_posts)
    
    # --- Test 6: get_user_posts_formatted (NEW) ---
    def test_get_user_posts_formatted():
        # Dynamically find an author known to have posts
        feed = tester.get_user_feed(USER_ID_TO_CHECK, limit=1)
        assert len(feed) > 0, "Cannot run test: initial user feed is empty (no authors found)."

        author_id_to_check = feed[0]['author_id']
        author_username = feed[0]['author_username']
        print(f"   -> Testing with known author from feed: @{author_username} ({author_id_to_check})")
        
        formatted_posts = tester.get_user_posts_formatted(author_id_to_check)
        assert formatted_posts is not None, "get_user_posts_formatted returned None."
        assert len(formatted_posts) > 0, "Expected formatted posts, but list is empty."
        assert isinstance(formatted_posts[0], str), "List does not contain strings."
        assert f"@{author_username}" in formatted_posts[0], "Formatted string content is incorrect."
        print(f"   -> Successfully retrieved and formatted {len(formatted_posts)} posts.")
    run_a_test("Query: get_user_posts_formatted", test_get_user_posts_formatted)

    # --- Test 7: get_user_feed_until (NEW) ---
    def test_get_user_feed_until():
        # This date is known to be in the middle of the collected data
        end_date_str = "2024-07-22 00:00:00+00"
        end_date_obj = datetime.datetime.fromisoformat(end_date_str)
        
        feed = tester.get_user_feed_until(USER_ID_TO_CHECK, end_date_str)
        assert feed is not None, "get_user_feed_until returned None."
        assert len(feed) > 0, "Expected feed to have posts, but it's empty."
        
        for post in feed:
            assert post['created_at'] <= end_date_obj, f"Found post {post['tweet_id']} created after end date."
        print(f"   -> Found {len(feed)} posts, all created on or before {end_date_str}.")
    run_a_test("Query: get_user_feed_until", test_get_user_feed_until)

    # --- Summary ---
    print("\n--- Test Summary ---")
    print(f"Passed: {test_results['passed']}")
    print(f"Failed: {test_results['failed']}")
    print("--------------------")

if __name__ == "__main__":
    # This script is now only for testing.
    # To populate, run populate_database.py
    conn_details = get_db_connection_details()
    run_tests(conn_details)

