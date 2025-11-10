import sqlite3
from contextlib import contextmanager
import textwrap
import datetime

def get_db_path():
    """Returns the path to the SQLite database file."""
    default_path = "LSS_twon.db"
    user_input = input(f"Database file path (default: {default_path}): ").strip()
    return user_input if user_input else default_path

def dict_factory(cursor, row):
    """Convert sqlite3.Row to dict for compatibility with PostgreSQL RealDictCursor."""
    return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}

class TwitterDBQuery:
    """
    A class to handle querying the Twitter relational database.
    """
    def __init__(self, db_path):
        self.db_path = db_path

    @contextmanager
    def _get_cursor(self):
        """A context manager for database connections and cursors."""
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = dict_factory
            cur = conn.cursor()
            yield cur
        except sqlite3.Error as e:
            print(f"Database error: {e}")
            raise
        finally:
            if conn:
                conn.close()

    def get_user_likes(self, user_id):
        """Gets a list of all tweet IDs liked by a specific user."""
        query = "SELECT tweet_id FROM Likes WHERE user_id = ?;"
        with self._get_cursor() as cur:
            cur.execute(query, (user_id,))
            return [row['tweet_id'] for row in cur.fetchall()]

    def get_user_by_username(self, username):
        """Gets a user's data by their username."""
        query = "SELECT user_id FROM Users WHERE username = ? LIMIT 1;"
        with self._get_cursor() as cur:
            cur.execute(query, (username,))
            return cur.fetchone()

    def get_followers(self, user_id):
        """Gets a list of user_ids for a given user's followers."""
        query = "SELECT follower_id FROM Follows WHERE followee_id = ?;"
        with self._get_cursor() as cur:
            cur.execute(query, (user_id,))
            return [row['follower_id'] for row in cur.fetchall()]

    def get_tweet_by_id(self, tweet_id):
        """Gets a single tweet by its ID."""
        query = "SELECT * FROM Tweets WHERE tweet_id = ?;"
        with self._get_cursor() as cur:
            cur.execute(query, (tweet_id,))
            return cur.fetchone()

    def get_followees(self, user_id):
        """Gets a list of user_ids for a given user's followees."""
        query = "SELECT followee_id FROM Follows WHERE follower_id = ?;"
        with self._get_cursor() as cur:
            cur.execute(query, (user_id,))
            return [row['followee_id'] for row in cur.fetchall()]

    def get_user_feed(self, user_id, limit=100):
        """
        Reconstructs a user's feed, composed of tweets from their followees
        and their own tweets, in reverse chronological order.
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
                SELECT followee_id FROM Follows WHERE follower_id = ?
                UNION
                SELECT ?
            )
        ORDER BY
            t.created_at DESC
        LIMIT ?;
        """
        with self._get_cursor() as cur:
            cur.execute(query, (user_id, user_id, limit))
            return cur.fetchall()

    def get_user_feed_until(self, user_id, enddate, limit=100):
        """
        Reconstructs a user's feed up to a specific date.
        
        :param user_id: The ID of the user whose feed to reconstruct.
        :param enddate: A date or timestamp. Only tweets created at or before this time.
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
                SELECT followee_id FROM Follows WHERE follower_id = ?
                UNION
                SELECT ?
            )
        AND
            t.created_at <= ?
        ORDER BY
            t.created_at DESC
        LIMIT ?;
        """
        with self._get_cursor() as cur:
            cur.execute(query, (user_id, user_id, enddate, limit))
            return cur.fetchall()
        
    def get_user_feed_until_between(self, user_id, enddate, startdate=None, limit=100):
        """
        Reconstructs a user's feed composed of tweets from their followees
        and their own tweets, created between a start and end date.
        If startdate is not provided, all tweets up to enddate are included.

        :param user_id: The ID of the user whose feed to reconstruct.
        :param enddate: The latest creation timestamp to include.
        :param startdate: The earliest creation timestamp to include (optional).
        :param limit: The maximum number of tweets to return.
        :return: A list of post dictionaries.
        """
        # Base query (same as before, but condition depends on startdate)
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
                SELECT followee_id
                FROM Follows
                WHERE follower_id = %s

                UNION

                SELECT %s
            )
        """

        # Apply date filtering logic dynamically
        params = [user_id, user_id]

        if startdate:
            query += " AND t.created_at BETWEEN %s AND %s"
            params.extend([startdate, enddate])
        else:
            query += " AND t.created_at <= %s"
            params.append(enddate)

        query += """
        ORDER BY
            t.created_at DESC
        LIMIT %s;
        """
        params.append(limit)

        with self._get_cursor() as cur:
            cur.execute(query, tuple(params))
            return cur.fetchall()


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
            t.author_id = ?
        ORDER BY
            t.created_at DESC;
        """
        with self._get_cursor() as cur:
            cur.execute(query, (user_id,))
            return cur.fetchall()

    def get_all_users(self):
        """Return a list of all users in the database."""
        query = "SELECT user_id, username FROM Users ORDER BY username;"
        with self._get_cursor() as cur:
            cur.execute(query)
            return cur.fetchall()

    def get_all_posts(self):
        """Return every post with its author information."""
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
        ORDER BY
            t.created_at ASC;
        """
        with self._get_cursor() as cur:
            cur.execute(query)
            return cur.fetchall()

    @staticmethod
    def _format_post(post):
        """
        Formats a single post dictionary into a human-readable string.
        """
        lines = []
        separator = "-" * 80
        lines.append(separator)

        if post.get('retweet_of_user_id'):
            lines.append(f"🔄 Retweeted by: @{post['author_username']} (ID: {post['author_id']})")
            lines.append(f"  Original Author: @{post.get('retweet_of_username', 'N/A')} (ID: {post['retweet_of_user_id']})")
        else:
            lines.append(f"👤 Tweet by: @{post['author_username']} (ID: {post['author_id']})")

        lines.append(f"  Date: {post['created_at']}")
        lines.append(f"  Tweet ID: {post['tweet_id']}")
        lines.append("\n")

        wrapped_text = textwrap.fill(
            post['full_text'],
            width=76,
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
        """
        posts = self.get_user_posts(user_id)
        return [self._format_post(post) for post in posts]

def run_tests(db_path):
    """
    Runs a suite of tests against the populated database.
    """
    print("\n--- Running Database Tests ---")
    tester = TwitterDBQuery(db_path)
    test_results = {'passed': 0, 'failed': 0}

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
            print(f"[FAIL] {name}\n   -> Unexpected error: {e}")
            test_results['failed'] += 1

    def test_user_likes_count():
        liked_tweets = tester.get_user_likes(USER_ID_TO_CHECK)
        expected_likes = 3155
        print(f"   -> Found {len(liked_tweets)} likes for user {USER_ID_TO_CHECK}.")
        assert len(liked_tweets) == expected_likes, f"Expected {expected_likes} likes, found {len(liked_tweets)}."
    run_a_test("Check Specific User's Like Count", test_user_likes_count)

    def test_known_follower():
        username_to_check = 'DeSantisJet'
        known_follower_id = 1088163738950295553
        user = tester.get_user_by_username(username_to_check)
        assert user is not None, f"User '{username_to_check}' not found."
        user_id = user['user_id']
        followers = tester.get_followers(user_id)
        assert known_follower_id in followers, f"Known follower {known_follower_id} not in list."
    run_a_test("Verify a Known Follower", test_known_follower)

    def test_no_orphaned_likes():
        liked_tweets = tester.get_user_likes(USER_ID_TO_CHECK)
        assert len(liked_tweets) > 0, "User has no liked tweets."
        tweet = tester.get_tweet_by_id(liked_tweets[0])
        assert tweet is not None, f"Liked tweet_id {liked_tweets[0]} does not exist."
    run_a_test("Data Integrity - No Orphaned Likes", test_no_orphaned_likes)
    
    def test_feed_reconstruction_logic():
        feed = tester.get_user_feed(USER_ID_TO_CHECK)
        assert feed is not None, "get_user_feed returned None."
        if len(feed) == 0:
            print("   -> [SKIP] Feed is empty.")
            return
        followees = tester.get_followees(USER_ID_TO_CHECK)
        valid_author_ids = set(followees)
        valid_author_ids.add(USER_ID_TO_CHECK)
        for tweet in feed:
            assert tweet['author_id'] in valid_author_ids, "Feed contains tweet from non-followed user."
        print(f"   -> All {len(feed)} tweets are from valid authors.")
    run_a_test("Feed Reconstruction Logic", test_feed_reconstruction_logic)

    def test_get_user_posts():
        feed = tester.get_user_feed(USER_ID_TO_CHECK, limit=1)
        assert len(feed) > 0, "Initial user feed is empty."
        
        author_id_to_check = feed[0]['author_id']
        author_username = feed[0]['author_username']
        print(f"   -> Testing with: @{author_username} ({author_id_to_check})")

        posts = tester.get_user_posts(author_id_to_check)
        assert posts is not None, "get_user_posts returned None."
        assert len(posts) > 0, "Expected user to have posts."
        for post in posts:
            assert post['author_id'] == author_id_to_check, "Incorrect author_id."
        print(f"   -> Found {len(posts)} posts.")
    run_a_test("Query: get_user_posts", test_get_user_posts)
    
    def test_get_user_posts_formatted():
        feed = tester.get_user_feed(USER_ID_TO_CHECK, limit=1)
        assert len(feed) > 0, "Initial user feed is empty."

        author_id_to_check = feed[0]['author_id']
        author_username = feed[0]['author_username']
        print(f"   -> Testing with: @{author_username} ({author_id_to_check})")
        
        formatted_posts = tester.get_user_posts_formatted(author_id_to_check)
        assert formatted_posts is not None, "get_user_posts_formatted returned None."
        assert len(formatted_posts) > 0, "Formatted posts list is empty."
        assert isinstance(formatted_posts[0], str), "List does not contain strings."
        assert f"@{author_username}" in formatted_posts[0], "Formatted string incorrect."
        print(f"   -> Successfully formatted {len(formatted_posts)} posts.")
    run_a_test("Query: get_user_posts_formatted", test_get_user_posts_formatted)

    def test_get_user_feed_until():
        end_date_str = "2024-07-22T00:00:00+00:00"
        
        feed = tester.get_user_feed_until(USER_ID_TO_CHECK, end_date_str)
        assert feed is not None, "get_user_feed_until returned None."
        assert len(feed) > 0, "Feed is empty."
        
        for post in feed:
            assert post['created_at'] <= end_date_str, f"Post {post['tweet_id']} created after end date."
        print(f"   -> Found {len(feed)} posts before {end_date_str}.")
    run_a_test("Query: get_user_feed_until", test_get_user_feed_until)

    print("\n--- Test Summary ---")
    print(f"Passed: {test_results['passed']}")
    print(f"Failed: {test_results['failed']}")
    print("--------------------")

if __name__ == "__main__":
    db_path = get_db_path()
    run_tests(db_path)
