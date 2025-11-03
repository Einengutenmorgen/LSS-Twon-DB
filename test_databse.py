import psycopg2
import sys
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
from datetime import datetime


# --- Import Core Logic ---
# We import the query class from your existing script to avoid duplicating code.
try:
    from query_database import TwitterDBQuery, get_db_connection_details
except ImportError:
    print("Error: Could not find 'query_database.py'.")
    print("Make sure this test script is in the same directory.")
    sys.exit(1)

def test_connection(conn_details):
    """
    Test 1: Check if a connection to the database can be established.
    This is a prerequisite for all other tests.
    """
    print("[RUNNING] Test 1: Database Connection")
    try:
        conn = psycopg2.connect(**conn_details)
        conn.close()
        print("[PASS] Successfully connected to the database.")
        return True
    except psycopg2.Error as e:
        print(f"[FAIL] Could not connect to the database.\n   -> Error: {e}")
        return False

def test_population(tester, failed_tests):
    """
    Test 2: Check if all tables are populated (i.e., not empty).
    """
    print("\n[RUNNING] Test 2: Data Population Checks")
    tables_to_check = ["Users", "Tweets", "Follows", "Likes"]
    all_populated = True
    
    try:
        with tester._get_cursor() as cur:
            for table in tables_to_check:
                # Check for at least one row in the table
                cur.execute(f"SELECT 1 FROM {table} LIMIT 1;")
                if cur.fetchone():
                    print(f"   -> [PASS] '{table}' table is populated.")
                else:
                    print(f"   -> [FAIL] '{table}' table is empty.")
                    all_populated = False
    except Exception as e:
        print(f"[FAIL] Error during population check: {e}")
        failed_tests.append("Database Population")
        return

    if not all_populated:
        failed_tests.append("Database Population (One or more tables are empty)")

def test_queries(tester, failed_tests):
    """
    Test 3: Run integration tests against key queries using known data.
    These tests assume the data from the CSVs has been populated.
    """
    print("\n[RUNNING] Test 3: Core Query Logic")
    
    # --- Test 3.1: get_user_likes ---
    # This test uses known values from your original test case.
    test_name = "Query: get_user_likes"
    try:
        user_id_to_check = 818934188
        liked_tweets = tester.get_user_likes(user_id_to_check)
        expected_likes = 3155  # Based on your original test data
        
        assert len(liked_tweets) == expected_likes
        print(f"   -> [PASS] {test_name} (Found {len(liked_tweets)} likes for user {user_id_to_check})")
    except AssertionError:
        print(f"   -> [FAIL] {test_name}: Expected {expected_likes} likes, found {len(liked_tweets)}")
        failed_tests.append(test_name)
    except Exception as e:
        print(f"   -> [FAIL] {test_name}: Error {e}")
        failed_tests.append(test_name)

    # --- Test 3.2: get_user_feed ---
    # This test validates the feed reconstruction logic.
    test_name = "Query: get_user_feed"
    try:
        user_id_to_check = 818934188
        feed = tester.get_user_feed(user_id_to_check, limit=50)
        
        if not feed:
            # This isn't a failure, but we can't test the logic.
            # It likely means this user/followees have no *collected* tweets.
            print(f"   -> [SKIP] {test_name}: Feed for user {user_id_to_check} is empty. Cannot validate logic.")
            return

        # Get all valid authors (the user + everyone they follow)
        followees = tester.get_followees(user_id_to_check)
        valid_authors = set(followees)
        valid_authors.add(user_id_to_check)

        # Check every tweet in the feed
        for tweet in feed:
            assert tweet['author_id'] in valid_authors, \
                f"Tweet {tweet['tweet_id']} by invalid author {tweet['author_id']}"
        
        print(f"   -> [PASS] {test_name} (All {len(feed)} tweets in feed are from valid authors)")
    except AssertionError as e:
        print(f"   -> [FAIL] {test_name}: {e}")
        failed_tests.append(test_name)
    except Exception as e:
        print(f"   -> [FAIL] {test_name}: Error {e}")
        failed_tests.append(test_name)

    # --- Test 3.3: get_user_feed_until_between ---
    test_name = "Query: get_user_feed_until_between"
    try:
        user_id_to_check = 818934188
        startdate_str = "2020-01-01T00:00:00Z"
        enddate_str = "2020-12-31T23:59:59Z"

        # Convert to datetime for proper comparison
        startdate = datetime.fromisoformat(startdate_str.replace("Z", "+00:00"))
        enddate = datetime.fromisoformat(enddate_str.replace("Z", "+00:00"))

        feed_between = tester.get_user_feed_until_between(
            user_id=user_id_to_check,
            startdate=startdate,
            enddate=enddate,
            limit=50
        )

        if not feed_between:
            print(f"   -> [SKIP] {test_name}: No tweets found between {startdate_str} and {enddate_str}.")
            return

        # Ensure all tweets are within range
        for tweet in feed_between:
            created = tweet["created_at"]
            assert startdate <= created <= enddate, \
                f"Tweet {tweet['tweet_id']} created_at={created} outside range."

        print(f"   -> [PASS] {test_name} (All {len(feed_between)} tweets within {startdate_str}–{enddate_str})")

        # Test without startdate
        feed_until = tester.get_user_feed_until_between(
            user_id=user_id_to_check,
            enddate=enddate,
            limit=20
        )

        if feed_until:
            for tweet in feed_until:
                created = tweet["created_at"]
                assert created <= enddate, \
                    f"Tweet {tweet['tweet_id']} created_at={created} after enddate={enddate_str}"
            print(f"   -> [PASS] {test_name} (Optional startdate None handled correctly, {len(feed_until)} tweets)")

    except AssertionError as e:
        print(f"   -> [FAIL] {test_name}: {e}")
        failed_tests.append(test_name)
    except Exception as e:
        print(f"   -> [FAIL] {test_name}: Error {e}")
        failed_tests.append(test_name)

def main():
    """
    Main function to run the test suite.
    """
    # Get connection details
    try:
        conn_details = get_db_connection_details()
    except EOFError:
        print("\nTest run cancelled.")
        sys.exit(0)
        
    print("\n--- Starting Database Test Suite ---")
    
    # --- Test 1: Connection (Prerequisite) ---
    if not test_connection(conn_details):
        print("\nConnection test failed. Aborting further tests.")
        sys.exit(1)
    
    # If connection is OK, initialize the query object and proceed
    tester = TwitterDBQuery(conn_details)
    failed_tests = []
    
    # --- Test 2: Population ---
    test_population(tester, failed_tests)
    
    # --- Test 3: Queries ---
    test_queries(tester, failed_tests)
    
    # --- Summary ---
    print("\n--- Test Summary ---")
    if not failed_tests:
        print("✅ All tests passed!")
    else:
        print(f"❌ Failed {len(failed_tests)} test(s):")
        for test in failed_tests:
            print(f"   - {test}")
        sys.exit(1)

if __name__ == "__main__":
    main()
