import sqlite3
import sys

try:
    from query_database import TwitterDBQuery, get_db_path
except ImportError:
    print("Error: Could not find 'query_database.py'.")
    print("Make sure this test script is in the same directory.")
    sys.exit(1)

def test_connection(db_path):
    """
    Test 1: Check if a connection to the database can be established.
    """
    print("[RUNNING] Test 1: Database Connection")
    try:
        conn = sqlite3.connect(db_path)
        conn.close()
        print("[PASS] Successfully connected to the database.")
        return True
    except sqlite3.Error as e:
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
    """
    print("\n[RUNNING] Test 3: Core Query Logic")
    
    # Test 3.1: get_user_likes
    test_name = "Query: get_user_likes"
    try:
        user_id_to_check = 818934188
        liked_tweets = tester.get_user_likes(user_id_to_check)
        expected_likes = 3155
        
        assert len(liked_tweets) == expected_likes
        print(f"   -> [PASS] {test_name} (Found {len(liked_tweets)} likes)")
    except AssertionError:
        print(f"   -> [FAIL] {test_name}: Expected {expected_likes} likes, found {len(liked_tweets)}")
        failed_tests.append(test_name)
    except Exception as e:
        print(f"   -> [FAIL] {test_name}: Error {e}")
        failed_tests.append(test_name)

    # Test 3.2: get_user_feed
    test_name = "Query: get_user_feed"
    try:
        user_id_to_check = 818934188
        feed = tester.get_user_feed(user_id_to_check, limit=50)
        
        if not feed:
            print(f"   -> [SKIP] {test_name}: Feed is empty. Cannot validate logic.")
            return

        followees = tester.get_followees(user_id_to_check)
        valid_authors = set(followees)
        valid_authors.add(user_id_to_check)

        for tweet in feed:
            assert tweet['author_id'] in valid_authors, \
                f"Tweet {tweet['tweet_id']} by invalid author {tweet['author_id']}"
        
        print(f"   -> [PASS] {test_name} (All {len(feed)} tweets from valid authors)")
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
    try:
        db_path = get_db_path()
    except EOFError:
        print("\nTest run cancelled.")
        sys.exit(0)
        
    print("\n--- Starting Database Test Suite ---")
    
    if not test_connection(db_path):
        print("\nConnection test failed. Aborting further tests.")
        sys.exit(1)
    
    tester = TwitterDBQuery(db_path)
    failed_tests = []
    
    test_population(tester, failed_tests)
    test_queries(tester, failed_tests)
    
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
