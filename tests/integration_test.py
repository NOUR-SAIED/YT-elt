import requests
import pytest
import psycopg2

def test_youtube_api_response(airflow_variable): # we can use the airflow_variable fixture to get the values of the environment variables that we have set up for our tests. This allows us to test the functionality of our code that relies on these variables without having to hardcode them in our test functions.
    api_key = airflow_variable("api_key")
    channel_handle = airflow_variable("channel_handle")
    
    url= f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={channel_handle}&key={api_key}"

    try: 
        response= requests.get(url)
        assert response.status_code == 200, f"Expected status code 200, but got {response.status_code}"
        
    except requests.RequestException as e:
        pytest.fail(f"API request failed, unable to connect to YouTube API:{e}")
        
        
        
def test_real_postgres_connection(real_postgres_connection):
    cursor = None

    try:
        cursor = real_postgres_connection.cursor()
        cursor.execute("SELECT 1;")
        result = cursor.fetchone()

        assert result[0] == 1

    except psycopg2.Error as e:
        pytest.fail(f"Database query failed: {e}")

    finally:
        if cursor is not None:
            cursor.close()