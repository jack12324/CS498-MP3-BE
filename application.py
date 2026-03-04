from flask import Flask, jsonify, request
import os
import pymysql
from pymysql.err import OperationalError
import logging
from flask_cors import CORS
import datetime

application = Flask(__name__)
CORS(application)
logging.basicConfig(level=logging.INFO)

#Endpoint: Health Check
@application.route('/health', methods=['GET'])
def health():
    """
    This endpoint is used by the autograder to confirm that the backend deployment is healthy.
    """
    return jsonify({"status": "healthy"}), 200

#Endpoint: Data Insertion
@application.route('/events', methods=['POST'])
def create_event():
    """
    This endpoint should eventually insert data into the database.
    The database communication is currently stubbed out.
    You must implement insert_data_into_db() function to integrate with your MySQL RDS Instance.
    """
    try:
        payload = request.get_json()
        required_fields = ["title", "date"]
        if not payload or not all(field in payload for field in required_fields):
            return jsonify({"error": "Missing required fields: 'title' and 'date'"}), 400

        insert_data_into_db(payload)
        return jsonify({"message": "Event created successfully"}), 201
    except NotImplementedError as nie:
        return jsonify({"error": str(nie)}), 501
    except Exception as e:
        logging.exception("Error occurred during event creation")
        return jsonify({
            "error": "During event creation",
            "detail": str(e)
        }), 500

#Endpoint: Data Retrieval
@application.route('/data', methods=['GET'])
def get_data():
    """
    This endpoint should eventually provide data from the database.
    The database communication is currently stubbed out.
    You must implement the fetch_data_from_db() function to integrate with your MySQL RDS Instance.
    """
    try:
        data = fetch_data_from_db()
        # The autograder expects a JSON object with a top-level "data" key.
        # Shape: { "data": [ { ...event }, ... ] }
        return jsonify({"data": data}), 200
    except NotImplementedError as nie:
        return jsonify({"error": str(nie)}), 501
    except Exception as e:
        logging.exception("Error occurred during data retrieval")
        return jsonify({
            "error": "During data retrieval",
            "detail": str(e)
        }), 500

def get_db_connection():
    """
    Establish and return a connection to the RDS MySQL database.
    The following variables should be added to the Elastic Beanstalk Environment Properties for better security. Follow guidelines for more info.
      - DB_HOST
      - DB_USER
      - DB_PASSWORD
      - DB_NAME
    """
    required_vars = ["DB_HOST", "DB_USER", "DB_PASSWORD", "DB_NAME"]
    missing = [var for var in required_vars if not os.environ.get(var)]
    if missing:
        msg = f"Missing environment variables: {', '.join(missing)}"
        logging.error(msg)
        raise EnvironmentError(msg)
    try:
        connection = pymysql.connect(
            host=os.environ.get("DB_HOST"),
            user=os.environ.get("DB_USER"),
            password=os.environ.get("DB_PASSWORD"),
            db=os.environ.get("DB_NAME")
        )
        return connection
    except OperationalError as e:
        raise ConnectionError(f"Failed to connect to the database: {e}")

def create_db_table():
    connection = get_db_connection()
    try:
        with get_db_connection() as connection:
            with connection.cursor() as cursor:
                create_table_sql = """
                CREATE TABLE IF NOT EXISTS events (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    title VARCHAR(255) NOT NULL,
                    description TEXT,
                    image_url VARCHAR(255),
                    date DATE NOT NULL,
                    location VARCHAR(255)
                )
                """
                cursor.execute(create_table_sql)
            connection.commit()
            logging.info("Events table created or already exists")
    except Exception as e:
        logging.exception("Failed to create or verify the events table")
        raise RuntimeError(f"Table creation failed: {str(e)}")

def insert_data_into_db(payload):
    """
    Insert an event into the `events` table using PyMySQL.
    Expects a payload containing at least `title` and `date`, and may include
    `description`, `image_url`, and `location`.
    """
    # Ensure the table exists before attempting to insert
    create_db_table()

    title = payload["title"]
    date = payload["date"]
    description = payload.get("description")
    image_url = payload.get("image_url")
    location = payload.get("location")

    insert_sql = """
        INSERT INTO events (title, description, image_url, date, location)
        VALUES (%s, %s, %s, %s, %s)
    """

    # Use a context manager so the connection is always closed properly
    with get_db_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                insert_sql,
                (title, description, image_url, date, location)
            )
        connection.commit()

#Database Function Stub
def fetch_data_from_db():
    """
    Fetch all rows from the `events` table and return them as a list of dicts,
    ordered by `date` in ascending order.
    """
    # Ensure the table exists before querying it
    create_db_table()

    select_sql = """
        SELECT id, title, description, image_url, date, location
        FROM events
        ORDER BY date ASC
    """

    with get_db_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(select_sql)
            rows = cursor.fetchall()

    columns = ["id", "title", "description", "image_url", "date", "location"]
    results = []
    for row in rows:
        record = dict(zip(columns, row))
        # Convert date objects to RFC 1123–style strings expected by the frontend,
        # e.g. "Mon, 25 Aug 2025 00:00:00 GMT"
        date_value = record.get("date")
        if hasattr(date_value, "strftime"):
            # If it's a date (no time), combine with midnight; if it's already
            # a datetime, use it directly.
            if isinstance(date_value, datetime.date) and not isinstance(
                date_value, datetime.datetime
            ):
                dt = datetime.datetime.combine(date_value, datetime.time())
            else:
                dt = date_value
            record["date"] = dt.strftime("%a, %d %b %Y %H:%M:%S GMT")
        # Do not expose the internal auto-incrementing ID in the API response
        record.pop("id", None)
        results.append(record)

    return results

if __name__ == '__main__':
    application.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
