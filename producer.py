from kafka import KafkaProducer
import psycopg2
import json
import time
import six
import sys
if sys.version_info >= (3, 12, 0):
    sys.modules['kafka.vendor.six.moves'] = six.moves

# Database connection parameters
DB_PARAMS = {
    'dbname': 'employees_db1',
    'user': 'postgres',
    'password': 'password',
    'host': 'localhost',
    'port': '5432'
}

# Kafka parameters
KAFKA_BROKER = 'localhost:29092'
TOPIC = 'employee_changes'

def get_cdc_data():
    """Function to fetch data from the CDC table."""
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()
    cur.execute("SELECT * FROM employees_cdc")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

def send_to_kafka(producer, data):
    """Function to send data to Kafka."""
    for row in data:
        message = {
            'emp_id': row[0],
            'first_name': row[1],
            'last_name': row[2],
            'dob': row[3].strftime('%Y-%m-%d'),
            'city': row[4],
            'action': row[5]
        }
        producer.send(TOPIC, value=json.dumps(message).encode('utf-8'))

def main():
    producer = KafkaProducer(bootstrap_servers=[KAFKA_BROKER])
    while True:
        data = get_cdc_data()
        if data:
            send_to_kafka(producer, data)
        time.sleep(1)  # Polling interval

if __name__ == "__main__":
    main()
