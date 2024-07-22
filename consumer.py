from kafka import KafkaConsumer
import psycopg2
import json

# Database connection parameters
DB_PARAMS = {
    'dbname': 'employees_db2',
    'user': 'postgres',
    'password': 'password',
    'host': 'localhost',
    'port': '5433'
}

# Kafka parameters
KAFKA_BROKER = 'localhost:29092'
TOPIC = 'employee_changes'

def process_message(message):
    """Process and apply the message to the target database."""
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()
    data = json.loads(message.value.decode('utf-8'))
    action = data['action']
    emp_id = data['emp_id']

    try:
        if action == 'INSERT':
            cur.execute("""
                INSERT INTO employees (emp_id, first_name, last_name, dob, city)
                VALUES (%s, %s, %s, %s, %s)
            """, (emp_id, data['first_name'], data['last_name'], data['dob'], data['city']))
        elif action == 'UPDATE':
            cur.execute("""
                UPDATE employees
                SET first_name = %s, last_name = %s, dob = %s, city = %s
                WHERE emp_id = %s
            """, (data['first_name'], data['last_name'], data['dob'], data['city'], emp_id))
        elif action == 'DELETE':
            cur.execute("DELETE FROM employees WHERE emp_id = %s", (emp_id,))
        conn.commit()
    except psycopg2.errors.UniqueViolation:
        conn.rollback()
        print(f"UniqueViolation: Duplicate key value for emp_id={emp_id}. Ignoring INSERT.")
    except Exception as e:
        conn.rollback()
        print(f"Error: {e}")
    finally:
        cur.close()
        conn.close()

def main():
    consumer = KafkaConsumer(TOPIC, bootstrap_servers=[KAFKA_BROKER])
    for message in consumer:
        process_message(message)

if __name__ == "__main__":
    main()
