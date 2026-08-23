from flask import Flask, render_template
from kafka import KafkaProducer
import mysql.connector
import os
import json
import ssl

app = Flask(__name__)


# --- Helper: Kafka Producer Initialization ---
def get_kafka_producer():
    try:
        security_proto = os.environ.get("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
        ssl_ctx = ssl.create_default_context() if "SSL" in security_proto else None

        return KafkaProducer(
            bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS"),
            security_protocol=security_proto,
            sasl_mechanism=os.environ.get("KAFKA_SASL_MECHANISM", "PLAIN"),
            sasl_plain_username=os.environ.get("KAFKA_USERNAME", "avnadmin"),
            sasl_plain_password=os.environ.get("KAFKA_PASSWORD", ""),
            ssl_context=ssl_ctx,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
    except Exception as e:
        print(f"Error initializing Kafka Producer: {e}")
        return None


producer = get_kafka_producer()


# --- Helper: MySQL Connection Initialization ---
def get_db_connection():
    config = {
        "host": os.environ.get("MYSQL_HOST"),
        "port": int(os.environ.get("MYSQL_PORT", 14727)),
        "user": os.environ.get("MYSQL_USER", "avnadmin"),
        "password": os.environ.get("MYSQL_PASSWORD"),
        "database": os.environ.get("MYSQL_DATABASE", "medical_data"),
    }

    ssl_ca = os.environ.get("MYSQL_SSL_CA")
    if ssl_ca and os.path.exists(ssl_ca):
        config["ssl_ca"] = ssl_ca
    else:
        config["ssl_disabled"] = False

    return mysql.connector.connect(**config)


# --- Routes ---

@app.route('/')
def homes():
    return render_template('home.html')


@app.route('/sig')
def signup():
    return render_template('signin.html')


# Send message to Kafka and save to MySQL
@app.route('/send/<msg>')
def send_message(msg):
    global producer
    if not producer:
        producer = get_kafka_producer()

    # Produce to Kafka
    if producer:
        producer.send('lakhan-medical', value={"message": msg})
        producer.flush()

    # Insert into MySQL table
    db = get_db_connection()
    cursor = db.cursor()
    cursor.execute("INSERT INTO messages (content) VALUES (%s)", (msg,))
    db.commit()
    cursor.close()
    db.close()

    return f"Message sent to Kafka and saved in MySQL: {msg}"


# Show medicines from MySQL
@app.route('/medicines')
def show_medicines():
    db = get_db_connection()
    cursor = db.cursor(dictionary=True)
    cursor.execute("SELECT * FROM medicines;")
    rows = cursor.fetchall()
    cursor.close()
    db.close()
    return render_template('medicines.html', medicines=rows)


# Update a record in MySQL
@app.route('/update/<int:id>/<new_msg>')
def update_message(id, new_msg):
    db = get_db_connection()
    cursor = db.cursor()
    cursor.execute("UPDATE messages SET content=%s WHERE id=%s", (new_msg, id))
    db.commit()
    cursor.close()
    db.close()
    return f"Updated message {id} to: {new_msg}"


# --- Run App ---
if __name__ == '__main__':
    app.run(debug=True, port=5001, host='0.0.0.0')