from flask import Flask, render_template, request, redirect, url_for, flash
from kafka import KafkaProducer
import mysql.connector
import os
import json
import ssl

app = Flask(__name__)
# Added a secret key to support secure browser flash messaging
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "super_secret_medical_key")


# --- Helper: Kafka Producer Initialization ---
def get_kafka_producer():
    try:
        bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
        if not bootstrap_servers:
            print("Kafka configuration missing. Producer skipped.")
            return None

        security_proto = os.environ.get("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
        ssl_ctx = ssl.create_default_context() if "SSL" in security_proto else None

        return KafkaProducer(
            bootstrap_servers=bootstrap_servers,
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


# Global variable placeholder (Initialized lazily during actual form posts)
producer = None


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


# Unified Signup Route: Handles viewing the form (GET) and submitting data (POST)
@app.route('/signup', methods=['GET', 'POST'])
def handle_signup():
    global producer

    # 1. Handle GET Request (Viewing the page)
    if request.method == 'GET':
        try:
            # Matches the exact 'signin.html' file visible in your PyCharm templates folder
            return render_template('signin.html')
        except Exception as e:
            print(f"Template rendering crash: {e}")
            return f"Template Error: Make sure 'signin.html' exists in your 'templates/' folder. Details: {e}", 500

    # 2. Handle POST Request (Form submission)
    # Captured exactly via the lowercase HTML field input 'name' keys
    form_data = {
        "full_name": request.form.get("fullName", ""),
        "medical_name": request.form.get("medicalName", ""),
        "pan_number": request.form.get("panNumber", ""),
        "license_number": request.form.get("licenseNumber", ""),  # Fixed case sensitivity bug
        "address": request.form.get("address", ""),
        "dob": request.form.get("dob", "")
    }

    # 3. Stream registration event logs into Apache Kafka safely
    if not producer:
        producer = get_kafka_producer()
    if producer:
        try:
            producer.send('lakhan-medical', value={"event": "user_signup", "data": form_data})
            producer.flush()
        except Exception as kafka_err:
            print(f"Kafka logging failed: {kafka_err}")

    # 4. Insert data records securely into your Aiven MySQL database table
    try:
        db = get_db_connection()
        cursor = db.cursor()

        sql = """INSERT INTO medical_users (full_name, medical_name, pan_number, license_number, address, dob) 
                 VALUES (%s, %s, %s, %s, %s, %s)"""
        values = (
            form_data["full_name"],
            form_data["medical_name"],
            form_data["pan_number"],
            form_data["license_number"],
            form_data["address"],
            form_data["dob"]
        )

        cursor.execute(sql, values)
        db.commit()
        cursor.close()
        db.close()

        return "Sign-Up Complete! Your data has been successfully broadcast to Kafka and saved to MySQL."

    except Exception as db_err:
        print(f"MySQL Error: {db_err}")
        return f"Database error encountered: {db_err}", 500


# Legacy tracking test endpoint preserved
@app.route('/send/<msg>')
def send_message(msg):
    global producer
    if not producer:
        producer = get_kafka_producer()

    if producer:
        producer.send('lakhan-medical', value={"message": msg})
        producer.flush()

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
