from flask import Flask, render_template
from kafka import KafkaProducer
import mysql.connector
import os
import json

app = Flask(__name__)

# --- Kafka Connection ---
producer = KafkaProducer(
    bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"],
    security_protocol=os.environ["KAFKA_SECURITY_PROTOCOL"],
    sasl_mechanism=os.environ["KAFKA_SASL_MECHANISM"],
    sasl_plain_username=os.environ["KAFKA_USERNAME"],
    sasl_plain_password=os.environ["KAFKA_PASSWORD"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# --- MySQL Connection ---
db = mysql.connector.connect(
    host=os.environ["MYSQL_HOST"],
    port=os.environ["MYSQL_PORT"],
    user=os.environ["MYSQL_USER"],
    password=os.environ["MYSQL_PASSWORD"],
    ssl_ca=os.environ["MYSQL_SSL_CA"]
)
cursor = db.cursor()

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
    # Produce to Kafka
    producer.send('lakhan-medical', value={"message": msg})
    producer.flush()

    # Insert into MySQL table
    cursor.execute("INSERT INTO messages (content) VALUES (%s)", (msg,))
    db.commit()

    return f"Message sent to Kafka and saved in MySQL: {msg}"
@app.route('/medicines')
def show_medicines():
    cursor.execute("SELECT * FROM medicines;")
    rows = cursor.fetchall()
    return render_template('medicines.html', medicines=rows)
    

# Show all messages from MySQL
@app.route('/messages')
def show_messages():
    cursor.execute("SELECT * FROM messages;")
    rows = cursor.fetchall()
    return {"messages": rows}

# Update a record in MySQL
@app.route('/update/<int:id>/<new_msg>')
def update_message(id, new_msg):
    cursor.execute("UPDATE messages SET content=%s WHERE id=%s", (new_msg, id))
    db.commit()
    return f"Updated message {id} to: {new_msg}"

# --- Run App ---
if __name__ == '__main__':
    app.run(debug=True, port=5001, host='0.0.0.0')
