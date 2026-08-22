from flask import Flask, render_template
from kafka import KafkaProducer
import os
import json

app = Flask(__name__)

# Aiven Kafka connection
producer = KafkaProducer(
    bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"],
    security_protocol=os.environ["KAFKA_SECURITY_PROTOCOL"],
    sasl_mechanism=os.environ["KAFKA_SASL_MECHANISM"],
    sasl_plain_username=os.environ["KAFKA_USERNAME"],
    sasl_plain_password=os.environ["KAFKA_PASSWORD"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

@app.route('/')
def homes():
    return render_template('home.html')

@app.route('/sig')
def signup():
    return render_template('signin.html')

if __name__ == '__main__':
    app.run(debug=True, port=5001, host='0.0.0.0')
