from confluent_kafka import Consumer, KafkaException
import psycopg2
import json

# Kafka configuration
kafka_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'email-consumer-group',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(kafka_config)
topic = 'emails'
consumer.subscribe([topic])

# Connect to the PostgreSQL server
conn = psycopg2.connect(
    dbname="ainbox",
    user="postgres",
    password="Habang3233",
    host="localhost",
    port="5432"
)

cur = conn.cursor()

# Function to insert email into PostgreSQL
def insert_email(email_data):
    insert_query = """
    INSERT INTO emails (email_id, subject, sender, recipients, cc, bcc, date, body_plain, links)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (id, email_id) DO UPDATE SET
        subject = EXCLUDED.subject,
        sender = EXCLUDED.sender,
        recipients = EXCLUDED.recipients,
        cc = EXCLUDED.cc,
        bcc = EXCLUDED.bcc,
        date = EXCLUDED.date,
        body_plain = EXCLUDED.body_plain,
        links = EXCLUDED.links;
    """
    cur.execute(insert_query, (
        email_data['email_id'],
        email_data['subject'],
        email_data['sender'],
        email_data['recipients'],
        email_data['cc'],
        email_data['bcc'],
        email_data['date'],
        email_data['body_plain'],
        email_data['links']
    ))
    conn.commit()

# Main loop to consume emails from Kafka and store them in PostgreSQL
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaException._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break

        email_data = json.loads(msg.value().decode('utf-8'))
        insert_email(email_data)

except KeyboardInterrupt:
    pass
finally:
    consumer.close()
    cur.close()
    conn.close()
