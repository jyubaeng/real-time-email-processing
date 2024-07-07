import imaplib
import email
from email.header import decode_header
import time
import re
import json
from confluent_kafka import Producer

# Kafka configuration
kafka_config = {
    'bootstrap.servers': 'localhost:9092',
}
producer = Producer(kafka_config)
topic = 'emails'

# Function to decode email content with fallback
def decode_email_content(content, encoding):
    try:
        if isinstance(content, bytes):
            return content.decode(encoding or 'utf-8')
    except UnicodeDecodeError:
        try:
            return content.decode('latin-1')
        except UnicodeDecodeError:
            return content.decode('ascii', errors='ignore')

# Function to fetch new emails
def fetch_new_emails():
    username = "jyubaeng@gmail.com"
    password = "wlzx uunw klyv aqsn"
    mail = imaplib.IMAP4_SSL("imap.gmail.com")
    mail.login(username, password)
    mail.select("inbox")

    # Search for all unseen emails
    status, messages = mail.search(None, "UNSEEN")
    email_ids = messages[0].split()

    for email_id in email_ids:
        status, msg_data = mail.fetch(email_id, "(RFC822)")
        for response_part in msg_data:
            if isinstance(response_part, tuple):
                msg = email.message_from_bytes(response_part[1])
                subject, encoding = decode_header(msg["Subject"])[0]
                if isinstance(subject, bytes):
                    subject = decode_email_content(subject, encoding)
                from_ = msg.get("From")
                to_ = msg.get("To")
                cc_ = msg.get("Cc")
                bcc_ = msg.get("Bcc")
                date_ = msg.get("Date")

                if date_:
                    date_ = email.utils.parsedate_to_datetime(date_)

                body_plain = ""
                if msg.is_multipart():
                    for part in msg.walk():
                        content_type = part.get_content_type()
                        content_disposition = str(part.get("Content-Disposition"))
                        if content_type == "text/plain" and "attachment" not in content_disposition:
                            body_plain = decode_email_content(part.get_payload(decode=True), part.get_content_charset())
                            break
                else:
                    body_plain = decode_email_content(msg.get_payload(decode=True), msg.get_content_charset())

                links = []
                if body_plain:
                    links = re.findall(r'(https?://\S+)', body_plain)

                email_data = {
                    'email_id': email_id.decode(),
                    'subject': subject,
                    'sender': from_,
                    'recipients': [recipient.strip() for recipient in to_.split(',')] if to_ else [],
                    'cc': [recipient.strip() for recipient in cc_.split(',')] if cc_ else [],
                    'bcc': [recipient.strip() for recipient in bcc_.split(',')] if bcc_ else [],
                    'date': date_.isoformat() if date_ else None,
                    'body_plain': body_plain,
                    'links': links
                }

                # Produce message to Kafka
                producer.produce(topic, value=json.dumps(email_data))
                producer.flush()

    mail.close()
    mail.logout()

# Main loop to continuously fetch new emails
while True:
    fetch_new_emails()
    time.sleep(60)  # Wait for a minute before checking for new emails again
