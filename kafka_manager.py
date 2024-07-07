import subprocess
import os
import signal
import sys
import time

# Paths to your scripts
producer_script = r"C:\Users\jyuba\Documents\ainbox\kafka_producer.py"
consumer_script = r"C:\Users\jyuba\Documents\ainbox\kafka_consumer.py"
# Function to start a script as a background process
def start_script(script_path):
    return subprocess.Popen(['python', script_path], creationflags=subprocess.CREATE_NEW_CONSOLE)

# Function to stop a running process
def stop_process(process):
    process.terminate()

# Function to handle termination signals
def signal_handler(sig, frame):
    print("Stopping producer and consumer...")
    stop_process(producer_process)
    stop_process(consumer_process)
    print("Producer and consumer stopped.")
    sys.exit(0)

if __name__ == "__main__":
    # Start producer and consumer processes
    producer_process = start_script(producer_script)
    consumer_process = start_script(consumer_script)

    # Set up signal handling for graceful termination
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Keep the main script running to maintain the subprocesses
    try:
        while True:
            time.sleep(1)  # Adjust sleep time as needed
    except KeyboardInterrupt:
        signal_handler(None, None)
