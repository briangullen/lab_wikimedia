from confluent_kafka import Producer
from sseclient import SSEClient as EventSource
import socket

KAFKA_TOPIC = 'wikimedia_events'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:51168'
REQUEST_URL = "https://stream.wikimedia.org/v2/stream/recentchange"

def delivery_callback(err, msg):
    if err:
        print('%% Message failed delivery: %s' % err)
    else:
        print('%% Message delivered to %s [%d]' % (msg.topic(), msg.partition()))

def main():
    # Kafka producer configuration
    conf = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'client.id': socket.gethostname()}

    # Create Kafka producer instance
    producer = Producer(conf)

    # Read the Wikimedia stream and produce messages to Kafka
    for event in EventSource(REQUEST_URL):
        if event.event == 'message':
            try:
                producer.produce(KAFKA_TOPIC, event.data, callback=delivery_callback)
                # Flush messages to Kafka to ensure they are sent immediately
                producer.flush()
            except ValueError:
                pass

    # Close Kafka producer
    producer.close()

if __name__ == '__main__':
    main()
