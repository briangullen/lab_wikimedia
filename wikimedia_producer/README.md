# Wikimedia Part 1 Solution

## Steps to Start
1. Ensure libraries are installed for each Python project (producter and consumer). See requirements.txt.
2. Ensure Kafka is installed and started: `confluent local kafka start`
3. Identify the Kafka port. Update `KAFKA_BOOTSTRAP_SERVERS` in each main.py (producer and consumer) to that port. eg. `KAFKA_BOOTSTRAP_SERVERS = ':56671'`
4. Run `wikimedia_producer.py`.