from kafka import KafkaProducer
import json

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'fraud'
JSON_FILE = 'transactions.txt'

try:
    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    with open(JSON_FILE, 'r') as data:
        for line in data:
            try:
                entry = json.loads(line)
                producer.send(KAFKA_TOPIC, entry)
            except json.JSONDecodeError as json_err:
                print(f"Error decoding JSON: {json_err}")
                

except Exception as err:
    print(f"Error occurred: {err}")

else:
    producer.flush()
    print(f'Data sent to topic {KAFKA_TOPIC} successfully.')
    

finally:
    producer.close()
    print(f"Producer Terminated Successfully.")
