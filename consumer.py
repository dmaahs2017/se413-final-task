from kafka import KafkaConsumer
import json
import base64
import argparse

def main():
    consumer = KafkaConsumer('test', bootstrap_servers='localhost:9092')
    
    if consumer.bootstrap_connected():
        print("Connected to kafka.")

    for msg in consumer:
        x = json.loads(msg.value);
        decoded_text = str(base64.b64decode(x['text']), 'utf-8')
        print()
        print("Tweet: ", decoded_text)
        print("Likes: ", x['favorites'])

if __name__ == '__main__':
    main()
