from kafka import KafkaProducer
import time



def main():
    producer = KafkaProducer(bootstrap_servers='localhost:9092')

    if producer.bootstrap_connected():
        print("Success")

    topic = 'test'

    while True:
        future = producer.send(topic, str.encode("test_data;"))
        producer.flush()
        future.get(timeout=60)
        print("message sent successfully...")
        time.sleep(5)

if __name__ == '__main__':
    main()
