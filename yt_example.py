from datetime import datetime
import tweepy
import time
from kafka import KafkaConsumer, KafkaProducer
import base64
from argparse import ArgumentParser


def normalize_timestamp(time):
    mytime = datetime.strptime(time, "%Y-%m-%d %H:%M:%S")
    return (mytime.strftime("%Y-%m-%d %H:%M:%S"))


def get_twitter_data(topic_name, api, producer, query):
    res = api.search_full_archive("dev3", query=query)
    for i in res:
        encoded_bytes = base64.b64encode(i.text.encode('utf-8'))
        encoded_str = str(encoded_bytes, 'utf-8')
        record = '{'
        record += f'"text": "{encoded_str}", "favorites": {i.favorite_count}'
        record += '}'
        producer.send(topic_name, str.encode(record))


def periodic_work(topic_name, interval, api, producer, query):
    while True:
        get_twitter_data(topic_name, api, producer, query)
        #interval should be an integer, the number of seconds to wait
        time.sleep(interval)

def main():
    parser = ArgumentParser();
    parser.add_argument("query");
    args = parser.parse_args();

    bear_token = "AAAAAAAAAAAAAAAAAAAAAP22bwEAAAAAdxJhuuja0YDk5Cz7eVwRlx4dUNI%3D7zdSKEgXiEX7zycsodoESRfNOT1yIhfKyIpGfgVuvGG3Rq8gUe"
    auth = tweepy.OAuth2BearerHandler(bear_token)
    api = tweepy.API(auth)
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    topic_name = 'test'
    get_twitter_data(topic_name, api, producer, args.query)
    periodic_work(topic_name, 60*0.1, api, producer, args.query) #get data every couple of minutes


if __name__ == '__main__':
    main()
