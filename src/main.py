import logging
import threading
from tweepy import OAuthHandler, Stream
from influxdb import InfluxDBClient
from collect import StdOutListener
from data import data
from store import *
import time

# TODO: Save credentials in config file.
access_token = "971353729369853952-nd1J0LwfQ2eaUbfGycZGIuvlPjb79JF"
access_token_secret = "iI7gDWynLj7hlS4MtWI2kdFSTZIwymtsSszhdR13JxMRS"
consumer_key = "JxZR7y7x7y6iR7GDO8O6lm6dM"
consumer_secret = "XuevFaX5ztvOKYOAibaZxjav3YypAjJ1zer4Twaz9jM4Z6eRgn"

def collector():
    wait_time = 1

    while True:
        stream.sample()
        logging.warning("Collector discontinued, waiting %s seconds until restarting.", wait_time)
        time.sleep(wait_time)
        wait_time *= 2



if __name__ == '__main__':

    # TODO: Set filename and level in config file.
    logging.basicConfig(filename='../logs/collector.log',
                        level=logging.INFO ,
                        format='%(levelname)s: %(asctime)s %(threadName)s %(message)s')

    # TODO: Add optional logging output on console via config setting.
    logging.getLogger().addHandler(logging.StreamHandler())

    # Create a global data instance, which is passed
    # to the collect and store module and functions as
    # a temporary data store.
    data = data()
    lock = threading.Lock()
    # TODO: Save credentials in config file.
    client = InfluxDBClient('52.57.236.217', 8086, 'admin', 'targa123', 'tweestat_test')
    # client and data are used by several threads and
    # mostly handled by a central lock instance


    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    connection = StdOutListener(data=data, lock=lock)
    stream = Stream(auth, connection)

    threading.Thread(target=collector, name='Collect').start()
    threading.Thread(target=store_tweets, kwargs=dict(client=client, data=data, lock=lock), name="StoreTweets").start()
    threading.Thread(target=store_tags_and_urls,
                     kwargs=dict(client=client, data=data, lock=lock), name="StoreHashURL").start()

    logging.info("Collector started ...")
