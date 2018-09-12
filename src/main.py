import logging
import threading
import traceback

from tweepy import OAuthHandler, Stream
from influxdb import InfluxDBClient
from collect import StdOutListener
from data import data
from store import *
import time
import configparser

def start(function, name, kwargs=None):
    wait_time = 1
    while True:
        logging.info("Started %s", name)
        t0 = time.time()
        try:
            if kwargs:
                function(**kwargs)
            else:
                function()
        except Exception as ex:
            kwargs[lock].release()
            if (time.time()-t0) > 300:
                wait_time = 1
            logging.error("%s discontinued: %s", name, str(ex))
            logging.warning("Waiting %s seconds until restarting %s ...", wait_time, name)
            time.sleep(wait_time)
            wait_time *= 2

if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read("../config.ini")

    access_token = config["TWITTER"]["access_token"]
    access_token_secret = config["TWITTER"]["access_token_secret"]
    consumer_key = config["TWITTER"]["consumer_key"]
    consumer_secret = config["TWITTER"]["consumer_secret"]

    logging.basicConfig(filename=config["LOGGING"]["file"],
                        level=logging.INFO ,
                        format='%(levelname)s: %(asctime)s %(message)s')

    if config["LOGGING"]["cli_log"] == "True":
        logging.getLogger().addHandler(logging.StreamHandler())

    logging.info("Tweestat started ...")

    # Create a global data instance, which is passed
    # to the collect and store module and functions as
    # a temporary data store.
    data = data()
    lock = threading.Lock()

    client = InfluxDBClient(config["INFLUXDB"]["IP"], 8086, config["INFLUXDB"]["user"],
                            config["INFLUXDB"]["password"], "tweestat")

    # client and data are used by several threads and
    # mostly handled by a central lock instance

    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    connection = StdOutListener(data=data, lock=lock)
    stream = Stream(auth, connection)

    threading.Thread(target=start, kwargs=dict(function=stream.sample, name='Collect'), name="Collect").start()
    threading.Thread(target=start, kwargs=dict(function=store_tweets, name="StoreTweets",
                                               kwargs=dict(client=client, data=data, lock=lock, interval=1))).start()
    #threading.Thread(target=start, kwargs=dict(function=store_tags_urls, name="StoreTagsUrls",
    #                                           kwargs=dict(client=client, data=data, lock=lock, interval=600))).start()
    #threading.Thread(target=start, kwargs=dict(function=store_source_lang, name="StoreSourceLang",
    #                                           kwargs=dict(client=client, data=data, lock=lock, interval=60))).start()