import logging
import threading
import time
import configparser
from tweepy import OAuthHandler, Stream
from influxdb import InfluxDBClient
from collect import TweetListener
from data import data
from store import *

# TODO Reimplement locks for thread safety. Was already implement but caused some problems.
# TODO Seems to run without locks for a few weeks now.


def start(function, name, kwargs=None):
    """ Runs a function and rerun it should an exception occur. """
    # Time to wait until running the function again after an exception occurred.
    wait_time = 1
    while True:
        logging.info("Started %s", name)
        t0 = time.time()
        try:
            # If arguments are given, run the function with these arguments.
            if kwargs:
                function(**kwargs)
            else:
                function()
        # Except any exception. Specific exceptions should be excepted on a lower level.
        # If any exception occurs at this point simple rerun the function.
        except Exception as ex:
            # When the function runs for at least 5 Minutes a rerun is not considered a reconnection
            # (see Twitter reconnection best practises) anymore. The timer is therefor reset.
            if (time.time()-t0) > 300:
                wait_time = 1
            logging.error("%s discontinued: %s", name, str(ex))
            logging.warning("Waiting %s seconds until restarting %s ...", wait_time, name)
            time.sleep(wait_time)
            # Doubling the time to wait between reconnections is suggested by the Twitter API documentation.
            wait_time *= 2

if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read("../config.ini")

    # Get Twitter credentials from the config file.
    access_token = config["TWITTER"]["access_token"]
    access_token_secret = config["TWITTER"]["access_token_secret"]
    consumer_key = config["TWITTER"]["consumer_key"]
    consumer_secret = config["TWITTER"]["consumer_secret"]

    # Get logfile location from config file and config logging
    logging.basicConfig(filename=config["LOGGING"]["file"],
                        level=logging.INFO ,
                        format='%(levelname)s: %(asctime)s %(message)s')

    # If cli_log is True, enable additional cli logging
    if config["LOGGING"]["cli_log"] == "True":
        logging.getLogger().addHandler(logging.StreamHandler())

    logging.info("Tweestat started ...")

    # Create a global data instance, which is passed
    # to the collect and store module and functions as
    # a temporary data store.
    data = data()

    # Initialize the InfluxDB client using the credentials stored in the config file
    client = InfluxDBClient(config["INFLUXDB"]["IP"], 8086, config["INFLUXDB"]["user"],
                            config["INFLUXDB"]["password"], "tweestat")

    # Initialize an OAuthHandler and TweetListener and pass them to the Stream object
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    connection = TweetListener(data=data)
    stream = Stream(auth, connection)

    # Start a Thread for collection data and one for storing data
    threading.Thread(target=start, kwargs=dict(function=stream.sample, name='Collect'), name="Collect").start()
    threading.Thread(target=start, kwargs=dict(function=store_tweets, name="StoreTweets",
                                               kwargs=dict(client=client, data=data, lock=lock))).start()

    # The following threads are not started since the according features are disabled at the moment.
    #
    # threading.Thread(
    #       target=start, kwargs=dict(function=store_tags_urls, name="StoreTagsUrls",
    #                                 kwargs=dict(client=client, data=data, lock=lock, interval=600))).start()
    # threading.Thread(
    #       target=start, kwargs=dict(function=store_source_lang, name="StoreSourceLang",
    #                                 kwargs=dict(client=client, data=data, lock=lock, interval=60))).start()
    #
