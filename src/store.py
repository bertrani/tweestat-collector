import time
import logging
from urllib3.exceptions import HTTPError

logger = logging.getLogger(__name__)


def store_tweets(client, data):
    # Wait a second before starting to make sure, the tweet_buffer is already initialised.
    time.sleep(1)
    while True:
        # Write the data to the database, if there are atleast 5000 objects in the Tweet buffer.
        if len(data.tweet_buffer) > 5000:
            _store_raw(client=client, data=data)
        # Wait for one minute vefore checking the size of the Tweet buffer again.
        time.sleep(60)


def _store_raw(client, data):
    if data.tweet_buffer:
        try:
            # Write multiple points to the InfluxDB database.
            client.write_points(data.tweet_buffer, database='tweestat_raw')
        except HTTPError as e:
            logging.warning("Error while parsing Header on Tweet storage: %s", str(e))
            data.reset_tweets()
        # Clear the Tweet buffer
        data.reset_tweets()
    else:
        logging.warning("Empty tweet buffer")


# All functions below were once used to store data about variables, depending the occurence of the variables.
# Eg. store only hashtags which occured atleast 5 times in 15 minutes. Since hashtags and URLs are not stored anymore
# and selected sources and languages are stored, these are not used at the moment. They remain here, to show how
# storing these variables could be done and possibly inspire future implementations.

def store_tags_urls(client, data, interval=900, min_size=5):
    while True:
        last_count = data.counter
        time.sleep(interval)
        _store_summed_list(client=client, data=data,
                           counter=data.hashtag_counter, name="hashtag", min_size=min_size, last_count=last_count)
        _store_summed_list(client=client, data=data,
                           counter=data.url_counter, name="url", min_size=min_size, last_count=last_count)


def store_source_lang(client, data, interval=60, min_size_lang=8, min_size_source=5):
    while True:
        last_count = data.counter
        time.sleep(interval)
        _store_summed(client=client, data=data,counter=data.source_counter, name="source", min_size=min_size_source, last_count=last_count)
        _store_summed(client=client, data=data,
                     counter=data.lang_counter, name="lang", min_size=min_size_lang, last_count=last_count)
        _store_summed(client=client, data=data,
                     counter=data.usr_lang_counter, name="usr_lang", min_size=min_size_lang, last_count=last_count)


def _store_summed_list(client, data, counter, name, min_size, last_count):
    if counter:
        json_list = []
        try:
            for key in counter:
                json = {"measurement": str(name), "tags": {}, "fields": {}}
                if counter[key] >= min_size:
                    json["tags"][str(name)] = key
                    json["fields"]["count"] = counter[key]
                    json["fields"]["total_count"] = data.counter - last_count
                    json_list.append(json)
            getattr(data, "reset_" + name)()
            client.write_points(json_list)
        except HTTPError as e:
            logging.warning("Error while parsing Header on %s storage: %s", name, str(e))
            getattr(data, "reset_" + name)()
    else:
        logging.warning("Empty %s map", name)


def _store_summed(client, data, counter, name, min_size, last_count):
    if counter:
        json = {"measurement": str(name), "tags": {}, "fields": {}}
        try:
            for key in counter:
                if counter[key] >= min_size:
                    json["fields"][key] = counter[key]
            json["fields"]["total_count"] = data.counter - last_count
            getattr(data, "reset_" + name)()
            client.write_points([json])
        except HTTPError as e:
            logging.warning("Error while parsing Header on %s storage: %s", name, str(e))
            getattr(data, "reset_" + name)()
    else:
        logging.warning("Empty %s map", name)
