import time
import logging

from urllib3.exceptions import HTTPError

logger = logging.getLogger(__name__)


def store_tweets(client, data):
    time.sleep(1)
    while True:
        print(len(data.tweet_buffer))
        if len(data.tweet_buffer) > 5000:
            _store_raw(client=client, data=data)
        time.sleep(20)


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


def _store_raw(client, data):
    if data.tweet_buffer:
        try:
            client.write_points(data.tweet_buffer, database='tweestat_raw')
        except HTTPError as e:
            logging.warning("Error while parsing Header on Tweet storage: %s", str(e))
            data.reset_tweets()
        data.reset_tweets()
    else:
        logging.warning("Empty tweet buffer")


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
