import time
import logging

logger = logging.getLogger(__name__)


def store_tweets(client, lock, data, interval=1):
    time.sleep(1)
    while True:
        time.sleep(interval)
        _store_raw(client=client, lock=lock, data=data)


def store_tags_urls(client, lock, data, interval=900, min_size=5):
    while True:
        last_count = data.counter
        time.sleep(interval)
        _store_summed_list(client=client, lock=lock, data=data,
                           counter=data.hashtag_counter, name="hashtag", min_size=min_size, last_count=last_count)
        _store_summed_list(client=client, lock=lock, data=data,
                           counter=data.url_counter, name="url", min_size=min_size, last_count=last_count)

def store_source_lang(client, lock, data, interval=60, min_size_lang=8, min_size_source=5):
    while True:
        last_count = data.counter
        time.sleep(interval)
        _store_summed(client=client, lock=lock, data=data,counter=data.source_counter, name="source", min_size=min_size_source, last_count=last_count)
        _store_summed(client=client, lock=lock, data=data,
                     counter=data.lang_counter, name="lang", min_size=min_size_lang, last_count=last_count)
        _store_summed(client=client, lock=lock, data=data,
                     counter=data.usr_lang_counter, name="usr_lang", min_size=min_size_lang, last_count=last_count)


def _store_raw(client, lock, data):
    if data.tweet_buffer:
        lock.acquire()
        client.write_points(data.tweet_buffer, database='tweestat_raw')
        data.reset_tweets()
        lock.release()
    else:
        logging.warning("Empty tweet buffer")


def _store_summed_list(client, lock, data, counter, name, min_size, last_count):
    if counter:
        try:
            json_list = []
            lock.acquire()
            for key in counter:
                json = {"measurement": str(name), "tags": {}, "fields": {}}
                if counter[key] >= min_size:
                    json["tags"][str(name)] = key
                    json["fields"]["count"] = counter[key]
                    json["fields"]["total_count"] = data.counter - last_count
                    json_list.append(json)
            getattr(data, "reset_"+name)()
            lock.release()
            client.write_points(json_list)
        except IndexError as e:
            logging.warning("IndexError occured while writing %s: %s", name, str(e))
    else:
        logging.warning("Empty %s map", name)


def _store_summed(client, lock, data, counter, name, min_size, last_count):
    if counter:
        json = {"measurement": str(name), "tags": {}, "fields": {}}
        try:
            lock.acquire()
            for key in counter:
                if counter[key] >= min_size:
                    json["fields"][key] = counter[key]
            json["fields"]["total_count"] = data.counter - last_count
            getattr(data, "reset_" + name)()
            lock.release()
            client.write_points([json])
        except IndexError as e:
            logging.warning("IndexError occured while writing %s: %s", name, str(e))
    else:
        logging.warning("Empty %s map", name)
