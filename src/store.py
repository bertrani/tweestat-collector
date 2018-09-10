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
                           tag_map=data.hashtag_map, name="hashtag", min_size=min_size, last_count=last_count)
        _store_summed_list(client=client, lock=lock, data=data,
                           tag_map=data.url_map, name="url", min_size=min_size, last_count=last_count)

def store_source_lang(client, lock, data, interval=60, min_size_lang=8, min_size_source=5):
    while True:
        last_count = data.counter
        time.sleep(interval)
        _store_summed(client=client, lock=lock, data=data,tag_map=data.source_map, name="source", min_size=min_size_source, last_count=last_count)
        _store_summed(client=client, lock=lock, data=data,
                     tag_map=data.lang_map, name="lang", min_size=min_size_lang, last_count=last_count)
        _store_summed(client=client, lock=lock, data=data,
                     tag_map=data.usr_lang_map, name="usr_lang", min_size=min_size_lang, last_count=last_count)


def _store_raw(client, lock, data):
    if data.tweet_buffer:
        lock.acquire()
        client.write_points(data.tweet_buffer, database='tweestat_raw')
        data.reset_tweets()
        lock.release()
    else:
        logging.warning("Empty tweet buffer")


def _store_summed_list(client, lock, data, tag_map, name, min_size, last_count):
    if tag_map:
        try:
            json_list = []
            lock.acquire()
            for key in tag_map:
                json = {"measurement": str(name), "tags": {}, "fields": {}}
                if tag_map[key] >= min_size:
                    json["tags"][str(name)] = key
                    json["fields"]["count"] = tag_map[key]
                    json["fields"]["total_count"] = data.counter - last_count
                    json_list.append(json)
            getattr(data, "reset_"+name)()
            lock.release()
            print(json_list)
            client.write_points(json_list)
        except IndexError as e:
            logging.warning("IndexError occured while writing %s: %s".format(name, str(e)))
    else:
        logging.warning("Empty %s map".format(name))


def _store_summed(client, lock, data, tag_map, name, min_size, last_count):
    if tag_map:
        json = {"measurement": str(name), "tags": {}, "fields": {}}
        try:
            lock.acquire()
            for key in tag_map:
                if tag_map[key] >= min_size:
                    json["fields"][key] = tag_map[key]
            json["fields"]["total_count"] = data.counter - last_count
            getattr(data, "reset_" + name)()
            lock.release()
            client.write_points(json)
        except IndexError as e:
            logging.warning("IndexError occured while writing %s: %s".format(name, str(e)))
    else:
        logging.warning("Empty %s map".format(name))
