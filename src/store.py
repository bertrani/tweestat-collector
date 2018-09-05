import time
import logging

logger = logging.getLogger(__name__)

def store_tags_and_urls(client, lock, data):
    hashtag = [{"measurement": "hashtag", "tags": {}, "fields": {}}]
    url = [{"measurement": "url", "tags": {}, "fields": {}}]
    while True:
        try:
            # Store every 15 minutes
            time.sleep(600)
            if data.hashtag_map:
                lock.acquire()
                # Create descending lists with the most commonly appeared hashtags
                sorted_hashs = list(reversed(sorted(data.hashtag_map, key=data.hashtag_map.__getitem__)))
                lock.release()
                try:
                    for x in range(100):
                        hashtag[0]["tags"]["hash"] = str(sorted_hashs[x])
                        # Estimating the total occurance of the hashtag per minute.
                        # Since a sample of 1% is used, the estimated total occurance is 100 times as high.
                        # Because values are accumulated for 10 minutes, the value is divided by 10 to
                        # get the occurrence per minute.
                        hashtag[0]["fields"]["count"] = round((data.hashtag_map[sorted_hashs[x]]*100)/10)
                        client.write_points(hashtag)
                except IndexError as e:
                    logging.warning("IndexError occured while writing hashtags: %s", str(e))
                lock.acquire()
                data.reset_hashtags()
                lock.release()
            else:
                logging.warning("Empty hashtag map")
            if data.url_map:
                lock.acquire()
                sorted_urls = list(reversed(sorted(data.url_map, key=data.url_map.__getitem__)))
                lock.release()
                try:
                    for x in range(50):
                        url[0]["tags"]["url"] = str(sorted_urls[x])
                        url[0]["fields"]["count"] = round((data.url_map[sorted_urls[x]]*100)/10)
                        client.write_points(url)
                except IndexError as e:
                    logging.warning("IndexError occured while writing urls: %s", str(e))

                lock.acquire()
                data.reset_urls()
                lock.release()
            else:
                logging.warning("Empty URL map")
        except Exception as e:
            logging.error("Failed to store hashtags/urls: %s", str(e))

def store_tweets(client, lock, data):
    while True:
        try:
            time.sleep(1)
            if data.tweet_buffer:
                lock.acquire()
                for tweet in data.tweet_buffer:
                    client.write_points(tweet)
                data.reset_tweets()
                lock.release()
            else:
                logging.warning("Empty tweet buffer")
                time.sleep(10)
        except Exception as e:
            logging.error("Failed to store tweets: %s", str(e))



