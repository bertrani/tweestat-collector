from tweepy.streaming import StreamListener
import simplejson as json
from urllib.parse import urlparse
import logging
import time

logger = logging.getLogger(__name__)

class StdOutListener(StreamListener):
    def __init__(self, data, lock):
        logging.info("Collector started ...")
        super().__init__()
        self.data = data
        self.lock = lock
        self.tweet = [{"measurement": "tweet", "tags": {}, "fields": {}}]

    def on_data(self, data):
        try:
            if data.startswith("{\"created_at"):
                json_data = json.loads(data)
                self.get_data(json_data)
        except Exception as e:
            logging.critical("Failed to collect data: %s", str(e))

    def on_error(self, status):
        logging.error("Error on API connection: %s", status)

    def get_data(self, json_data):
        self.entities_count_reader(json_data, 'hashtags')
        self.entities_count_reader(json_data, 'urls')
        self.user_reader(json_data, 'followers_count')
        self.user_reader(json_data, 'friends_count')
        self.user_reader(json_data, 'statuses_count')
        self.user_reader(json_data, 'favourites_count')
        self.boolean_reader(json_data, 'possibly_sensitive')
        self.boolean_reader(json_data, 'is_quote_status')
        self.exists_reader(json_data, 'retweeted_status')
        self.null_reader(json_data, 'coordinates')
        self.null_reader(json_data, 'place')
        self.misc_reader(json_data)
        self.hash_url_reader(json_data)

        self.lock.acquire()
        self.data.tweet_buffer.append(self.tweet)
        self.lock.release()

    def entities_count_reader(self, json_data, field):
        try:
            self.tweet[0]["fields"]["no_" + field] = len(json_data["entities"][field])
        except KeyError:
            self.tweet[0]["fields"]["no_" + field] = 0
            logging.warning("KeyError while reading " + field)

    def user_reader(self, json_data, field):
        try:
            self.tweet[0]["fields"]["usr_" + field] = json_data["user"][field]
        except KeyError:
            self.tweet[0]["fields"]["usr_" + field] = 0
            logging.warning('KeyError while reading ' + field)

    def null_reader(self, json_data, field):
        try:
            if json_data[field]:
                self.tweet[0]["fields"]['has_' + field] = 1
            else:
                self.tweet[0]["fields"]['has_' + field] = 0
        except KeyError:
            self.tweet[0]["fields"]['has_' + field] = 0
            logging.warning('KeyError while reading ' + field)

    def boolean_reader(self, json_data, field):
        try:
            if str(json_data[field]) == "True":
                self.tweet[0]["fields"][field] = 1
            else:
                self.tweet[0]["fields"][field] = 0
        except KeyError:
            self.tweet[0]["fields"][field] = 0

    def exists_reader(self, json_data, field):
        try:
            if json_data[field]:
                self.tweet[0]["fields"][field] = 1
            else:
                self.tweet[0]["fields"][field] = 0
        except KeyError:
            self.tweet[0]["fields"][field] = 0

    def misc_reader(self, json_data):
        try:
            self.tweet[0]["tags"]["usr_language"] = json_data["user"]["lang"]
        except KeyError:
            logging.warning('KeyError while reading user/lang')

        try:
            self.tweet[0]["tags"]["tweet_language"] = json_data["lang"]
        except KeyError:
            logging.warning('KeyError while reading tweet/lang')

        try:
            source = json_data["source"]
            self.tweet[0]["tags"]["source"] = source[source.index(">")+1:source.index("<", source.index(">") + 1)]
        except KeyError:
            logging.warning('KeyError while reading source')
        except Exception as e:
            logging.warning('Failed to read source field: %s', str(e))

        try:
            characters = json_data["text"]
            self.tweet[0]["fields"]["no_characters"] = len(characters)
        except KeyError:
            logging.warning('KeyError while reading length of text')

    def hash_url_reader(self, json_data):
        try:
            for tag in json_data["entities"]["hashtags"]:
                tag_str = str.lower(tag["text"])
                self.lock.acquire()
                if tag_str in self.data.hashtag_map:
                    self.data.hashtag_map[tag_str] += 1
                else:
                    self.data.hashtag_map[tag_str] = 1
                self.lock.release()
        except KeyError as e:
            logging.warning('KeyError while reading hashtags: %s', str(e))

        try:
            for url in json_data["entities"]["urls"]:
                netloc = urlparse(url["expanded_url"]).netloc
                self.lock.acquire()
                if netloc in self.data.url_map:
                    self.data.url_map[netloc] += 1
                else:
                    self.data.url_map[netloc] = 1
                self.lock.release()
        except KeyError as e:
            logging.warning('KeyError while reading urls: %s', str(e))

