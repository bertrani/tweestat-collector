from tweepy.streaming import StreamListener
import simplejson as json
from urllib.parse import urlparse
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

languages = ["de", "en", "ja", "es", "fr", "ru"]
sources = {"Twitter for iPhone":"iphone", "Twitter for Android":"android", "Twitter Web Client":"web", "Twitter for iPad":"ipad"}


class TweetListener(StreamListener):
    def __init__(self, data):
        super().__init__()
        self.data = data
        self.tweet = {"measurement": "tweet", "tags": {}, "fields": {}}

    def on_data(self, data):
        self.data.count()
        if data.startswith("{\"created_at"):
            json_data = json.loads(data)
            self.get_data(json_data)

    def on_error(self, status):
        logging.error("Error on API connection: %s", status)

    def get_data(self, json_data):
        self.set_time()

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
        self.char_reader(json_data)
        #self.hash_url_reader(json_data)
        self.tag_reader(json_data)

        self.data.tweet_buffer.append(self.tweet)

        self.tweet = {"measurement": "tweet", "tags": {}, "fields": {}}

    def set_time(self):
        self.tweet["time"] = datetime.now().isoformat()

    def entities_count_reader(self, json_data, field):
        try:
            self.tweet["fields"]["no_" + field] = len(json_data["entities"][field])
        except KeyError:
            self.tweet["fields"]["no_" + field] = 0
            logging.warning("KeyError while reading " + field)

    def user_reader(self, json_data, field):
        try:
            self.tweet["fields"]["usr_" + field] = json_data["user"][field]
        except KeyError:
            self.tweet["fields"]["usr_" + field] = 0
            logging.warning('KeyError while reading ' + field)

    def null_reader(self, json_data, field):
        try:
            if json_data[field]:
                self.tweet["fields"]['has_' + field] = 1
            else:
                self.tweet["fields"]['has_' + field] = 0
        except KeyError:
            self.tweet["fields"]['has_' + field] = 0
            logging.warning('KeyError while reading ' + field)

    def boolean_reader(self, json_data, field):
        try:
            if str(json_data[field]) == "True":
                self.tweet["fields"][field] = 1
            else:
                self.tweet["fields"][field] = 0
        except KeyError:
            self.tweet["fields"][field] = 0

    def exists_reader(self, json_data, field):
        try:
            if json_data[field]:
                self.tweet["fields"][field] = 1
            else:
                self.tweet["fields"][field] = 0
        except KeyError:
            self.tweet["fields"][field] = 0

    def char_reader(self, json_data):
        try:
            characters = json_data["text"]
            self.tweet["fields"]["no_characters"] = len(characters)
        except KeyError:
            logging.warning('KeyError while reading length of text')

    def tag_reader(self, json_data):
        try:
            usr_lang = json_data["user"]["lang"]
            if usr_lang in languages:
                self.tweet["fields"]["usr_language_"+usr_lang] = 1
                rest = [x for x in languages if x != usr_lang]
                for l in rest:
                    self.tweet["fields"]["usr_language_" + l] = 0
            else:
                for l in languages:
                    self.tweet["fields"]["usr_language_" + l] = 0
        except KeyError:
            logging.warning('KeyError while reading user/lang')

        try:
            lang = json_data["lang"]
            if lang in languages:
                self.tweet["fields"]["tweet_language_"+lang] = 1
                rest = [x for x in languages if x != lang]
                for l in rest:
                    self.tweet["fields"]["tweet_language_" + l] = 0
            else:
                for l in languages:
                    self.tweet["fields"]["tweet_language_" + l] = 0
        except KeyError:
            logging.warning('KeyError while reading tweet/lang')

        try:
            source = json_data["source"]
            source_string = source[source.index(">") + 1:source.index("<", source.index(">") + 1)]
            if source_string in sources:
                self.tweet["fields"]["source_"+sources[source_string]] = 1
                rest = dict(sources)
                del rest[source_string]
                for s in rest:
                    self.tweet["fields"]["source_" + sources[s]] = 0
            else:
                for s in sources:
                    self.tweet["fields"]["source_" + sources[s]] = 0
        except KeyError as e:
            logging.warning('KeyError while reading source')
            print(str(e))
        except ValueError as e:
            logging.warning('Failed to read source field: %s', str(e))

    def hash_url_reader(self, json_data):
        try:
            for tag in json_data["entities"]["hashtags"]:
                tag_str = str.lower(tag["text"])
                self.data.hashtag_counter[tag_str] += 1
        except KeyError as e:
            logging.warning('KeyError while reading hashtags: %s', str(e))

        try:
            for url in json_data["entities"]["urls"]:
                netloc = urlparse(url["expanded_url"]).netloc
                self.data.url_counter[netloc] += 1
        except KeyError as e:
            logging.warning('KeyError while reading urls: %s', str(e))