from tweepy.streaming import StreamListener
import simplejson as json
from urllib.parse import urlparse
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

# List of languages to record
languages = ["de", "en", "ja", "es", "fr", "ru"]
# Dictionary of sources to record
sources = {"Twitter for iPhone":"iphone", "Twitter for Android":"android",
           "Twitter Web Client":"web", "Twitter for iPad":"ipad"}


class TweetListener(StreamListener):
    def __init__(self, data):
        super().__init__()
        self.data = data
        # This is the basic form InfluxDBClient requires to store data to InfluxDB
        self.tweet = {"measurement": "tweet", "tags": {}, "fields": {}}

    # Is run on every message received from the Twitter API
    def on_data(self, data):
        # Counts the amount of Tweets
        self.data.count()
        # Tweet objects start with "{created_at";
        # only process these messages containing Tweet objects.
        if data.startswith("{\"created_at"):
            json_data = json.loads(data)
            # Process the data and safe if to the Tweet buffer
            self.get_data(json_data)

    def on_error(self, status):
        logging.error("Error on API connection: %s", status)

    def get_data(self, json_data):
        # Set the Tweets time to the current time
        self.set_time()

        # Use different readers to read the received data, extract according values
        # and save it in the Tweet dicitonary
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
        self.tag_reader(json_data)

        # Hashtags and URLs are disabled at the moment
        # self.hash_url_reader(json_data)

        # Append the extracted data to the Tweet buffer
        self.data.tweet_buffer.append(self.tweet)

        # Reset the Tweet object.
        self.tweet = {"measurement": "tweet", "tags": {}, "fields": {}}

    def set_time(self):
        self.tweet["time"] = datetime.now().isoformat()

    # Gets how often a given entity occurred in a given Tweet
    def entities_count_reader(self, json_data, field):
        try:
            self.tweet["fields"]["no_" + field] = len(json_data["entities"][field])
        except KeyError:
            self.tweet["fields"]["no_" + field] = 0
            logging.warning("KeyError while reading " + field)

    # Gets values of different properties related to a given Tweets author such as the
    # amount of followers or send statuses
    def user_reader(self, json_data, field):
        try:
            self.tweet["fields"]["usr_" + field] = json_data["user"][field]
        except KeyError:
            self.tweet["fields"]["usr_" + field] = 0
            logging.warning('KeyError while reading ' + field)

    # Checks if a given field is present in the Tweet object and sets the related
    # value accordingly
    def null_reader(self, json_data, field):
        try:
            if json_data[field]:
                self.tweet["fields"]['has_' + field] = 1
            else:
                self.tweet["fields"]['has_' + field] = 0
        except KeyError:
            self.tweet["fields"]['has_' + field] = 0
            logging.warning('KeyError while reading ' + field)

    # Sets a variable to 1 if the related value is "True" and 0 if not "True"
    def boolean_reader(self, json_data, field):
        try:
            if str(json_data[field]) == "True":
                self.tweet["fields"][field] = 1
            else:
                self.tweet["fields"][field] = 0
        except KeyError:
            self.tweet["fields"][field] = 0

    # Very similar to the "null_reader" but doesn't add "has_" to the variable name.
    def exists_reader(self, json_data, field):
        try:
            if json_data[field]:
                self.tweet["fields"][field] = 1
            else:
                self.tweet["fields"][field] = 0
        except KeyError:
            self.tweet["fields"][field] = 0

    # Counts the amount of characters in a Tweets text.
    def char_reader(self, json_data):
        try:
            characters = json_data["text"]
            self.tweet["fields"]["no_characters"] = len(characters)
        except KeyError:
            logging.warning('KeyError while reading length of text')

    # Reads user language, Tweet language and Tweet source
    def tag_reader(self, json_data):
        try:
            usr_lang = json_data["user"]["lang"]
            # Check if the user language is in the language array defined above,
            # sets the according variable to 1 and all other language variables to 0.
            if usr_lang in languages:
                self.tweet["fields"]["usr_language_"+usr_lang] = 1
                rest = [x for x in languages if x != usr_lang]
                for l in rest:
                    self.tweet["fields"]["usr_language_" + l] = 0
            # Set all user language variables to 0 if the one given is not in the
            # defined language array
            else:
                for l in languages:
                    self.tweet["fields"]["usr_language_" + l] = 0
        except KeyError:
            logging.warning('KeyError while reading user/lang')

        # Very similar to the user language, the language of the Tweet is set.
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
            # Gets the string containing the source
            source_string = source[source.index(">") + 1:source.index("<", source.index(">") + 1)]
            # Get the source of the Tweet similar to how user language and language were gotten.
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

    ### Not used at the moment.
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