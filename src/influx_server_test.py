import time
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from influxdb import InfluxDBClient
import simplejson as json
import datetime
import threading
from urllib.parse import urlparse

access_token = "971353729369853952-nd1J0LwfQ2eaUbfGycZGIuvlPjb79JF"
access_token_secret = "iI7gDWynLj7hlS4MtWI2kdFSTZIwymtsSszhdR13JxMRS"
consumer_key = "JxZR7y7x7y6iR7GDO8O6lm6dM"
consumer_secret = "XuevFaX5ztvOKYOAibaZxjav3YypAjJ1zer4Twaz9jM4Z6eRgn"

hashtag = [{"measurement": "hashtag",
            "tags": {
                "hash": ""
            },
            "fields": {
                "count":""
            }}]

url = [{"measurement": "url",
            "tags": {
                "url": ""
            },
            "fields": {
                "count":""
            }}]

tweet = [{"measurement": "tweet",
          "tags": {
              # NOMINAL
              "language":"",
              "source":"",
              "usr_language":""
          },
          "fields": {
              "no_hashtags":"",
              "no_urls":"",
              "no_usr_follower":"",
              "no_usr_friend":"",
              "no_usr_statuses":"",
              "no_usr_favourites":"",
              #ORDINAL
              "is_sensitive":"",
              "is_quote":"",
              "is_retweeted":"",
              "has_coordinates":"",
              "has_place":"",
              "has_hash":"",
              "has_url":"",
              "has_poll":"",
              "usr_is_verified":"",
              #NOT YET INCLUDED IN THESIS:
              "no_words":"",
          }}]

on_data_time = []
first_time = []
second_time = []
third_time = []
write_time = []

class StdOutListener(StreamListener):
    def __init__(self):
        self.hashtags={}
        self.urls={}
        self.tweets = []
        super().__init__()
        self.client = InfluxDBClient('52.57.236.217', 8086, 'admin', 'targa123', 'tweestat_test')

    def on_data(self, data):
        try:
            t0 = time.time()
            if data.startswith("{\"created_at"):
                json_data = json.loads(data)
                on_data_time.append(time.time()-t0)
                self.build_tweet(json_data)
        except KeyError as e:
            print(datetime.datetime.now())
            print(repr(e))
            print("")


    def on_error(self, status):
        print(status)

    def migrate_dicts(self):
        while True:
            time.sleep(60)
            sorted_hashs = list(reversed(sorted(self.hashtags, key=self.hashtags.__getitem__)))
            sorted_urls = list(reversed(sorted(self.urls, key=self.urls.__getitem__)))
            for x in range(20):
                hashtag[0]["tags"]["hash"] = str(sorted_hashs[x])
                hashtag[0]["fields"]["count"] = self.hashtags[sorted_hashs[x]]
            for x in range(10):
                self.client.write_points(hashtag)
                url[0]["tags"]["url"] = str(sorted_urls[x])
                url[0]["fields"]["count"] = self.urls[sorted_urls[x]]
                self.client.write_points(url)
            self.hashtags = {}
            print("on_data_time:  " + str(sum(on_data_time) / float(len(on_data_time))))
            print("first_time:  " + str(sum(first_time) / float(len(first_time))))
            print("second_time:  " + str(sum(second_time) / float(len(second_time))))
            print("write_time:  " + str(sum(write_time) / float(len(write_time))))
            print("third_time:  " + str(sum(third_time) / float(len(third_time))))

    def migrate_tweets(self):
        while True:
            time.sleep(1)
            for tweet in self.tweets:
                self.client.write_points(tweet)



    def build_tweet(self, json_data):
        t0 = time.time()
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

        first_time.append(time.time()-t0)
        t0 = time.time()

        try:
            tweet[0]["tags"]["usr_language"] = json_data["user"]["lang"]
        except KeyError:
            print('usr_language error')

        try:
            tweet[0]["tags"]["tweet_language"] = json_data["lang"]
        except KeyError:
            print('tweet_language_error')

        try:
            source = json_data["source"]
            tweet[0]["tags"]["source"] = source[source.index(">")+1:source.index("<", source.index(">") + 1)]
        except KeyError:
            print('source_error')

        try:
            characters = json_data["text"]
            tweet[0]["fields"]["no_characters"] = len(characters)
        except KeyError:
            print('Character count error')

        second_time.append(time.time() - t0)
        t0 = time.time()
        self.tweets.append(tweet)
        #self.client.write_points(tweet)
        write_time.append(time.time() - t0)

        t0 = time.time()

        try:
            for tag in json_data["entities"]["hashtags"]:
                tag_str = tag["text"]
                if tag_str in self.hashtags:
                    self.hashtags[tag_str] += 1
                else:
                    self.hashtags[tag_str] = 1
                #hashtag[0]["tags"]["hash"] = tag["text"]
                #self.client.write_points(hashtag)
        except KeyError:
            print('Hashtag key error')

        try:
            for url in json_data["entities"]["urls"]:
                netloc = urlparse(url["expanded_url"]).netloc
                if netloc in self.urls:
                    self.urls[netloc] += 1
                else:
                    self.urls[netloc] = 1
                #url[0]["tags"]["hash"] = urlparse(url["expanded_url"]).netloc
                #self.client.write_points(url)
        except KeyError:
            print('Hashtag key error')

        third_time.append(time.time() - t0)

    def entities_count_reader(self, json_data, field):
        try:
            tweet[0]["fields"]["no_" + field] = len(json_data["entities"][field])
        except KeyError:
            tweet[0]["fields"]["no_" + field] = 0
            print("Count reader error on: " + field)

    def user_reader(self, json_data, field):
        try:
            tweet[0]["fields"]["usr_" + field] = json_data["user"][field]
        except KeyError:
            tweet[0]["fields"]["usr_" + field] = 0
            print('User reader error on: ' + field)

    def null_reader(self, json_data, field):
        try:
            if json_data[field]:
                tweet[0]["fields"]['has_' + field] = 1
            else:
                tweet[0]["fields"]['has_' + field] = 0
        except KeyError:
            tweet[0]["fields"]['has_' + field] = 0
            print('Null reader error on: ' + field)

    def boolean_reader(self, json_data, field):
        try:
            if str(json_data[field]) == "True":
                tweet[0]["fields"][field] = 1
            else:
                tweet[0]["fields"][field] = 0
        except KeyError:
            tweet[0]["fields"][field] = 0

    def exists_reader(self, json_data, field):
        try:
            if json_data[field]:
                tweet[0]["fields"][field] = 1
            else:
                tweet[0]["fields"][field] = 0
        except KeyError:
            tweet[0]["fields"][field] = 0
            # print("Exists reader error on: " + field)


if __name__ == '__main__':

    #This handles Twitter authetification and the connection to Twitter Streaming API
    print("STARTED ...")
    connection = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, connection)

    threading.Thread(target=stream.sample).start()
    threading.Thread(target=connection.migrate_dicts).start()
    threading.Thread(target=connection.migrate_tweets()).start()
